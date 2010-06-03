package Gearman::WorkerSpawner::Supervisor;

use strict;
use warnings;

use Danga::Socket ();
use Gearman::WorkerSpawner::Common;
use Gearman::WorkerSpawner::Supervisor::ParentConnection;
use IO::Handle ();
use POSIX qw( :sys_wait_h );
use Storable qw( thaw );

use constant PARENT_CHECK_PERIOD => 5;
use constant CHILD_CHECK_PERIOD  => 5;

use constant {
    SLOT_NUM    => 0,
    SLOT_ID     => 1,
    SLOT_PARAMS => 2,
};

if (!caller()) {
    # we're being called as a script, not a module, presumably from exec in Gearman::WorkerSpawner::_spawn_workers.
    supervise(@ARGV);
}

# Loads the given L<Gearman::Worker> subclass, then parses additional arguments
# as specified by the return value of the worker class' C<options()> class
# method via L<Getopt::Long>. These options are passed to the worker object's
# constructor and the C<work> method of the worker object is called repeatedly
# until either SIG_INT is received or the ppid changes (parent went away).
#
# This class method is automatically executed if
# Gearman::WorkerSpawner::Supervisor has no C<caller()>, i.e. if it is run as a
# script rather than loaded as a module. This should only be done by other
# internal methods of this package.

sub supervise {
    my $fileno = shift;
    die "missing fileno parameter" unless $fileno;

    open my $parent_sock, '<&=', $fileno or die "failed to open pipe: $!\n";

    $SIG{__DIE__} = sub {
        my $err = shift;
        warn $err;
        local $SIG{PIPE} = '';
        print $parent_sock $err;
        exit 1;
    };

    chomp(my $startup_data = <$parent_sock>); # need this now, so allow blocking read
    my $supervisor_params = _unserialize($startup_data);

    @INC = @{ $supervisor_params->{inc} };

    my $worker_class = $supervisor_params->{class};
    $0 = sprintf "%s supervisor", $worker_class;

    die "no worker class provided" unless $worker_class;
    die "parent went away before I started" if getppid != $supervisor_params->{parent_pid};

    if (my $source_file = $supervisor_params->{source}) {
        unless (eval "require '$source_file'; 1") {
            die "failed to load worker class $worker_class from $source_file: $@";
        }
    }
    else {
        unless (eval "use $worker_class; 1") {
            die "failed to load worker class $worker_class: $@";
        }
    }

    my $servers = $supervisor_params->{servers};
    my $supervisor_pid = $$;

    # try to reap any worker processes, and start up any that are missing. also
    # starts up workers for the first time after they're added
    my (%kids, @open_slots);
    my $worker_checker = sub {
        # reap slots from dead kids
        while ((my $pid = waitpid(-1, WNOHANG)) > 0) {
            my $slot = delete $kids{$pid};
            unless ($slot) {
                warn "dead child $pid didn't own a slot";
                next;
            }
            push @open_slots, $slot;
        }

        return unless @open_slots;

        # refill lowest slots first
        @open_slots = sort {$a->[SLOT_NUM]<=>$b->[SLOT_NUM]} @open_slots;

        while (my $slot = shift @open_slots) {
            my $pid = fork;

            unless (defined $pid) {
                warn "fork failed: $!\n";
                # try again next time through
                return;
            }

            unless ($pid) {
                # run worker code in child

                my $slot = shift;
                my $worker_params = $slot->[SLOT_PARAMS];
                my $worker_class = $worker_params->{class};
                $0 = sprintf "%s #%d", $worker_class, $slot->[SLOT_NUM];

                my $worker = $worker_class->new($slot->[SLOT_NUM], $worker_params->{config}, $servers);

                die "failed to create $worker_class object" unless $worker;

                $worker->job_servers(@$servers);

                # each worker gets a unique function so we can ping it in wait_until_all_ready
                $worker->register_function(_ping_name($slot->[SLOT_ID]) => sub {
                    if ($worker->can('unregister_function')) {
                        # remove the function so it doesn't pollute server "status" command
                        $worker->unregister_function(_ping_name($slot->[SLOT_ID]));
                    }
                    return 1;
                });

                my $call_post_work = $worker->can('post_work');

                my $quitting = 1;
                $SIG{INT} = sub { $quitting = 1 };

                while (!$quitting) {
                    eval {
                        $worker->work(stop_if => sub {1});
                    };
                    $@ && warn "$worker_class [$$] failed: $@";

                    $worker->post_work if $call_post_work;

                    # bail if supervisor went away
                    last if getppid() != $supervisor_pid;
                }

                exit 0;
            }

            # parent is still supervisor
            $kids{$pid}{slot} = $slot;
        }
    };

    # get prompt notification of workers exiting
    $SIG{CHLD} = $worker_checker;

    # cleanup workers when exiting
    my $cleanup = sub {
        my $sig = shift;
        kill $sig, keys %kids;
        exit 0;
    };
    $SIG{INT} = $SIG{TERM} = $cleanup;

    # watch for parent going away
    _run_periodically(sub {
        if (getppid != $supervisor_params->{parent_pid}) {
            $cleanup->();
        }
    }, PARENT_CHECK_PERIOD);

    # look for additional instructions from parent spawner
    IO::Handle::blocking($parent_sock, 0);
    my $cxn = Gearman::WorkerSpawner::Supervisor::ParentConnection->new(
        $parent_sock,
        sub {
            my $lines = shift;
            push @open_slots, @{ _unserialize($_) } for @$lines;
            $worker_checker->();
        },
        $cleanup, # exit on loss of cxn to parent
    );

    # periodically check for children needing replacement; handler for SIGCHLD
    # should make this unnecessary except where it isn't delivered correctly
    _run_periodically($worker_checker, CHILD_CHECK_PERIOD);

    # run event loop forever
    Danga::Socket->EventLoop;
    die "Danga::Socket->EventLoop returned unexpectedly";
}

1;
