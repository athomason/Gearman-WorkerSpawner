=head1 NAME

Gearman::WorkerSpawner::BaseWorker - Base class to simplifyimplifies the process of creating a Gearman worker for use with Gearman::WorkerSpawner

=head1 SYNOPSIS

    # create manager as normal. the contents of 'config' can be arbitrary; the
    # special 'max_jobs' option instructs the worker to exit cleanly after
    # performing that many jobs.

    my $worker_manager = Gearman::WorkerSpawner->new(gearmand => 'external');
    $worker_manager->add_worker(
        class  => 'AdditionWorker',
        config => {
            left_hand => 5,
            max_jobs  => 100,
        },
    );

    # invoke run_method instead of add_task for workers derived from
    # BaseWorker. serialization of options is handled for you, and if you only care
    # about the success case you can provide only that callback

    $worker_manager->run_method(adder => { right_hand => 3 }, sub {
        my $return = shift;
        print $return->{sum};
    });

    Danga::Socket->EventLoop;

    # in the worker:

    package MethodWorker;
    use base 'Gearman::WorkerSpawner::BaseWorker';

    # Gearman::WorkerSpawner will instantiate your class; the object will
    # contain populated 'config' and 'slot' fields
    sub new {
        my $class = shift;
        my $self = bless Gearman::WorkerSpawner::BaseWorker->new(@_), $class;
        print "I am worker $self->{slot}\n";
        $self->register_method(adder => \&add);
        return $self;
    }

    sub add {
        my MethodWorker $self = shift;
        my $args = shift;
        return { sum => $self->{config}{left_hand} + $args->{right_hand} };
    }

=cut

package Gearman::WorkerSpawner::BaseWorker;

use strict;
use warnings;

use base 'Gearman::Worker';

use fields (
    'config',
    'slot',
    'max_jobs',
    'jobs_done',
);

use Storable qw(nfreeze thaw);

sub new {
    my ($this, $slot, $config, $gearmands, $max_jobs) = @_;

    my $class = ref $this || $this;
    my Gearman::WorkerSpawner::BaseWorker $self = Gearman::Worker->new(job_servers => $gearmands);

    $self->{slot}      = $slot;
    $self->{config}    = $config;
    $self->{max_jobs}  = $config->{max_jobs} || undef;
    $self->{jobs_done} = 0;

    return bless $self, $class;
}

sub register_method {
    my Gearman::WorkerSpawner::BaseWorker $self = shift;
    my $name   = shift;
    my $method = shift;

    $self->register_function($name => sub {
        $self->{jobs_done}++;

        my $job = shift;
        my $arg = $job->arg;

        # deserialize argument
        my $param = thaw($job->arg);

        my $retval = $self->$method(ref $param eq 'SCALAR' ? $$param : $param);

        # serialize return value
        return \nfreeze(ref $retval ? $retval : \$retval);
    });
}

sub post_work {
    my Gearman::WorkerSpawner::BaseWorker $self = shift;
    return unless $self->{max_jobs};
    exit 0 if $self->{jobs_done} >= $self->{max_jobs};
}

1;
