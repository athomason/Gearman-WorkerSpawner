package Gearman::Spawner::Singleton;

# class that encapsulates things of which there should only be one per process:
# - SIGCHLD handler
# - gearman server

use strict;
use warnings;

use Danga::Socket ();
use Gearman::Spawner::Util qw( run_periodically );
use POSIX qw( WNOHANG );
use Scalar::Util qw( weaken );

my $_instance;
sub instance {
    my $class = shift;
    return $_instance if $_instance;

    weaken($_instance = $class->new(@_));

    return $_instance;
}

sub new {
    my $class = shift;

    my $self = bless {
        sigchld         => 1,
        check_period    => 5,
        callbacks       => {},
        fallback        => undef, # called with ($pid, $exit_code) for processes which weren't registered with watch_pid
        old_sigchld     => undef, # called after our sigchld handler is
        @_,
    }, $class;

    my $reaper = sub { $self->reap };

    # fire callbacks after next loop if installing sigchld handler
    $SIG{CHLD} = sub {
        Danga::Socket->AddTimer(0, $reaper);
        $self->{old_sigchld}->(@_) if $self->{old_sigchld};
    } if $self->{sigchld};

    # ... and/or check for dead children periodically on a timer
    run_periodically($self->{check_period}, $reaper) if $self->{check_period};

    return $self;
}

sub reap {
    my $self = shift;
    while ((my $pid = waitpid(-1, WNOHANG)) > 0) {
        my $cb = delete $self->{callbacks}{$pid} || $self->{fallback};
        $cb->($pid, $?) if $cb;
    }
}

sub watch_pid {
    my $self = shift;
    my $pid = shift;
    my $callback = shift;

    $self->{callbacks}{$pid} = $callback;
}

# starts up a gearman server on a random local port. returns an arrayref
# containing the PID of the server process and the host:port spec where it is
# listening
#
# NB: this will block until the server is listening
sub auto_server {
    my $self = shift;

    return $self->{server} if $self->{server} && kill 0, $self->{server}[0];

    # ask OS for open listening port
    require IO::Socket::INET;
    my $sock = IO::Socket::INET->new(
        Proto     => 'tcp',
        Listen    => 1,
        Reuse     => 1,
    );
    die "no sock: $!" unless $sock;

    my $host = $sock->sockhost;
    my $port = $sock->sockport;
    my $addr = "0.0.0.0:$port";

    $sock->close;

    # fork a gearmand that watches for its parent to go away
    my $parent_pid = $$;
    my $pid = fork;
    die "fork failed: $!" unless defined $pid;

    if ($pid) {
        # parent: returns to caller

        # don't return until the server is contactable
        select undef, undef, undef, 0.1 until IO::Socket::INET->new($addr);
        return $self->{server} = [$pid, "$host:$port"];
    }
    else {
        # child: gearman server
        $0 = 'gearmand-Spawner';
        Danga::Socket->Reset;
        my $server = Gearman::Server->new;
        $server->create_listening_sock($port);

        # when parent dies, ppid resets to 1
        run_periodically($self->{check_period}, sub { exit if getppid != $parent_pid });

        Danga::Socket->EventLoop();
        exit 0;
    }
}

1;
