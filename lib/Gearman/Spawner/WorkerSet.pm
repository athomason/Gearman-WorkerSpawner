package Gearman::Spawner::WorkerSet;

use strict;
use warnings;

use Carp qw( croak );
use Gearman::Client;
use Gearman::Spawner::Util qw( :slots ping_name );

sub new {
    my $class = shift;
    my %params = @_;

    my $self = bless {
        num_workers => delete $params{num_workers},
        params      => \%params,
    }, $class;

    $self->{slots} = [];

    return $self;
}

sub init {
    my $self = shift;
    my $connection = shift;
    $self->{connection} = $connection;
    $connection->cmd(setup => $self->{params});
    $self->num_workers($self->{num_workers});
}

=item $spawner->num_workers([$count])

Gets or sets the number of workers in this set.

=cut

sub num_workers {
    my $self = shift;
    my $num = shift;

    die 'not connected yet' unless $self->{connection};

    return $self->{num_workers} unless defined $num;
    $self->{num_workers} = $num;

    if ($num > @{ $self->{slots} }) {
        my $slots = $self->{slots};
        for my $slot_num (0 .. $self->{num_workers} - 1) {
            my $worker_id = sprintf '%d:%s/%s', $slot_num, $self->{params}{class}, substr rand() . '0'x16, 2, 16;
            push @$slots, [$slot_num, $worker_id, $self->{params}{pingable}];
            $self->{connection}->cmd(add_slot => [$slot_num, $worker_id]);
        }
    }
    else {
        while (@{ $self->{slots} } > $num) {
            my $slot = pop @{ $self->{slots} };
            $self->{connection}->cmd(delete_slot => $slot->[SLOT_NUM]);
        }
    }

    return $num;
}

=item $spawner->wait_until_all_ready()

Blocks until all workers are ready to accept jobs. Only workers which have not
been pinged before with this method will be checked. The worker set must have
been created with the I<pingable> option.

=cut

sub wait_until_all_ready {
    my $self = shift;
    my $timeout = shift || 0.1;

    croak "this WorkerSet isn't pingable" unless $self->{params}{pingable};

    my $client = Gearman::Client->new(job_servers => $self->{params}{servers});
    my $task_set = $client->new_task_set;

    for my $slot (@{ $self->{slots} }) {
        next unless $slot->[SLOT_PINGABLE];
        $task_set->add_task(
            ping_name($slot->[SLOT_ID]),
            undef,
            {
                timeout     => $timeout,
                retry_count => 1_000_000,
            }
        );
        $slot->[SLOT_PINGABLE] = 0;
    }

    $task_set->wait;
}

1;
