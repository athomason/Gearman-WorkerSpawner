package Gearman::WorkerSpawner::Parent::SupervisorConnection;

use strict;
use warnings;

use base 'Danga::Socket';

use fields qw(
    buf_ref
);

sub new {
    my $class   = shift;
    my $sock    = shift;
    my $buf_ref = shift;

    my Gearman::WorkerSpawner::Parent::SupervisorConnection $self = bless $class->SUPER::new($sock), $class;

    $self->{buf_ref} = $buf_ref;

    $self->watch_read(1);

    return $self;
}

sub event_read {
    my Gearman::WorkerSpawner::Parent::SupervisorConnection $self = shift;

    my $input = $self->read(128 * 1024);
    unless (defined $input) {
        $self->close;
        return;
    }

    ${ $self->{buf_ref} } .= $input;
}

1;
