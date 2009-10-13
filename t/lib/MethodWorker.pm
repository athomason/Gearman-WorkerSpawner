package MethodWorker;

use strict;
use warnings;

use base 'Gearman::WorkerSpawner::BaseWorker';

sub new {
    my $class = shift;
    my MethodWorker $self = bless $class->SUPER::new(@_), $class;
    $self->register_method(adder => \&add);
    return $self;
}

sub add {
    my MethodWorker $self = shift;
    my $args = shift;
    return { sum => $self->{config}{left_hand} + $args->{right_hand} };
}

1;
