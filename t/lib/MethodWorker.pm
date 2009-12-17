package MethodWorker;

use strict;
use warnings;

use base 'Gearman::WorkerSpawner::BaseWorker';

sub new {
    my $class = shift;
    my MethodWorker $self = bless $class->SUPER::new(@_), $class;
    $self->register_method('constant');
    $self->register_method('echo');
    $self->register_method('echo_ref');
    $self->register_method('add');
    return $self;
}

sub constant {
    my MethodWorker $self = shift;
    my $arg = shift;
    if ($arg) {
        return "string";
    }
    else {
        return 123;
    }
}

sub echo {
    my MethodWorker $self = shift;
    my $arg = shift;
    return $arg;
}

sub echo_ref {
    my MethodWorker $self = shift;
    my $arg_ref = shift;
    my $arg = $$arg_ref;
    return \$arg;
}

sub add {
    my MethodWorker $self = shift;
    my $args = shift;
    return { sum => $self->{config}{left_hand} + $args->{right_hand} };
}

1;
