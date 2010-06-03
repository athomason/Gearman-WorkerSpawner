package Gearman::WorkerSpawner::Supervisor::ParentConnection;

use strict;
use warnings;

use base 'Danga::Socket';

use fields qw(
    read_buf
    line_cb
    err_cb
);

sub new {
    my $class   = shift;
    my $pipe    = shift;
    my $line_cb = shift;
    my $err_cb  = shift;

    my Gearman::WorkerSpawner::Supervisor::ParentConnection $self = bless $class->SUPER::new($pipe), $class;

    $self->{line_cb} = $line_cb;
    $self->{err_cb} = $err_cb;

    $self->watch_read(1);

    return $self;
}

sub event_read {
    my Gearman::WorkerSpawner::Supervisor::ParentConnection $self = shift;

    my $input = $self->read(128 * 1024);
    unless (defined $input) {
        $self->close;
        return;
    }

    my @lines;
    while ($input =~ s/^([^\n]+)\n+//) {
        push @lines, $1;
    }
    $self->push_back_read($input) if length $input;

    $self->{line_cb}->(\@lines) if @lines;
}

no warnings 'once';

# close on any error
*event_err = *event_hup = \&close;

sub close {
    my Gearman::WorkerSpawner::Supervisor::ParentConnection $self = shift;
    $self->{err_cb}->(@_);
    $self->SUPER::close(@_);
}

1;
