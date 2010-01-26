package Gearman::WorkerSpawner::BaseWorker::Client;

=head1 NAME

Gearman::WorkerSpawner::BaseWorker::Client - Gearman::Client wrapper for use
with Gearman::WorkerSpawner::BaseWorker workers

=head1 SYNOPSIS

    my $client = Gearman::WorkerSpawner::BaseWorker::Client->new;
    $client->job_servers('127.0.0.1');
    my @retvals = $client->run_method(adder => { right_hand => 3 });

=head1 DESCRIPTION

This class implements the run_method function from Gearman::WorkerSpawner in
the synchronous Gearman::Client client. Instead of calling an on_complete
callback, the return value of the job is returned.

=cut

use strict;
use warnings;

use base 'Gearman::Client';

use Storable qw/ nfreeze thaw /;

sub new {
    my $ref = shift;
    my $class = ref $ref || $ref;

    return bless Gearman::Client->new(@_), $class;
}

sub run_method {
    my $self = shift;
    my ($methodname, $arg) = @_;

    my $frozen_arg = \nfreeze([$arg]);

    my $ref_to_frozen_retval = $self->do_task($methodname => $frozen_arg);

    if (!$ref_to_frozen_retval || ref $ref_to_frozen_retval ne 'SCALAR') {
        die "marshaling error";
    }

    my $rets = eval { thaw($$ref_to_frozen_retval) };
    die "unmarshaling error: $@" if $@;
    die "unmarshaling error (incompatible clients?)" if ref $rets ne 'ARRAY';

    return @$rets;
}

1;
