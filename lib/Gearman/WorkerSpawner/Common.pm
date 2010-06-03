package Gearman::WorkerSpawner::Common;

use strict;
use warnings;

use base 'Exporter';

our @EXPORT = qw( _run_periodically _serialize _unserialize _ping_name );

use Danga::Socket ();
use Storable qw();

# takes a subref and a number of seconds, and runs the sub that often
sub _run_periodically {
    my $sub    = shift;
    my $period = shift;
    my $recycler;
    $recycler = sub {
        $sub->();
        Danga::Socket->AddTimer($period, $recycler);
    };
    Danga::Socket->AddTimer(0, $recycler);
}

sub _serialize {
    return unpack('h*', Storable::freeze shift) . "\n";
}

sub _unserialize {
    return Storable::thaw(pack 'h*', shift);
}

sub _ping_name {
    return 'ping_' . $_[0];
}

1;
