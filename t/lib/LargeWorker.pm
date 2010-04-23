package LargeWorker;

use base 'Gearman::Worker';

sub new {
    my ($class, $slot, $config, $gearmands) = @_;

    # use some memory
    my $x = '1' x 100000;

    # use some cpu
    my $done = 0;
    $SIG{ALRM} = sub { $done++ };
    alarm 1;
    1 until $done;

    return bless Gearman::Worker->new(job_servers => $gearmands), $class;
}

1;
