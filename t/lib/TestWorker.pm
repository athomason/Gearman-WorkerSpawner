package TestWorker;

use base 'Gearman::Worker';

sub new {
    my ($class, $slot, $config, $gearmands) = @_;
    my TestWorker $self = Gearman::Worker->new(job_servers => $gearmands);
    bless $self, $class;
    $self->register_function(testfunc => \&testfunc);
    return $self;
}

sub testfunc {
    my $job = shift;
    my $arg = $job->arg;

    return $arg + 5;
}

1;
