package TestWorker;

use base 'Gearman::Worker';

use fields 'args';

use Storable qw/ nfreeze thaw /;

sub new {
   my $class = shift;
   my $slot  = shift;
   my $args = shift;
   my TestWorker $self = Gearman::Worker->new;
   bless $self, $class;
   $self->{args} = $args;
   $self->register_function(testfunc => sub { $self->testfunc(@_) });
   return $self;
}

sub testfunc {
    my TestWorker $self = shift;
    my $job = shift;
    my $args = thaw($job->arg);

    return nfreeze([1 + $args->{value}, $self->{args}]);
}

1;
