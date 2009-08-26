use strict;
use warnings;

use Test::More tests => 5;
use Storable qw/ nfreeze thaw /;

use FindBin '$Bin';

BEGIN { use_ok('Gearman::WorkerSpawner', ':all') };

my $spawner = Gearman::WorkerSpawner->new(gearmand => 'external');

my %args = (foo => 2, bar => 3.14);

$spawner->add_worker(
   class        => 'TestWorker',
   source       => "$Bin/lib/TestWorker.pm",
   num_workers  => 1,
   worker_args  => \%args,
);

$spawner->add_task(Gearman::Task->new(testfunc => \nfreeze({value => 3}), {
    on_complete => sub {
        my $ref = shift;
        is(ref $ref, 'SCALAR', 'got ref back');
        my $result = eval { thaw $$ref };
        ok(!$@, 'thawed result');
        is($result->[0], 4, 'function computed value'); 
        is_deeply($result->[1], \%args, 'worker args were passed and stored'); 
        exit;
    },
}));

Danga::Socket->EventLoop;
