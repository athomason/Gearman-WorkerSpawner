use strict;
use warnings;

use Test::More tests => 1;

use FindBin '$Bin';
use Gearman::WorkerSpawner;

my $left_hand = 3;
my $right_hand = 5;

my $worker_manager = Gearman::WorkerSpawner->new;

push @INC, "$Bin/lib";

# test that different workers coexist
$worker_manager->add_worker(class => 'TestWorker');

$worker_manager->add_worker(
    class        => 'MethodWorker',
    num_workers  => 2,
    config       => {
        left_hand => $left_hand,
    },
);

$worker_manager->run_method(adder => { right_hand => $right_hand }, \&add_finished);

sub add_finished {
    my $return = shift;
    is($return->{sum}, $left_hand + $right_hand);
    exit;
}

Danga::Socket->EventLoop;
