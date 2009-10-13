use strict;
use warnings;

use Test::More tests => 7;

use FindBin '$Bin';
use Gearman::WorkerSpawner;

my $left_hand = 3;
my $right_hand = 5;

my $spawner = Gearman::WorkerSpawner->new;

push @INC, "$Bin/lib";

# test that different workers coexist
$spawner->add_worker(class => 'TestWorker');

$spawner->add_worker(
    class        => 'MethodWorker',
    num_workers  => 2,
    config       => {
        left_hand => $left_hand,
    },
);

$spawner->run_method(constant => 0, sub {
    my $number = shift;
    is(ref $number, 'SCALAR', 'numeric scalar ref');
    is($$number, 123, 'numeric scalar value');
    $spawner->run_method(constant => 1, sub {
        my $string = shift;
        is(ref $string, 'SCALAR', 'string scalar ref');
        is($$string, 'string', 'string scalar value');
        $spawner->run_method(echo => 'foo', sub {
            my $echoed = shift;
            is(ref $echoed, 'SCALAR');
            is($$echoed, 'foo');
            $spawner->run_method(add => { right_hand => $right_hand }, sub {
                my $return = shift;
                is($return->{sum}, $left_hand + $right_hand);
                exit;
            });
        });
    });
});


Danga::Socket->EventLoop;
