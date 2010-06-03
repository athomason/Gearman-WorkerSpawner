package Gearman::WorkerSpawner::Manager;

use strict;
use warnings;

use base 'Gearman::WorkerSpawner';

use IO::Socket::INET;

sub new {
    my $class = shift;
    my %opts = @_;

    my $self = bless $class->SUPER::new(%opts), $class;

    my $servers = $self->gearman_servers;

    # occasionally check if we should start up some new workers
    Gearman::WorkerSpawner::_run_periodically(sub {
        my $need_more = 0;
        SERVER:
        for my $server (@$servers) {
            my $monitor_sock = $self->{monitor_socks}{$server};
            unless ($monitor_sock) {
                $monitor_sock = $self->{monitor_socks}{$server} =
                    IO::Socket::INET->new(PeerAddr => $server);
                die "failed to connect to $server: $!" unless $monitor_sock;
            }
            unless (print $monitor_sock "status\n") {
                # try again next time
                delete $self->{monitor_socks}{$server};
                return;
            }
            while (<$monitor_sock>) {
                last if /^\.$/;
                my ($name, $queued, $working, $idle) = split;
                my $worker_config = $self->{config}{workers}{$name};
                if (
                    (defined $worker_config->{min} && $queued < $worker_config->{min}) ||
                    (defined $worker_config->{max} && $queued < $worker_config->{max})
                ) {
                    $need_more++;
                    last SERVER;
                }
            }
        }
        if ($need_more) {
            $verbose && warn "Spawning another $worker_increment\n";
            add_workers($self->{config}{worker_increment});

            $num = min($num, max(0, $max_workers - $total_workers)) if $max_workers;
            $worker_manager->add_worker(
                class           => 'GetPage',
                caller_source   => 1,
                num_workers     => $workers,
                config => {
                    proxy       => $proxy,
                    jar         => $jar,
                    bust_cache  => $bust_cache,
                },
            );
            $total_workers += $num;
        }
    }, $check_period);
}

sub add_workers {
    my $num = shift;
}

# suck urls off stdin
my $buf = '';
sub handle_input {
    my $data;
    $buf .= $data while sysread STDIN, $data, 1024;

    while ($buf =~ s/\n*([^\n]+)\n+//) {
        my $url = $1;
        $worker_manager->run_method(get_page => $url, sub {
            my $code = shift;
            printf "%3d %s\n", $code, substr $url, 0, 100;
        });
    }
}

$verbose && warn "Starting replay\n";

IO::Handle::blocking(\*STDIN, 0);
if (!$worker_only) {
    $verbose && warn "Watching STDIN for urls\n";
    Danga::Socket->AddOtherFds(fileno(STDIN), \&handle_input);
}
Danga::Socket->EventLoop();

__END__

=head1 NAME

Gearman::WorkerSpawner

=head1 SYNOPSIS

  perl set-dynamic towleroad.typepad.com
  tail -f /var/log/6a/today/tp/16/squid-local5.log \
      | grep towleroad \
      | grep -v "204.9.177" \
      | perl squid-parse.pl \
      | perl http-replay-gearman.pl --workers 100 --bust-cache

=head1 DESCRIPTION

This script reads URLs from stdin and dispatches Gearman workers to do GET
requests on them. Can optionally be directed to go through a proxy, eg a
perlbal.

=head1 OPTIONS

=over 4

=item --gearman <auto|hostspec>

May be "auto" to start up a local gearmand, or a comma-separated list of
"host:port" specs.

=item --workers <num>

Number of workers to start with.

=item --[no-]worker-only

Don't listen on STDIN for URLs; just run workers.

=item --check-period <secs>

How often to check gearmand "status" to see if the queue is backed up.

=item --worker-increment <num>

Number of workers to add if any gearmand status indicates that its queue is
deeper than the number of workers.

=item --max-workers <num>

Maximum number of workers to run.

=item --proxy <host>

Specify a hostname (or host:port) to proxy requests through. The original
request hostname will be passed via the Host: header. Useful for pointing at a
perlbal, for instance.

=item --cookies <jarfile>

Cookie file to use, probably in conjuction with set-dynamic.pl.

=item --[no-]bust-cache

Use "Pragma: No-cache" header in requests. Default off.

=item --[no-]verbose

Show verbose output. Default off.

=item --help

Show this help.

=back

=head1 AUTHOR

Adam Thomason, E<lt>athomason@sixapart.comE<gt>

=cut
