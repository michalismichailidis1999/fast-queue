#!/usr/bin/env perl
use strict;
use warnings;
use IO::Socket::INET;

my $ip = $ARGV[0] or die "Usage: send_tcp_request.pl <ip> <port> <request> [timeout]\n";
my $port = $ARGV[1] or die "Usage: send_tcp_request.pl <ip> <port> <request> [timeout]\n";
my $request = $ARGV[2] or die "Usage: send_tcp_request.pl <ip> <port> <request> [timeout]\n";
my $timeout = $ARGV[3] // 5;

my $sock = IO::Socket::INET->new(
    PeerAddr => $ip,
    PeerPort => $port,
    Proto    => 'tcp',
    Timeout  => $timeout
) or die "Cannot connect to cluster node: $!\n";

$sock->autoflush(1);

$sock->send($request);

my $response_bytes = 0;
my $header = '';

my $header_bytes = $sock->recv($header, 4);

if (!defined $header_bytes)
{
    $sock->close();
    die "Socket closed by server unexpectedly.";
}

if (length($header) != 4)
{
    $sock->close();
    die "Received invalid response header format.";
}

$response_bytes = unpack('N', $header);

my $response = '';
my $body_bytes = $sock->recv($response, $response_bytes);

$sock->close();

if (!defined $body_bytes)
{
    die "Socket closed by server unexpectedly.";
}

if (length($response) != $response_bytes)
{
    die "Received invalid response body format.";
}

print $response;
exit 0;