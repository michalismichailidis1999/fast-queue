#!/usr/bin/env perl
use strict;
use warnings;
use IO::Socket::INET;

my $ip = $ARGV[0] or die "Usage: send_tcp_request.pl <ip> <port> <encoded> <response_handler> [timeout]\n";
my $port = $ARGV[1] or die "Usage: send_tcp_request.pl <ip> <port> <encoded> <response_handler> [timeout]\n";
my $encoded = $ARGV[2] or die "Usage: send_tcp_request.pl <ip> <port> <encoded> <response_handler> [timeout]\n";
my $response_handler = $ARGV[3] or die "Usage: send_tcp_request.pl <ip> <port> <encoded> <response_handler> [timeout]\n";
my $timeout = $ARGV[4] // 5;

my $sock = IO::Socket::INET->new(
    PeerAddr => $ip,
    PeerPort => $port,
    Proto    => 'tcp',
    Timeout  => $timeout
) or die "Cannot connect to cluster node: $!\n";

$sock->autoflush(1);

# ── parse encoded request string ──────────────────────────────────────
# format: "\i<val>|\i<val>|\s<val>|..."
# \i = convert value to 4 byte big-endian int
# \s = keep value as raw string bytes

my $request = '';

my @fields = split(/\|/, $encoded);
# split on | separator — produces list of fields
# example: ("\i45", "\i1", "\i4", "\i8", "\shello_world", ...)

foreach my $field (@fields) {
    # extract type prefix (first 2 chars) and value (rest)
    my $type  = substr($field, 0, 2); # first 2 chars: \i or \s
    my $value = substr($field, 2); # everything after first 2 chars

    if ($type eq '\\i') {
        # convert to 4 byte big-endian integer
        $request .= pack("V", int($value));
    } elsif ($type eq '\\s') {
        # keep as raw string bytes
        $request .= $value;
    } else {
        die "ERROR: unknown type prefix '$type' in field '$field'\n";
    }
}

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

$response_bytes = unpack('V', $header);

our $response = '';
our $body_bytes = $sock->recv($response, $response_bytes);

$sock->close();

if (!defined $body_bytes)
{
    die "Socket closed by server unexpectedly.";
}

if (length($response) != $response_bytes)
{
    die "Received invalid response body format.";
}

my $result = do $response_handler;

if ($@) {
    die "ERROR: response handler failed: $@\n";
}
if (!defined $result) {
    die "ERROR: response handler returned undef: $!\n";
}

print "OK";
exit 0;