#!/usr/bin/env perl

my $offset = 0;

our $error_code = unpack('V', substr($response, $offset, 4));

my $error_code_handler = "./common/handle_response_err_code.pl";

my $err_code_handling_res = do $error_code_handler;

if ($@) {
    die $@;
}
if (!defined $err_code_handling_res) {
    die "ERROR: error code handler returned undef: $!\n";
}

$offset += 4;

my $res_val_key = '';
my $ok = '';
my $deleted = '';

while ($offset < $response_bytes) {
    $res_val_key = unpack('V', substr($response, $offset, 4));
    $offset += 4;

    if ($res_val_key == 1) {
        $ok = unpack('C', substr($response, $offset, 1));
        $offset += 1;
    } elsif ($res_val_key == 10) {
        $deleted = unpack('C', substr($response, $offset, 1));
        $offset += 1;
    } else {
        die "Received incorrect response value key $res_val_key";
    }
}

if ($ok == 0) {
    die "Queue failed to be created";
}

if ($deleted == 0) {
    2;
} else {
    1;
}