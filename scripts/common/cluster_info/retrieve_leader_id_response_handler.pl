#!/usr/bin/env perl

my $offset = 0;

our $error_code = unpack('V', substr($response, $offset, 4));

my $error_code_handler = "./common/handle_response_err_code.pl";

my $err_code_handling_res = do $error_code_handler;

if ($@) {
    die $@;
}
elsif (!defined $err_code_handling_res) {
    die "ERROR: error code handler returned undef: $!\n";
}

$offset += 4;

my $res_val_key = '';
my $leader_id = '';

while ($offset < $response_bytes) {
    $res_val_key = unpack('V', substr($response, $offset, 4));
    $offset += 4;

    if ($res_val_key == 2) {
        $leader_id = unpack('V', substr($response, $offset, 4));
        $offset += 4;
    } else {
        die "Received incorrect response value key $res_val_key";
    }
}

if ($leader_id <= 0) {
    die "No leader elected yet";
}

$leader_id;