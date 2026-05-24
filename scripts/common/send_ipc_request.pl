#!/usr/bin/env perl
use strict;
use warnings;

# ════════════════════════════════════════════════════════
# Platform detection
# ════════════════════════════════════════════════════════
my $IS_WINDOWS = $^O eq 'MSWin32';

my $cmd = $ARGV[0] or die "Usage: create_pipe_connection.pl <command> <request_bytes> [timeout]\n";
my $req_body = $ARGV[1] or die "Usage: create_pipe_connection.pl <command> <request_bytes> [timeout]\n";
my $timeout = $ARGV[2] // 5;

# ════════════════════════════════════════════════════════
# Windows — Win32 Named Pipes via Win32API::File
# ════════════════════════════════════════════════════════
if ($IS_WINDOWS) {
    require Win32API::File;
    Win32API::File->import(qw(
        CreateFile   CloseHandle
        ReadFile     WriteFile
        CreateNamedPipe ConnectNamedPipe
        GENERIC_READ GENERIC_WRITE
        OPEN_EXISTING
        PIPE_ACCESS_INBOUND
        PIPE_TYPE_BYTE PIPE_WAIT
        FILE_FLAG_OVERLAPPED
        INVALID_HANDLE_VALUE
    ));

    my $broker_pipe = '\\\\.\\pipe\\ipc_receiver.pipe';
    my $resp_name   = "\\\\.\\pipe\\${cmd}_$$"; # $$ = PID

    # 1. create response pipe BEFORE sending request
    my $resp_handle = CreateNamedPipe(
        $resp_name,
        PIPE_ACCESS_INBOUND(),
        PIPE_TYPE_BYTE() | PIPE_WAIT(),
        1, # max instances
        4096, 4096,
        $timeout * 1000,
        []
    ) or die "CreateNamedPipe failed: $^E\n";

    # 2. open broker pipe and send request
    my $h = CreateFile(
        $broker_pipe,
        GENERIC_WRITE(),
        0, [],
        OPEN_EXISTING(),
        0, []
    ) or die "Cannot open cluster node read pipe: $^E\n";

    my $resp_name_len = length($resp_name);
    my $req_body_len  = length($req_body);

    my $payload = pack("N", $resp_name_len)   # 4 bytes — resp_name length
            . $resp_name                      # $resp_name_len bytes — resp_name
            . pack("N", $req_body_len)        # 4 bytes — req_body length
            . $req_body;                      # $req_body_len bytes — req_body

    die "ERROR: request too large (" . length($payload) . " bytes, max 4096)\n"
        if length($payload) > 4096;

    my $request = pack("A4096", $payload); # pad with \0 to exactly 4096 bytes

    my $written  = 0;
    WriteFile($h, $request, length($request), $written, [])
        or die "WriteFile failed: $^E\n";

    CloseHandle($h);

    # 3. wait for server — handle race condition
    my $connected = ConnectNamedPipe($resp_handle, []);

    if (!$connected) {
        my $err = Win32::GetLastError();

        if ($err == $ERROR_PIPE_CONNECTED) {
            # server is still connected — normal slow path, proceed to read
        }
        elsif ($err == $ERROR_NO_DATA) {
            # server already wrote and disconnected (fast server, race condition)
            # data is still in the buffer — safe to read below
            # no action needed, fall through to ReadFile
        }
        else {
            CloseHandle($resp_handle);
            die "ConnectNamedPipe failed: error $err\n";
        }
    }

    my $response = "";
    my $bytes_read = 0;

    ReadFile($resp_handle, $response, 4096, $bytes_read, [])
        or die "ReadFile failed: $^E\n";

    CloseHandle($resp_handle);

    print $response;

    exit 0;
}

# ════════════════════════════════════════════════════════
# Linux — POSIX named pipes (FIFO)
# ════════════════════════════════════════════════════════
use POSIX qw(mkfifo);

my $broker_pipe = '/tmp/ipc_receiver.pipe';
my $resp_pipe   = "/tmp/${cmd}_$$.pipe"; # $$ = PID

# check broker is running
die "ERROR: broker pipe not found at $broker_pipe\n"
    unless -p $broker_pipe;

# cleanup on any exit
local $SIG{INT} = sub { unlink $resp_pipe; exit 1; };
local $SIG{TERM} = sub { unlink $resp_pipe; exit 1; };

# 1. create personal response pipe
mkfifo($resp_pipe, 0666)
    or die "mkfifo failed: $!\n";

# 2. send request — open + write + close in one shot
open(my $fh, '>', $broker_pipe)
    or do { unlink $resp_pipe; die "Cannot open broker pipe: $!\n"; };

my $resp_name_len = length($resp_name);
my $req_body_len  = length($req_body);

my $payload = pack("N", $resp_name_len)   # 4 bytes — resp_name length
        . $resp_name                      # $resp_name_len bytes — resp_name
        . pack("N", $req_body_len)        # 4 bytes — req_body length
        . $req_body;                      # $req_body_len bytes — req_body

die "ERROR: request too large (" . length($payload) . " bytes, max 4096)\n"
    if length($payload) > 4096;

my $request = pack("A4096", $payload); # pad with \0 to exactly 4096 bytes

# disable any newline translation
# ensures raw bytes are written as-is
binmode($fh);

# write all 4096 bytes
print $fh $request;

# signal EOF to server
close($fh);

# 3. block until server writes response, with timeout
my $response = do {
    local $SIG{ALRM} = sub {
        unlink $resp_pipe;
        die "ERROR: timeout after ${timeout}s waiting for response\n";
    };

    alarm($timeout);

    open(my $resp_fh, '<', $resp_pipe)
        or do { unlink $resp_pipe; die "Cannot open resp pipe: $!\n"; };

    # read raw bytes, no translation
    binmode($resp_fh);

    my $data;
    read($resp_fh, $data, 4096) # read up to 4096 bytes of response
        or do { unlink $resp_pipe; die "Cannot read resp pipe: $!\n"; };

    close($resp_fh);
    alarm(0); # cancel alarm

    $data;
};

unlink $resp_pipe;

print $response;

exit 0;