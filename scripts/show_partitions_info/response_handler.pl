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
my $partitions = 0;
my $replication_factor = 0;

my $leader_id = 0;
my $leader_offset = 0;
my $leader_commited_offset = 0;

my $follower_id = 0;
my $follower_offset = 0;

my @partitions_info = ();

=begin comment

structure of partitions_info stored object

{
leader_id => int
leader_offset => unsigned long long
leader_commited_offset => unsigned long long
followers => [
    {
    follower_id => int
    follower_offset => int
    }
]
}

=cut

my $info;
my $follower_info;

while ($offset < $response_bytes) {
    $res_val_key = unpack('V', substr($response, $offset, 4));
    $offset += 4;

    if ($res_val_key == 4) {
        $partitions = unpack('V', substr($response, $offset, 4));
        $offset += 4;
    } elsif ($res_val_key == 22) {
        $replication_factor = unpack('V', substr($response, $offset, 4));
        $offset += 4;
    } elsif ($res_val_key == 25) {
        for my $i (0..$partitions-1) {
            $leader_id = unpack('V', substr($response, $offset, 4));
            $offset += 4;

            $leader_offset = unpack('V', substr($response, $offset, 8));
            $offset += 8;

            $leader_commited_offset = unpack('V', substr($response, $offset, 8));
            $offset += 8;

            $info = {
                leader_id => $leader_id,
                leader_offset => $leader_offset,
                leader_commited_offset => $leader_commited_offset,
                followers => []
            };

            if ($replication_factor > 0) {
                $follower_id = unpack('V', substr($response, $offset, 4));
                $offset += 4;

                $follower_offset = unpack('V', substr($response, $offset, 8));
                $offset += 8;

                $follower_info = {
                    follower_id => $follower_id,
                    follower_offset => $follower_offset
                };

                for my $j (0..$replication_factor-1) {
                    push @{ $info->{followers} }, $follower_info;
                }
            }

            push @partitions_info, $info;
        }
    } else {
        die "Received incorrect response value key $res_val_key";
    }
}

print STDERR "Partitions: $partitions";
print STDERR "Replication Factor: $replication_factor";

for my $i (0..$partitions-1) {
    $info = $partitions_info[$i];

    $leader_id = $info->{leader_id};
    $leader_offset = $info->{leader_offset};
    $leader_commited_offset = $info->{leader_commited_offset};

    print STDERR "Partition: $i";
    print STDERR "=========================\n";

    print STDERR "Leader id: $leader_id";
    print STDERR "Leader offset: $leader_offset";
    print STDERR "Leader commited offset: $leader_commited_offset";

    if ($replication_factor > 0) {
        for my $j (0..$replication_factor-1) {
            $follower_info = $info->{followers}[$j];

            $follower_id = $follower_info->{follower_id};
            $follower_offset = $follower_info->{follower_offset};

            print STDERR "\nReplica: $j\n";
            print STDERR "Follower id: $follower_id";
            print STDERR "Follower offset: $follower_offset";
            print STDERR "\n";
        }
    }

    print STDERR "=========================\n\n";
}

1;