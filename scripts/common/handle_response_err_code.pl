#!/usr/bin/env perl

if ($error_code == 0) {
	1;
} elsif ($error_code == 1) {
	die "Internal Server Error (1)";
} elsif ($error_code == 2) {
	die "Incorrect Request Body (2)";
} elsif ($error_code == 3) {
	die "Incorrect Partition Number (3)";
} elsif ($error_code == 4) {
	die "Queue Does Not Exist (4)";
} elsif ($error_code == 5) {
	die "Partition Does Not Exist (5)";
} elsif ($error_code == 6) {
	die "Incorrect Action (6)";
} elsif ($error_code == 7) {
	die "Incorrect Leader (7)";
} elsif ($error_code == 8) {
	die "Incorrect Message Count (8)";
} elsif ($error_code == 9) {
	die "Too Many Bytes Received (9)";
} elsif ($error_code == 10) {
	die "Unauthorized (10)";
} elsif ($error_code == 11) {
	die "Too Few Available Nodes (11)";
} elsif ($error_code == 12) {
	die "Unassigned Leadership (12)";
} elsif ($error_code == 13) {
	die "Consumer Not Found (13)";
} elsif ($error_code == 14) {
	die "Incorrect Consumer Group (14)";
} elsif ($error_code == 15) {
	die "Queue Already Exists (15)";
} elsif ($error_code == 16) {
	die "Consumer Unregistered (16)";
} elsif ($error_code == 17) {
	die "Data Node Not Registered Yet (17)";
} elsif ($error_code == 18) {
	die "Transaction Group Not Found (18)";
} elsif ($error_code == 19) {
	die "Transaction Group Unregistered (19)";
} elsif ($error_code == 20) {
	die "Queue Assigned To Transaction Group (20)";
} elsif ($error_code == 21) {
	die "Max Connection Limit Reached (21)";
} else {
	die "Unknown Error Code ($status_code)";
}