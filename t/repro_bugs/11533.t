# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file: basic.pl
# author: yiwen sun
# email: yiwensun@hengtiansoft.com
# date: Oct 15, 2012
# description: basic sample for testcase

#!/usr/bin/perl

use strict;
use warnings;
use Switch;
use threads;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Stress;
use Fdftest::Node;
use Test::More tests => 21;

my $node;
my $nconn = 256;
my $loop  = 10;

sub worker {
	my ($conn, $cguid, $keyoffset, $keylen, $datalen, $nops) = @_;
	my $ret = ZSSet ($node->conn(0), $cguid, $keyoffset, $keylen, $datalen, $nops);
	like ($ret, qr/^OK.*/, $ret);
}

sub read{
	my ($conn, $cguid, $keyoffset, $keylen, $datalen, $nops) = @_;
	my $ret = ZSGet ($node->conn(0), $cguid, $keyoffset, $keylen, $datalen, $nops);
	like ($ret, qr/^OK.*/, $ret);
}

sub test_run {
    my $ret;
    my $cguid;
    my @threads;
    my $size = 0;
	my @snap_seq;

    $ret = $node->start (
        gdb_switch   => 1,
        ZS_REFORMAT => 1,
    );
    like ($ret, qr/^OK.*/, 'Node start');

	for ( 1 .. 2 ) {
    # Create containers with $nconn connections
    my $ctrname = 'ctrn-01';
    $ret = ZSOpen ($node->conn (0), $ctrname, 3, $size, "ZS_CTNR_CREATE", "no", "ZS_DURABILITY_HW_CRASH_SAFE");
    like ($ret, qr/^OK.*/, $ret);

    if ($ret =~ /^OK cguid=(\d+)/) {
        $cguid = $1;
    }else {
        return;
    }
	
	my ($keyoffset, $keylen, $datalen, $nops) = (0, int(rand(200)),int(rand(2048)), 100);
	my @threads = ();
	push(@threads, threads->new(\&worker, $node->conn($_), $cguid, $keyoffset, $keylen, $datalen, $nops));
	$_->join for (@threads);

	# Create snapshot after set 10 objs.
	$ret = ZSCreateSnapshot($node->conn(0), $cguid);
	like ($ret, qr/^OK.*/, $ret);
	if ($ret =~ /^OK(.*)snap_seq=(\d+)/) {
		$snap_seq[1] = $2;
	}

	$datalen = $datalen + 100;
	@threads = ();
	push(@threads, threads->new(\&worker, $node->conn($_), $cguid, $keyoffset, $keylen, $datalen, $nops));
	$_->join for (@threads);

	$ret = ZSDeleteSnapshot($node->conn(0), $cguid, $snap_seq[1]);
	like ($ret, qr/^OK.*/, $ret);	

	$ret = ZSClose($node->conn(0), $cguid);
	like ($ret, qr/^OK.*/, $ret);

	$node->stop ();
    $ret = $node->start (
        ZS_REFORMAT => 0,
    );
	like ($ret, qr/^OK.*/, "Node restart");

    $ret = ZSOpen ($node->conn (0), $ctrname, 3, $size, "ZS_CTNR_RW_MODE", "no", "ZS_DURABILITY_HW_CRASH_SAFE");
    like ($ret, qr/^OK.*/, $ret);

	$ret = ZSClose($node->conn(0), $cguid);
	like ($ret, qr/^OK.*/, $ret);

	$ret = ZSDelete($node->conn(0), $cguid);
	like ($ret, qr/^OK.*/, $ret);

	}

	return;
}

sub test_init {
    $node = Fdftest::Node->new (
        ip    => "127.0.0.1",
        port  => "24422",
        nconn => $nconn,
    );
    return;
}

sub test_clean {
    $node->stop ();
    $node->set_ZS_prop (ZS_REFORMAT => 1);
    return;
}

#
# main
#
{
    test_init ();

    test_run ();

    test_clean ();
}

# clean ENV
END {
    $node->clean ();
}
