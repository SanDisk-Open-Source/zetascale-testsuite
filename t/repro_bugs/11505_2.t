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
use Test::More tests => 18;

my $node;
my $nconn = 256;
my $loop  = 10;

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

    # Create containers with $nconn connections
    my $ctrname = 'ctrn-01';
    $ret = ZSOpen ($node->conn (0), $ctrname, 3, $size, "ZS_CTNR_CREATE", "no", "ZS_DURABILITY_HW_CRASH_SAFE");
    like ($ret, qr/^OK.*/, $ret);

    if ($ret =~ /^OK cguid=(\d+)/) {
        $cguid = $1;
    }else {
        return;
    }
	
	my ($keyoffset, $keylen, $datalen, $nops) = (0, 100, 5000, 10000);
	$ret = ZSSet ($node->conn(0), $cguid, $keyoffset, $keylen, $datalen, $nops);
	like ($ret, qr/^OK.*/, $ret);

	# Create snapshot after set 10 objs.
	$ret = ZSCreateSnapshot($node->conn(0), $cguid);
	like ($ret, qr/^OK.*/, $ret);
	if ($ret =~ /^OK(.*)snap_seq=(\d+)/) {
		$snap_seq[1] = $2;
	}

	# Update 0-5 objs and verify data by read obj
	($keyoffset, $keylen, $datalen, $nops) = (0, 100, 5010, 10000);
	$ret = ZSSet ($node->conn(0), $cguid, $keyoffset, $keylen, $datalen, $nops);
	like ($ret, qr/^OK.*/, $ret);

	$ret = ZSGet ($node->conn(0), $cguid, $keyoffset, $keylen, $datalen, $nops);
	like ($ret, qr/^OK.*/, $ret);

	$ret = ZSDeleteSnapshot($node->conn(0), $cguid, $snap_seq[1]);
	like ($ret, qr/^OK.*/, $ret);	

	$ret = ZSCreateSnapshot($node->conn(0), $cguid);
	like ($ret, qr/^OK.*/, $ret);
	if ($ret =~ /^OK(.*)snap_seq=(\d+)/) {
		$snap_seq[2] = $2;
	}

	for ( 1 .. 10 ) {
		system("sleep 1");
		$ret = ZSGet ($node->conn(0), $cguid, $keyoffset, $keylen, $datalen, $nops);
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

