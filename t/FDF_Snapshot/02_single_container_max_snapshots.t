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
use Test::More tests => 4515;

#tests =( 6*($nconn+1) + 2) * ($loop+1) + 4
my $node;
my $nconn = 256;
my $loop  = 10;

sub test_run {
    my $ret;
    my $cguid;
    my @threads;
    my $size = 1024;
	my @snap_seq;

    $ret = $node->start (
        #gdb_switch   => 1,
        ZS_REFORMAT => 1,
    );
    like ($ret, qr/^OK.*/, 'Node start');

    # Create containers with $nconn connections
    my $ctrname = 'ctrn-01';
    $ret = ZSOpen ($node->conn (0), $ctrname, 3, $size, "ZS_CTNR_CREATE", "yes");
    like ($ret, qr/^OK.*/, $ret);

    if ($ret =~ /^OK cguid=(\d+)/) {
        $cguid = $1;
    }else {
        return;
    }
	
	# Getsnapshots when container is empty.
	$ret = ZSGetSnapshots($node->conn(0), $cguid);
	like ($ret, qr/^OK.*/, $ret);

	# Create snapshot when container is empty. 
	$ret = ZSCreateSnapshot($node->conn(0), $cguid);
	like ($ret, qr/^OK.*/, $ret);
	if ($ret =~ /^OK(.*)snap_seq=(\d+)/) {
		$snap_seq[0] = $2;
	}

	my $i = 500;
	my @keysum;
	my @valuesum;
	my @counts;
	for ( 1 .. $i) { 
		my ($keyoffset, $keylen, $datalen, $nops) = (0+50*$_, 20, 500, 100);
		$ret = ZSSetGet ($node->conn(0), $cguid, $keyoffset, $keylen, $datalen, $nops);
		like ($ret, qr/^OK.*/, $ret);

=comment
		$ret = ZSGet ($node->conn(0), $cguid, $keyoffset, $keylen, $datalen, $nops);
		like ($ret, qr/^OK.*/, $ret);

		$ret = ZSDel ($node->conn(0), $cguid, $keyoffset, $keylen, $nops/5);
		like ($ret, qr/^OK.*/, $ret);
=cut
		# Create snapshot after set 10 objs.
		$ret = ZSCreateSnapshot($node->conn(0), $cguid);
		if ($ret =~ /OK.*/ || $ret =~ /SERVER_ERROR ZS_TOO_MANY_SNAPSHOTS.*/){
			like ($ret, qr/^.*/, $ret);
		}
		if ($ret =~ /^OK(.*)snap_seq=(\d+)/) {
			$snap_seq[$_] = $2;
		}
		$ret = ZSGetSnapshots($node->conn(0), $cguid);
		like ($ret, qr/^OK.*/, $ret);
	}

	my ($keyoffset, $keylen, $datalen, $nops) = (0, 30, 600, 1000);
	$ret = ZSSet ($node->conn(0), $cguid, $keyoffset, $keylen, $datalen, $nops);

	# After close container.	
	$ret = ZSClose ($node->conn(0), $cguid);
	like ($ret, qr/^OK.*/, $ret);

	for ( 1 .. $i ) {
    	$ret = ZSOpen ($node->conn (0), $ctrname, 3, $size, "ZS_CTNR_RW_MODE", "yes");
	    like ($ret, qr/^OK.*/, $ret);

		$ret = ZSGetSnapshots($node->conn(0), $cguid);
		like ($ret, qr/^OK.*/, $ret);

		$ret =  ZSClose ($node->conn (0), $cguid);
		like ($ret, qr/^OK.*/, $ret);
	}

    $ret = ZSOpen ($node->conn (0), $ctrname, 3, $size, "ZS_CTNR_RW_MODE", "yes");
	like ($ret, qr/^OK.*/, $ret);

	for ( 0 .. @snap_seq-1) {
		# Delete snapshot s0, s1.
    	$ret = ZSDeleteSnapshot($node->conn(0), $cguid, $snap_seq[$_]);
	    like ($ret, qr/^OK.*/, $ret);
	
    	$ret = ZSDeleteSnapshot($node->conn(0), $cguid, $snap_seq[$_]);
	    like ($ret, qr/ZS_SNAPSHOT_NOT_FOUND/, $ret);
	
		$ret = ZSGetSnapshots($node->conn(0), $cguid);
		like ($ret, qr/^OK.*/, $ret);
	}

	# After close container.	
	$ret = ZSClose ($node->conn(0), $cguid);
	like ($ret, qr/^OK.*/, $ret);

	$ret = ZSGetSnapshots($node->conn(0), $cguid);
	like ($ret, qr/^Error.*/, $ret);

	$ret = ZSDeleteSnapshot($node->conn(0), $cguid, $snap_seq[1]);
	like ($ret, qr/^Error.*/, $ret);	
	
	# After delete container.	
	$ret = ZSDelete ($node->conn(0), $cguid);
	like ($ret, qr/^OK.*/, $ret);

	$ret = ZSGetSnapshots($node->conn(0), $cguid);
	like ($ret, qr/^Error.*/, $ret);

	$ret = ZSDeleteSnapshot($node->conn(0), $cguid, $snap_seq[1]);
	like ($ret, qr/^Error.*/, $ret);	
	
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

