#----------------------------------------------------------------------------
# ZetaScale
# Copyright (c) 2016, SanDisk Corp. and/or all its affiliates.
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU Lesser General Public License version 2.1 as published by the Free
# Software Foundation;
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License v2.1 for more details.
#
# A copy of the GNU Lesser General Public License v2.1 is provided with this package and
# can also be found at: http:#opensource.org/licenses/LGPL-2.1
# You should have received a copy of the GNU Lesser General Public License along with
# this program; if not, write to the Free Software Foundation, Inc., 59 Temple
# Place, Suite 330, Boston, MA 02111-1307 USA.
#----------------------------------------------------------------------------

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
use Test::More tests => 30;

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
        gdb_switch   => 1,
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


	my ($keyoffset, $keylen, $datalen, $nops) = (0, 20, 500, 10);
	$ret = ZSSet ($node->conn(0), $cguid, $keyoffset, $keylen, $datalen, $nops);
	like ($ret, qr/^OK.*/, $ret);


	# Create snapshot after set 10 objs.
	$ret = ZSCreateSnapshot($node->conn(0), $cguid);
	like ($ret, qr/^OK.*/, $ret);
	if ($ret =~ /^OK(.*)snap_seq=(\d+)/) {
		$snap_seq[1] = $2;
	}

	# Update 0-5 objs and verify data by read obj
	($keyoffset, $keylen, $datalen, $nops) = (0, 20, 600, 5);
	$ret = ZSSet ($node->conn(0), $cguid, $keyoffset, $keylen, $datalen, $nops);
	like ($ret, qr/^OK.*/, $ret);
	$ret = ZSGet ($node->conn(0), $cguid, $keyoffset, $keylen, $datalen, $nops);
	like ($ret, qr/^OK.*/, $ret);

	$keyoffset	= 5;
	$datalen	= 500;
	$ret = ZSGet ($node->conn(0), $cguid, $keyoffset, $keylen, $datalen, $nops);
	like ($ret, qr/^OK.*/, $ret);

	$ret = ZSCreateSnapshot($node->conn(0), $cguid);
	like ($ret, qr/^OK.*/, $ret);
	if ($ret =~ /^OK(.*)snap_seq=(\d+)/) {
		$snap_seq[2] = $2;
	}

	$keyoffset	= 5;
	$datalen	= 600;
	$ret = ZSDel ($node->conn(0), $cguid, $keyoffset, $keylen, $nops);
	like ($ret, qr/^OK.*/, $ret);

	$ret = ZSCreateSnapshot($node->conn(0), $cguid);
	like ($ret, qr/^OK.*/, $ret);
	if ($ret =~ /^OK(.*)snap_seq=(\d+)/) {
		$snap_seq[3] = $2;
	}

	$ret = ZSGetSnapshots($node->conn(0), $cguid);
	like ($ret, qr/^OK.*/, $ret);

	# Delete snapshot s0, s1.
	$ret = ZSDeleteSnapshot($node->conn(0), $cguid, $snap_seq[0]);
	like ($ret, qr/^OK.*/, $ret);	

	$ret = ZSDeleteSnapshot($node->conn(0), $cguid, $snap_seq[1]);
	like ($ret, qr/^OK.*/, $ret);	

	$ret = ZSGetSnapshots($node->conn(0), $cguid);
	like ($ret, qr/^OK.*/, $ret);

	# After close container.	
	$ret = ZSClose ($node->conn(0), $cguid);
	like ($ret, qr/^OK.*/, $ret);

	$ret = ZSGetSnapshots($node->conn(0), $cguid);
	like ($ret, qr/^Error.*/, $ret);

    $ctrname = 'ctrn-01';
    $ret = ZSOpen ($node->conn (0), $ctrname, 3, $size, "ZS_CTNR_RW_MODE", "yes");
    like ($ret, qr/^OK.*/, $ret);

	$ret = ZSGetSnapshots($node->conn(0), $cguid);
	like ($ret, qr/^OK.*/, $ret);

	$ret = ZSCreateSnapshot($node->conn(0), $cguid);
	like ($ret, qr/^OK.*/, $ret);
	if ($ret =~ /^OK(.*)snap_seq=(\d+)/) {
		$snap_seq[4] = $2;
	}

	$ret = ZSDeleteSnapshot($node->conn(0), $cguid, $snap_seq[1]);
	like ($ret, qr/ZS_SNAPSHOT_NOT_FOUND/, $ret);	
	
	$ret = ZSGetSnapshots($node->conn(0), $cguid);
	like ($ret, qr/^OK.*/, $ret);

	$ret = ZSDeleteSnapshot($node->conn(0), $cguid, $snap_seq[2]);
	like ($ret, qr/^OK.*/, $ret);	
	
	$ret = ZSGetSnapshots($node->conn(0), $cguid);
	like ($ret, qr/^OK.*/, $ret);

	$ret = ZSDeleteSnapshot($node->conn(0), $cguid, $snap_seq[3]);
	like ($ret, qr/^OK.*/, $ret);	
	
	$ret = ZSGetSnapshots($node->conn(0), $cguid);
	like ($ret, qr/^OK.*/, $ret);
	
	$ret = ZSDelete($node->conn(0), $cguid);
	like ($ret, qr/^OK.*/, $ret);

	$ret = ZSGetSnapshots($node->conn(0), $cguid);
	like ($ret, qr/^Error.*/, $ret);

	$ret = ZSDeleteSnapshot($node->conn(0), $cguid, $snap_seq[4]);
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

