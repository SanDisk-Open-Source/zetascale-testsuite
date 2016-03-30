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
use Test::More tests =>  101;

my $node;
my $nconn = 256;
my $ncntr = 20;
my $loop  = 2;

sub worker {
	my ($conn_id, $cguid) = @_;
	my ($ret, $snap_seq, $chksum_base, $chksum, $count_base, $count);
	my ($keyoffset, $keylen, $datalen, $nops) = (0+$cguid+$conn_id, int(rand(250)), int(rand(8192)), 10000);
	$ret = ZSSetGet ($node->conn($conn_id), $cguid, $keyoffset, $keylen, $datalen, $nops) ;
	like ($ret, qr/^OK.*/, $ret);

    $ret = ZSCreateSnapshot($node->conn($conn_id), $cguid);
    like ($ret, qr/^OK.*/, $ret);
    if ($ret =~ /^OK(.*)snap_seq=(\d+)/) {
        $snap_seq = $2;
    }else {
		$snap_seq = 0;
	}
}


sub test_run {
    my $ret;
    my @cguids;
    my @threads;
    my $size = 0;
	my @snap_seq;

    $ret = $node->start (
        gdb_switch   => 1,
        ZS_REFORMAT => 1,
    );
    like ($ret, qr/^OK.*/, 'Node start');

    # Create containers with $nconn connections
	for ( 1 .. $ncntr ) {
    	my $ctrname = 'ctrn-'.$_;
	    $ret = ZSOpen ($node->conn (0), $ctrname, 3, $size, "ZS_CTNR_CREATE", "no");
	    like ($ret, qr/^OK.*/, $ret);

    	if ($ret =~ /^OK cguid=(\d+)/) {
	        $cguids[$_] = $1;
	    }else {
    	    return;
	    }
	}
	
	for ( 1 .. $loop ) {
		@threads=();
		for ( 1 .. $ncntr ){
			push(@threads, threads->new (\&worker, $_, $cguids[$_]));
		}
		$_->join for (@threads);
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

