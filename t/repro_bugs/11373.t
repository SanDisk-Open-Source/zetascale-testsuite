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
#use BSD::Resource;
use Test::More tests => 603;

#tests =( 6*($nconn+1) + 2) * ($loop+1) + 4
my $node;
my $nconn = 150;
my $loop  = 10;

sub worker {
    my ($connid, $cguid, $keyoffset, $keylen, $datalen, $nops) = @_;
    my ($ret, $msg);
    my $size = 0;

    $ret = ZSSet ($node->conn ($connid), $cguid, $keyoffset, $keylen, $datalen, $nops);
    like ($ret, qr/^OK.*/, $ret);
    #$ret = ZSFlushRandom ($node->conn ($connid), $cguid, $keyoffset);
    #like ($ret, qr/^OK.*/, $ret);
    #$ret = ZSEnumerate ($node->conn ($connid), $cguid);
    #like ($ret, qr/^OK.*/, $ret);
	$ret = ZSCreateSnapshot($node->conn(0), $cguid);
    like ($ret, qr/^OK.*/, $ret);
	$ret = ZSCreateSnapshot($node->conn(0), $cguid);
    like ($ret, qr/^OK.*/, $ret);
	$ret = ZSCreateSnapshot($node->conn(0), $cguid);
    like ($ret, qr/^OK.*/, $ret);
}


sub test_run {
    my $ret;
    my $cguid;
    my @threads;
    my $size = 0;
	my @snap_seq;
    #setrlimit(RLIMIT_NOFILE, 4096000,4096000);

    $ret = $node->start (
        ZS_REFORMAT => 1,
        threads	    => $nconn,
    );
    like ($ret, qr/^OK.*/, 'Node start');

    # Create containers with $nconn connections
    my $ctrname = 'ctrn-01';
    $ret = ZSOpen ($node->conn (0), $ctrname, 3, $size, "ZS_CTNR_CREATE", "no","ZS_DURABILITY_HW_CRASH_SAFE");
    like ($ret, qr/^OK.*/, $ret);

    if ($ret =~ /^OK cguid=(\d+)/) {
        $cguid = $1;
    }else {
        return;
    }
	
	# Getsnapshots when container is empty.
	$ret = ZSGetSnapshots($node->conn(0), $cguid);
	like ($ret, qr/^OK.*/, $ret);

	my $i = 100;
	my @keysum;
	my @valuesum;
	my @counts;
	for ( 1 .. $nconn) { 
		my ($keyoffset, $keylen, $datalen, $nops) = (0, 20 + $_,  5000+$_, 1000);
        push(@threads, threads->new (\&worker, $_, $cguid, $keyoffset, $keylen, $datalen, $nops));
	}
	$_->join for (@threads);
	
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

