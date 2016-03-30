# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file:
# author: Jing Xu(Lee)
# email: leexu@hengtiansoft.com
# date: Sep 16, 2014
# description:


#!/usr/bin/perl

use strict;
use warnings;
use threads;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::Stress;
use Test::More tests => 1091;

my $node; 

sub test_run {
    my ($ret, $cguid);
    my $ncntr = 5;
    my $keyoffset = 1000;
    my $size = 0;
    my @prop = ([3, "no", "ZS_DURABILITY_HW_CRASH_SAFE" ],);
    #my @data = ([50, 64000, 625], [100, 128000, 625], [150, 512, 3750]);
    my @data = ([64, 16000, 300], [74, 32000, 300], [84, 64000, 300], [94, 128000, 300], [104, 48, 6000]);

    $ret = $node->start(ZS_REFORMAT  => 1,);
    like($ret, qr/OK.*/, 'Node Start: ZS_REFORMAT=1');

    foreach my $p(@prop){
    	for(1 .. $ncntr){
		my $cname = "ctr-$_"; 
		$ret = ZSOpen($node->conn(0),$cname,$$p[0],$size,"ZS_CTNR_CREATE",$$p[1],$$p[2]);
		like ($ret, qr/^OK.*/, $ret);
		$cguid = $1 if($ret =~ /OK cguid=(\d+)/);
		foreach my $d(@data){
			$ret = ZSSet($node->conn(0), $cguid, $keyoffset, $$d[0], $$d[1], $$d[2], "ZS_WRITE_MUST_NOT_EXIST");
			like ($ret, qr/^OK.*/, $ret);
			$ret = ZSGet($node->conn(0), $cguid, $keyoffset, $$d[0], $$d[1], $$d[2]);
			like ($ret, qr/^OK.*/, $ret);
			$ret = ZSDel($node->conn(0), $cguid, $keyoffset, $$d[0], $$d[2]);
			like ($ret, qr/^OK.*/, $ret);
		}

        for(1 .. 10){
            $ret = ZSClose($node->conn(0), $cguid);
            like ($ret, qr/^OK.*/, $ret);

            $ret = $node->stop();
            like($ret,qr/OK.*/,"Node Stop");
            $ret = $node->start(ZS_REFORMAT => 0);
            like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=0");

            $ret = ZSOpen($node->conn(0),$cname,$$p[0],$size,"ZS_CTNR_RW_MODE",$$p[1],$$p[2]);
            like ($ret, qr/^OK.*/, $ret);

	    foreach my $d(@data){
		    $ret = ZSSet($node->conn(0), $cguid, $keyoffset, $$d[0], $$d[1], $$d[2], "ZS_WRITE_MUST_NOT_EXIST");
		    like ($ret, qr/^OK.*/, $ret);
		    $ret = ZSGet($node->conn(0), $cguid, $keyoffset, $$d[0], $$d[1], $$d[2]);
		    like ($ret, qr/^OK.*/, $ret);
		    $ret = ZSDel($node->conn(0), $cguid, $keyoffset, $$d[0], $$d[2]);
		    like ($ret, qr/^OK.*/, $ret);
	    }

            my $mode  = ZSTransactionGetMode (
                $node->conn(0),
            );
            chomp($mode);

            if ($mode =~ /.*mode=1.*/){
                $ret = ZSRangeAll($node->conn(0), $cguid, 0);
                like ($ret, qr/^OK.*/, $ret);
            }elsif ($mode =~ /.*mode=2.*/){
                $ret = ZSEnumerate($node->conn(0), $cguid);
                like ($ret, qr/^OK.*/, $ret);
            }
        }

        $ret = ZSClose($node->conn(0), $cguid);
        like ($ret, qr/^OK.*/, $ret);
        $ret = ZSDelete($node->conn(0), $cguid);
        like ($ret, qr/^OK.*/, $ret);
       }
    }
}

sub test_init {
    $node = Fdftest::Node->new(
        ip     => "127.0.0.1",
        port   => "24422",
        nconn  => 10,
    );
}

sub test_clean {
    $node->stop();
    $node->set_ZS_prop(ZS_REFORMAT  => 1);

    return;
}

#
# main
#
{
    test_init();

    test_run();

    test_clean();
}

# clean ENV
END {
    $node->clean();
}
                
