# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file:
# author: YouyouCai
# email: youyoucai@hengtiansoft.com
# date: Mar 09, 2015
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
use Test::More tests => 2003;
#use Test::More 'no_plan';

my $node; 

sub test_run {
    my ($ret, $cguid, @cguids, $ncntr1, $ncntr2);
    my $size = 0;
    my @cnums = (900,100);
    my @ctr_type = ("BTREE","BTREE");
    my @prop  = ( [3,"ZS_DURABILITY_HW_CRASH_SAFE","no"]);
    my $zs_max_num_cntrs = 0;

    #set ZS_MAX_NUM_CONTAINERS
    $zs_max_num_cntrs = $cnums[0] +10;
    $node -> set_ZS_prop(ZS_MAX_NUM_CONTAINERS => $zs_max_num_cntrs);

    $ret = $node->start(ZS_REFORMAT  => 1,);
    like($ret, qr/OK.*/, 'Node started');

    foreach my $p(@prop){
        @cguids = ();
        $ncntr1 = $cnums[0];
        print "Cntr num=$ncntr1\n";

	#create cntr1
	print "=== create cntrs1 ===\n";
        for(0 .. $ncntr1-1){
            $ret = ZSOpen($node->conn(0),"ctr-$_",$$p[0],$size,"ZS_CTNR_CREATE",$$p[2],$$p[1], $ctr_type[$_%2], $ctr_type[$_%2]);
            like ($ret, qr/^OK.*/, $ret);
            $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
	    push(@cguids, $cguid);
        }


        # delete containers
	#print "=== delete containers ===\n";
        for(0 .. $ncntr1-1){
            $ret = ZSDelete($node->conn(0), $cguids[$_]);
            like ($ret, qr/^OK.*/, $ret);
            $cguids[$_] = "";
        }


	#restart zs
	print "=== restart zs and update ZS_MAX_NUM_CONTAINERS ===\n";
        $ret = $node->stop();
        like($ret,qr/OK.*/,"Node Stop");

	#update ZS_MAX_NUM_CONTAINERS
	$zs_max_num_cntrs = $cnums[1] + 10; 
	$node -> set_ZS_prop(ZS_MAX_NUM_CONTAINERS => $zs_max_num_cntrs);

        $ret = $node->start(ZS_REFORMAT => 0);
        like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=0");


	#create cntrs againe
	print "=== create cntrs ===\n";
	$ncntr2 = $cnums[1];
	print "Cntr2 num=$ncntr2\n";
	for(0 .. $ncntr2-1){
	    $ret = ZSOpen($node->conn(0), "ctr-$_", $$p[0], $size, "ZS_CTNR_CREATE", $$p[2], $$p[1], $ctr_type[$_%2]);
	    like ($ret, qr/^OK.*/, $ret);
	    $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
	    push(@cguids, $cguid);
	}

	foreach(@cguids){
	    if ($_ ne ""){
	        $ret = ZSClose($node->conn(0), $_);
                like ($ret, qr/^OK.*/, $ret);
	    }
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
    $node->set_ZS_prop(ZS_MAX_NUM_CONTAINERS => 6000);
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
                
