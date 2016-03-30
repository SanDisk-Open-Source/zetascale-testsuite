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
use Test::More 'no_plan';

my $node; 

sub test_run {
    my ($ret, $cguid, @cguids);
    my $size = 0;
    my $loop = 2;
    my $ncntr = 64000;
    my @prop  = ( [3,"ZS_DURABILITY_HW_CRASH_SAFE","no"]);

    $ret = $node->set_ZS_prop (ZS_MAX_NUM_CONTAINERS => 64000);
    like ($ret, qr//, 'set ZS_MAX_NUM_CONTAINERS to 64K');

    $ret = $node->start(ZS_REFORMAT  => 1,);
    like($ret, qr/OK.*/, 'Node started');

    foreach my $p(@prop){
        print "Cntr num=$ncntr\n";

        for(1 .. $loop){
            print "=== create cntr ===\n";
            @cguids = ();
            for(1 .. $ncntr){
                $ret = ZSOpen($node->conn(0),"ctr-$_",$$p[0],$size,"ZS_CTNR_CREATE",$$p[2],$$p[1]);
                like ($ret, qr/^OK.*/, $ret);
                $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
	        push(@cguids, $cguid);
            }
        
            print "=== del cntr ===\n";
            foreach(@cguids){
                $ret = ZSClose($node->conn(0), $_);
                like ($ret, qr/^OK.*/, $ret);
                $ret = ZSDelete($node->conn(0), $_);
                like ($ret, qr/^OK.*/, $ret);
            }
        }

        $ret = $node->stop();
        like($ret,qr/OK.*/,"Node Stop");
        $ret = $node->start(ZS_REFORMAT => 0);
        like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=0");

        for(1 .. $loop){
            print "=== create cntr ===\n";
            @cguids = ();
            for(1 .. $ncntr){
               $ret = ZSOpen($node->conn(0),"ctr-$_",$$p[0],$size,"ZS_CTNR_CREATE",$$p[2],$$p[1]);
                like ($ret, qr/^OK.*/, $ret);
                $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
                push(@cguids, $cguid);
            }

            print "=== del cntr ===\n";
            foreach(@cguids){
                $ret = ZSClose($node->conn(0), $_);
                like ($ret, qr/^OK.*/, $ret);
                $ret = ZSDelete($node->conn(0), $_);
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
    $node->set_ZS_prop (ZS_REFORMAT => 1, ZS_MAX_NUM_CONTAINERS => 6000);

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
                
