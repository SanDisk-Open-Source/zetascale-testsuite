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
    my ($ret, $cguid, @cguids, $ncntr);
    my $size = 0;
    my $keyoff = 1000;
    my @cnums = (2000, 4000, 8000, 16000);
    my @prop  = ( [3,"ZS_DURABILITY_HW_CRASH_SAFE","no"]);

    $ret = $node->set_ZS_prop (ZS_MAX_NUM_CONTAINERS => 64000);
    like ($ret, qr//, 'set ZS_MAX_NUM_CONTAINERS to 64K');

    $ret = $node->start(ZS_REFORMAT  => 1,);
    like($ret, qr/OK.*/, 'Node started');

    foreach my $p(@prop){
        @cguids = ();
        $ncntr = $cnums[ rand(@cnums) ];
        print "Cntr num=$ncntr\n";

        #open_created_cntr
        print "=== open_created_cntr ===\n";
        for(1 .. $ncntr){
            $ret = ZSOpen($node->conn(0),"ctr-$_",$$p[0],$size,"ZS_CTNR_CREATE",$$p[2],$$p[1]);
            like ($ret, qr/^OK.*/, $ret);
            $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
	    push(@cguids, $cguid);
        }

        for(1 .. $ncntr){
            $ret = ZSOpen($node->conn(0),"ctr-$_",$$p[0],$size,"ZS_CTNR_RW_MODE",$$p[2],$$p[1]);
            like ($ret, qr/^OK.*/, $ret);
            $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
            is($cguids[$_ - 1], $cguid, "create cguid=$cguids[$_ - 1], open cguid=$cguid");
        }

        foreach(@cguids){
            $ret = ZSGetProps($node->conn(0), $_);
            like ($ret, qr/.*durability_level=2.*/, "durability_level=$$p[1]");
        }
        $ret = ZSGetConts($node->conn(0), $ncntr);
        like ($ret, qr/^OK.*/, $ret);

        #create_created_cntr
        print "=== create_created_cntr ===\n";
        for(1 .. $ncntr){
            $ret = ZSOpen($node->conn(0),"ctr-$_",$$p[0],$size,"ZS_CTNR_CREATE",$$p[2],$$p[1]);
            like ($ret, qr/SERVER_ERROR ZS_CONTAINER_EXISTS.*/, $ret);
        }

        $ret = ZSGetConts($node->conn(0), $ncntr);
        like ($ret, qr/^OK.*/, $ret);

        #open_closed_cntr
        print "=== open_closed_cntr ===\n";
        foreach(@cguids){
            $ret = ZSClose($node->conn(0), $_);
            like ($ret, qr/^OK.*/, $ret);
        }

        for(1 .. $ncntr){
            $ret = ZSOpen($node->conn(0),"ctr-$_",$$p[0],$size,"ZS_CTNR_RW_MODE",$$p[2],$$p[1]);
            like ($ret, qr/^OK.*/, $ret);
        }

        $ret = ZSGetConts($node->conn(0), $ncntr);
        like ($ret, qr/^OK.*/, $ret);

        #open_unexists_cntr && del_closed_cntr
        print "=== open_unexists_cntr && del_closed_cntr ===\n";
        foreach(@cguids){
            $ret = ZSClose($node->conn(0), $_);
            like ($ret, qr/^OK.*/, $ret);
        }

        foreach(@cguids){
            $ret = ZSDelete($node->conn(0), $_);
            like ($ret, qr/^OK.*/, $ret);
        }

        for(1 .. $ncntr){
            $ret = ZSOpen($node->conn(0),"ctr-$_",$$p[0],$size,"ZS_CTNR_RW_MODE",$$p[2],$$p[1]);
            like ($ret, qr/SERVER_ERROR ZS_CONTAINER_UNKNOWN.*/, $ret);
        }

        $ret = ZSGetConts($node->conn(0), 0);
        like ($ret, qr/^OK.*/, $ret);

        #del_unclosed_cntr && create_cntr_after_del
        print "=== del_unclosed_cntr && create_cntr_after_del ===\n";
        @cguids = ();
        for(1 .. $ncntr){
            $ret = ZSOpen($node->conn(0),"ctr-$_",$$p[0],$size,"ZS_CTNR_CREATE",$$p[2],$$p[1]);
            like ($ret, qr/^OK.*/, $ret);
            $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
            push(@cguids, $cguid);
        }

        foreach(@cguids){
            $ret = ZSDelete($node->conn(0), $_);
            like ($ret, qr/^OK.*/, $ret);
        }
  
        $ret = ZSGetConts($node->conn(0), 0);
        like ($ret, qr/^OK.*/, $ret);

        #del_unexists_cntr
        print "=== del_unexists_cntr ===\n";
        foreach(@cguids){
            $ret = ZSDelete($node->conn(0), $_);
            like ($ret, qr/SERVER_ERROR ZS_FAILURE.*/, $ret);
        }

        $ret = ZSGetConts($node->conn(0), 0);
        like ($ret, qr/^OK.*/, $ret);

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
    $node->set_ZS_prop(ZS_REFORMAT  => 1, ZS_MAX_NUM_CONTAINERS => 6000);

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
                
