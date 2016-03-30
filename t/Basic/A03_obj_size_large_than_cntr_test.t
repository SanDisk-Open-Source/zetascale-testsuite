# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file: 
# author: Shanshan Shen 
# email: ssshen@hengtiansoft.com
# date: Jan 10, 2013
# description: 

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::BasicTest;
use Test::More tests => 73;

my $node; 

sub test_run {
    my $ret;
    my ($cguid, @cguids);
    my $i;
    my @prop = ([4, "ZS_DURABILITY_HW_CRASH_SAFE", "no"],);
    #my @data = ([50, 64000, 30000], [100, 128000, 30000], [150, 512, 180000]);
    my @data = ([64, 16000, 15000], [74, 32000, 15000], [84, 64000, 15000], [94, 128000, 15000], [104, 48,300000]);

    $ret = $node->start(
               ZS_REFORMAT  => 1,gdb_switch => 1,
           );
    like($ret, qr/OK.*/, 'Node start');

    foreach my $p(@prop){
        foreach $i(0,1,2,3){
            $ret=OpenContainer($node->conn(0), "c$i","ZS_CTNR_CREATE",0,$$p[0],$$p[1],$$p[2]);
            $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
            foreach my $d(@data){
                WriteObjects($node->conn(0),$cguid,0,$$d[0],1000,$$d[1],$$d[2]);
            } 
            FlushContainer($node->conn(0),$cguid);
            CloseContainer($node->conn(0),$cguid);
            DeleteContainer($node->conn(0),$cguid);

            $ret=OpenContainer($node->conn(0), "c$i\_2","ZS_CTNR_CREATE",0,$$p[0],$$p[1],$$p[2]);
            $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
            push(@cguids, $cguid);
            foreach my $d(@data){
                WriteObjects($node->conn(0),$cguid,0,$$d[0],1000,$$d[1],$$d[2]);
            }
            FlushContainer($node->conn(0),$cguid);
            CloseContainer($node->conn(0),$cguid);
        }

        foreach(@cguids){
            DeleteContainer($node->conn(0),$_);
        }
        @cguids=();
    }
    return;
}

sub test_init {
    $node = Fdftest::Node->new(
                ip     => "127.0.0.1", 
                port   => "24422",
                nconn  => 1,
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


