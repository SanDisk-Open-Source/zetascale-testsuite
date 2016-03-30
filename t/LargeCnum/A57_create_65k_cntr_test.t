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
use Test::More tests => 2;

my $node; 

sub test_run {
    my $ret;
    my ($cguid, @cguids);
    my @prop = ([4, "ZS_DURABILITY_HW_CRASH_SAFE", "no"],);
    #my @data = ([50, 64000, 20], [100, 128000, 20], [150, 512, 120]);
    my @data = ([64, 16000, 10], [74, 32000, 10], [84, 64000, 10], [94, 128000, 10], [104, 48, 200]);

    $ret = $node->set_ZS_prop (ZS_MAX_NUM_CONTAINERS => 64000);
    like ($ret, qr//, 'set ZS_MAX_NUM_CONTAINERS to 64K');

    $ret = $node->start(
               ZS_REFORMAT  => 1,
           );
    like($ret, qr/OK.*/, 'Node start');

    foreach my $p(@prop){
        for(my $j=1; $j<=65000; $j++){
            $ret=OpenContainer($node->conn(0), "c\_$j","ZS_CTNR_CREATE",0,$$p[0],$$p[1],$$p[2]);
            $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
            push(@cguids, $cguid);
            foreach my $d(@data){
                WriteReadObjects($node->conn(0),$cguid,0,$$d[0],100,$$d[1],$$d[2]);
            }
            CloseContainer($node->conn(0),$cguid);
        }
       
        for(@cguids){
            DeleteContainer($node->conn(0),$_);
        }
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


