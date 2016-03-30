# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file:
# author: yiwen lu
# email: yiwenlu@hengtiansoft.com
# date: Jan 3, 2013
# description:

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::TestCase;
use Test::More tests => 86;

my $node; 
sub test_run {
    my $cguid;
    my $ret; 
    my $loops=3;
    my @prop = ([4, "ZS_DURABILITY_HW_CRASH_SAFE", "no"],);
    #my @data = ([50, 64000, 200000], [100, 128000, 200000], [150, 512, 1200000]);
    my @data = ([64, 16000, 100000], [74, 32000, 100000], [84, 64000, 100000], [94, 128000, 100000], [104, 48, 2000000]);

    foreach my $p(@prop){
        $ret = $node->start(ZS_REFORMAT => 1);    
	like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=1");

	for(my $i=0; $i<$loops; $i++){
            my $nops = 0;
            print "=CYCLE:$i=\n";
	    $ret = OpenContainer($node->conn(0),"ctr-1","ZS_CTNR_CREATE",0,$$p[0],$$p[1],$$p[2]);
	    $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
	    $ret = GetContainers($node->conn(0),1);
            foreach my $d(@data){
                WriteReadObjects($node->conn(0),$cguid,0,$$d[0],1000,$$d[1],$$d[2]);
                $nops = $nops + $$d[2];
            }
	    FlushContainer($node->conn(0),$cguid);
	    ContainerEnumerate($node->conn(0),$cguid,$nops);
            foreach my $d(@data){
                DeleteObjects($node->conn(0),$cguid,0,$$d[0],$$d[2]/2);
            }
	    ContainerEnumerate($node->conn(0),$cguid,$nops/2); 
	    CloseContainer($node->conn(0),$cguid);

	    $ret = $node->stop();
	    like($ret,qr/OK.*/,"Node Stop");
	    $ret = $node->start(ZS_REFORMAT => 0);    
	    like($ret,qr/OK.*/,"Node Start: REFORMAT=0");

	    OpenContainer($node->conn(0),"ctr-1","ZS_CTNR_RW_MODE",0,$$p[0],$$p[1],$$p[2]);
	    ContainerEnumerate($node->conn(0),$cguid,$nops/2);
	    CloseContainer($node->conn(0),$cguid);
	    DeleteContainer($node->conn(0),$cguid);
	    GetContainers($node->conn(0),0);
        }
	$ret = $node->stop();
	like($ret,qr/OK.*/,"Node Stop");
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


