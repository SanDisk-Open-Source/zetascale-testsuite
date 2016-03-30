# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file:
# author: yiwen lu
# email: yiwenlu@hengtiansoft.com
# date: Apr 9, 2015
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
    my @data = ([30, 16000, 3000], [60, 32000, 3000], [90, 64000, 3000], [120, 128000, 3000], [150, 48, 60000]);

    foreach my $p(@prop){
        $node->set_ZS_prop(ZS_FLOG_MODE  => "ZS_FLOG_FILE_MODE");
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
    $node->set_ZS_prop(ZS_FLOG_MODE  => "ZS_FLOG_FILE_MODE");
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


