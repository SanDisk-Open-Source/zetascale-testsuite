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
use Test::More tests => 15;

my $node; 

sub test_run {
    my $ret;
    my $cguid;
    my $write_suc = 0;
    #my @data = ([50, 64000, 256], [100, 128000, 256], [150, 512, 1536]);   
    my @data = ([64, 16000, 100], [74, 32000, 100], [84, 64000, 100], [94, 128000, 100], [104, 48, 2000]);
    my @prop = ([4, "ZS_DURABILITY_HW_CRASH_SAFE", "no"],);

    $ret = $node->start(
               ZS_REFORMAT  => 1,gdb_switch => 1,
           );
    like($ret, qr/OK.*/, 'Node start');

    foreach my $p(@prop){
    	$ret=OpenContainer($node->conn(0), "c0","ZS_CTNR_CREATE",0,$$p[0],$$p[1],$$p[2]);
    	$cguid = $1 if($ret =~ /OK cguid=(\d+)/);
        foreach my $d(@data){
        	$ret = ZSWriteObject (
                	$node->conn (0),
                	cguid       => "$cguid",
                	key_offset  => 1000,
			key_len     => $$d[0],
			data_offset => 1000,
			data_len    => $$d[1],
			nops        => $$d[2],
			flags       => "ZS_WRITE_MUST_NOT_EXIST",
			);
		chomp $ret;
		if($ret =~ qr/OK.*/){
			like($ret, qr/OK.*/,"ZSWriteObject: cguid=$cguid key_off=1000 key_len=$$d[0] data_off=1000 data_len=$$d[1] nops=$$d[2]-->".$ret);
			$write_suc = $$d[2];
		}
		else{
			$write_suc = $1 if ($ret =~ /.*\s+(\d+).*/);
			like(0, qr/0/,"ZSWriteObject: cguid=$cguid key_off=1000 key_len=$$d[0] data_off=1000 data_len=$$d[1] nops=$$d[2]-->".$ret);
		}
		ReadObjects($node->conn(0),$cguid,1000,$$d[0],1000,$$d[1],$write_suc);
        }
        FlushContainer($node->conn(0),$cguid);
        CloseContainer($node->conn(0),$cguid);
        DeleteContainer($node->conn(0),$cguid);
    }
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


