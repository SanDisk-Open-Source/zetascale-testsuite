# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file: 
# author: yiwen lu
# email: yiwenlu@hengtiansoft.com
# date: 
# description:

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
#use Test::More tests => "no_plan";
use Test::More 'no_plan';

my $node; 

sub test_run {

    my $ret;
    my $cguid;
    my @cnames;
    my $ctr_num = 64000;
    my @prop = (["yes","no","yes","ZS_DURABILITY_HW_CRASH_SAFE","no"],);
    #my @data = ([50, 64000, 25], [100, 128000, 25], [150, 512, 150]);
    my @data = ([64, 16000, 10], [74, 32000, 10], [84, 64000, 10], [94, 128000, 10], [104, 48, 200]);

    $ret = $node->set_ZS_prop (ZS_MAX_NUM_CONTAINERS => 64000);
    like ($ret, qr//, 'set ZS_MAX_NUM_CONTAINERS to 64K');

    $ret = $node->start(
               ZS_REFORMAT  => 1,
               time_out => 1800,
           );
    like($ret, qr/OK.*/, 'Node start');

    foreach my $p(@prop){
        for(my $i=0; $i<$ctr_num; $i++){    
            $ret = ZSOpenContainer(
                $node->conn(0), 
                cname            => "c$i",
                fifo_mode        => "no",
                persistent       => $$p[0],
                evicting         => $$p[1],
                writethru        => $$p[2],
                async_writes     => $$p[4],
                size             => 0,
                durability_level => $$p[3],
                num_shards       => 1,
                flags            => "ZS_CTNR_CREATE",
            );
	    $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
	    like($ret, qr/OK.*/, "ZSOpenContainer canme=c$i,cguid=$cguid,fifo_mode=no,persistent=$$p[0],evicting=$$p[1],writethru=$$p[2],flags=CREATE");

            foreach my $d(@data){
                $ret = ZSWriteObject(
                    $node->conn(0),
                    cguid         => "$cguid",
                    key_offset    => 0,
                    key_len       => $$d[0],
                    data_offset   => 1000,
                    data_len      => $$d[1],
                    nops          => $$d[2],
                    flags         => "ZS_WRITE_MUST_NOT_EXIST",
                );
                like($ret, qr/OK.*/, "ZSWriteObject-->cguid=$cguid nops=$$d[2]");

                $ret = ZSReadObject(
                    $node->conn(0),
                    cguid         => "$cguid",
                    key_offset    => 0,
                    key_len       => $$d[0],
                    data_offset   => 1000,
                    data_len      => $$d[1],
                    nops          => $$d[2],
                    check         => "yes",
                    keep_read     => "yes",
                );
                like($ret, qr/OK.*/, "ZSReadObject->cguid=$cguid nops=$$d[2]");
            }

	    $ret = ZSFlushContainer(
                $node->conn(0),
                cguid     => "$cguid",
            );
	    like($ret, qr/OK.*/, "ZSFlushContainer->cguid=$cguid");
        
	    $ret = ZSCloseContainer(
                $node->conn(0),
                cguid        => "$cguid",
            );
	    like($ret, qr/OK.*/, "ZSCloseContainer->cguid=$cguid");
	}
    
#    $ret = ZSGetContainers($node->conn(0));
#    print "$ret";
	$ret = $node->stop();
	like($ret, qr/OK.*/, 'Node stop');
	$ret = $node->start(
            ZS_REFORMAT  => 0,
        );
	like($ret, qr/OK.*/, 'Node restart');
    
	for(my $i=0; $i<$ctr_num; $i++){
            $ret = ZSOpenContainer(
                $node->conn(0), 
                cname            => "c$i",
                fifo_mode        => "no",
                persistent       => $$p[0],
                evicting         => $$p[1],
                writethru        => $$p[2],
                async_writes     => $$p[4],
                size             => 0,
                durability_level => $$p[3],
                num_shards       => 1,
                flags            => "ZS_CTNR_RW_MODE",
            );
	    $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
	    like($ret, qr/OK.*/, "ZSOpenContainer cguid=$cguid flags=RW_MODE");

            foreach my $d(@data){
                $ret = ZSReadObject(
                    $node->conn(0),
                    cguid         => "$cguid",
                    key_offset    => 0,
                    key_len       => $$d[0],
                    data_offset   => 1000,
                    data_len      => $$d[1],
                    nops          => $$d[2],
                    check         => "yes",
                    keep_read     => "yes",
                );
                like($ret, qr/OK.*/, "ZSReadObject->cguid=$cguid nops=$$d[2]");
            }
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


