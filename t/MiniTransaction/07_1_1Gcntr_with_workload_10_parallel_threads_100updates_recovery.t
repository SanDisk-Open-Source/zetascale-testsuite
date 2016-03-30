# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file: t/MiniTransaction/07_1_1Gcntr_with_workload_10_parallel_threads_100updates_recovery.t
# author: xiaofeng chen
# email: xiaofengchen@hengtiansoft.com
# date: Jan 28, 2013
# description: 

#!/usr/bin/perl

use strict;
use warnings;
use threads;
use Switch;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::Stress;
use Test::More "no_plan";

my $node;
my $nthread = 10;
#my @data = ([50, 64000, 0.125], [100, 128000, 0.125], [150, 512, 0.75]);
my @data = ([64, 16000, 0.05], [74, 32000, 0.05], [84, 64000, 0.05], [94, 128000, 0.05], [104, 48,1]);

sub test_ZSWriteObject_update{
    my ($connid, $cguid, $key_offset, $val_offset, $nops) = @_;

    my $ret = ZSTransactionStart(
            $node->conn($connid),
            );
    like($ret, qr/OK.*/, 'ZSTransactionStart');

    foreach my $d(@data){
        $ret = ZSWriteObject(
            $node->conn($connid),
            cguid         => $cguid,
            key_offset    => $key_offset+$nops*$$d[2]*$connid,
            key_len       => $$d[0],
            data_offset   => $val_offset+$nops*$$d[2]*$connid,
            data_len      => $$d[1],
            nops          => $nops*$$d[2],
            flags         => "ZS_WRITE_MUST_EXIST",
            );
        #like($ret, qr/OK.*/, "ZSWriteObject, update $nops objects on cguid=$cguid");
    }
}

sub test_run {
    my $ret;
    my $cguid;
    my $cname = "Tran_Cntr";
    my $update_thread;
    my $key_offset = 1000;
    my $val_offset = 100;
    my $size = 0;
    my $nobject = 10000;
    my $update = 400;
    my @prop = (["yes","yes","no","ZS_DURABILITY_HW_CRASH_SAFE","no"],);

    $ret = $node->start(
               ZS_REFORMAT  => 1,
               threads       => 10,
           );
    like($ret, qr/OK.*/, 'Node start');

    foreach my $p(@prop){
        $ret = ZSOpenContainer(
                    $node->conn(0),
                    cname            => $cname,
                    fifo_mode        => "no",
                    persistent       => $$p[0],
                    writethru        => $$p[1],
                    evicting         => $$p[2],
                    size             => $size,
                    durability_level => $$p[3],
                    async_writes     => $$p[4],
                    num_shards       => 1,
                    flags            => "ZS_CTNR_CREATE"
                    );
	$cguid = $1 if($ret =~ /OK cguid=(\d+)/);
	like($ret, qr/OK.*/, "ZSopenContainer: cname=$cname, cguid=$cguid");

        foreach my $d(@data){
  	    $ret = ZSWriteObject(
                    $node->conn(0),
                    cguid         => $cguid,
                    key_offset    => $key_offset,
                    key_len       => $$d[0],
                    data_offset   => $val_offset,
                    data_len      => $$d[1],
                    nops          => $nobject*$$d[2],
                    flags         => "ZS_WRITE_MUST_NOT_EXIST",
                    );
	   like($ret, qr/OK.*/, "ZSWriteObject: load ".$nobject*$$d[2]." objects to $cname, cguid=$cguid");
	}

	for(0 .. $nthread - 1)
	{
	    $update_thread = threads->create(\&test_ZSWriteObject_update, 
			    $_, 
			    $cguid, 
			    $key_offset, 
			    $val_offset - 1, 
			    $update / $nthread); 
	}
    
	$node->kill();

	$ret = $node->start(
               ZS_REFORMAT  => 0,
           );
	like($ret, qr/OK.*/, 'Node restart');

	$ret = ZSOpenContainer(
                    $node->conn(0),
                    cname            => $cname,
                    fifo_mode        => "no",
                    persistent       => $$p[0],
                    writethru        => $$p[1],
                    evicting         => $$p[2],
                    size             => $size,
                    durability_level => $$p[3],
                    async_writes     => $$p[4],
                    num_shards       => 1,
                    flags            => "ZS_CTNR_RW_MODE"
                    );
	$cguid = $1 if($ret =~ /OK cguid=(\d+)/);
	like($ret, qr/OK.*/, "ZSopenContainer: cname=$cname, cguid=$cguid");

	foreach my $d(@data){
	    $ret = ZSReadObject(
                    $node->conn(0),
                    cguid         => $cguid,
                    key_offset    => $key_offset,
                    key_len       => $$d[0],
                    data_offset   => $val_offset,
                    data_len      => $$d[1],
                    nops          => $nobject*$$d[2],
                    check         => "yes",
                    );
	    like($ret, qr/OK.*/, "ZSReadObject nops=".$nobject*$$d[2]." on $cname, cguid=$cguid");
	}

	$ret = ZSCloseContainer(
                    $node->conn(0),
                    cguid      => $cguid,
                    );
	like($ret, qr/OK.*/, "ZSCloseContainer, cguid=$cguid");

	$ret = ZSDeleteContainer(
                    $node->conn(0),
                    cguid      => $cguid,
                    );
	like($ret, qr/OK.*/, "ZSDeleteContainer, cguid=$cguid");
    }

    return;
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


