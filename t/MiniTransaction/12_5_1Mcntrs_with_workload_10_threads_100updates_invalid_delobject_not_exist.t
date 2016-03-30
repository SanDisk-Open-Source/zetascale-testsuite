# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file: t/MiniTransaction/12_5_1Mcntrs_with_workload_10_threads_100updates_invalid_delobject_not_exist.t
# author: xiaofeng chen
# email: xiaofengchen@hengtiansoft.com
# date: Jan 28, 2013
# description: change cntr_size from 1M to 1G

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
use Test::More tests => 171;

my $node;
my $nthread = 10;
my $ncntr = 5;
#my @data = ([50, 64000, 0.125], [100, 128000, 0.125], [150, 512, 0.75]);
my @data = ([64, 16000, 0.05], [74, 32000, 0.05], [84, 64000, 0.05], [94, 128000, 0.05], [104, 48,1]);

sub test_ZSWriteObject_update{
    my ($connid, $cguid, $key_offset, $val_offset, $nops) = @_;
    my $ret;

    $ret = ZSTransactionStart(
                    $node->conn($connid),
                    );
    like($ret, qr/OK.*/, 'ZSTransactionStart');

    foreach my $d(@data){
        $ret = ZSWriteObject(
                        $node->conn($connid),
                        cguid         => $cguid,
                        key_offset    => $key_offset + $nops*$$d[2]*($connid%2),
                        key_len       => $$d[0] + $connid/2,
                        data_offset   => $val_offset + $nops*$$d[2]*($connid%2),
                        data_len      => $$d[1],
                        nops          => $nops*$$d[2],
                        flags         => "ZS_WRITE_MUST_EXIST",
                        );
	like($ret, qr/OK.*/, "ZSWriteObject, update ".$nops*$$d[2]." objects on cguid=$cguid");
    }

#Delete a non exist object
    $ret = ZSDeleteObject(
                    $node->conn($connid),
                    cguid         => $cguid,
                    key_offset    => $key_offset - 1,
                    key_len       => 50,
                    nops          => 1,
                    );
    like($ret, qr/SERVER_ERROR ZS_OBJECT_UNKNOWN.*/, "ZSDeleteObject, delete an non_existent obect, expect return SERVER_ERROR ZS_OBJECT_UNKNOWN");

    $ret = ZSTransactionCommit(
                    $node->conn($connid)
                    );
    like($ret, qr/OK.*/, 'ZSTransactionCommit');
}

sub test_run {
    my $ret;
    my $cguid;
    my @cguid;
    my $cname;
    my @update_thread;
    my $key_offset = 1000;
    my $val_offset = 100;
    my $size = 0;
    my $nobject = 10000;
    my $update = 400;
    my @prop = (["yes","yes","no","ZS_DURABILITY_HW_CRASH_SAFE","no"],);

    $ret = $node->start(
        ZS_REFORMAT  => 1,
        threads      => $nthread,
    );
    like($ret, qr/OK.*/, 'Node start');

    foreach my $p(@prop){
        @cguid = ();
	for(0 .. $ncntr - 1)
	{
            $ret = ZSOpenContainer(
                            $node->conn(0),
                            cname            => 'Tran_Cntr'.$_,
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
            like($ret, qr/OK.*/, "ZSopenContainer:  Tran_Cntr$_, cguid=$cguid");
            push(@cguid, $cguid);

            foreach my $d(@data){
                $ret = ZSWriteObject(
                            $node->conn(0),
                            cguid         => $cguid,
                            key_offset    => $key_offset,
                            key_len       => $$d[0] + $_,
                            data_offset   => $val_offset,
                            data_len      => $$d[1],
                            nops          => $nobject*$$d[2],
                            flags         => "ZS_WRITE_MUST_NOT_EXIST",
                            );
                like($ret, qr/OK.*/, "ZSWriteObject: load ".$nobject*$$d[2]." to Tran_Cntr$_, cguid=$cguid");
	    }
	}

	for(0 .. $nthread - 1)
	{
	    $update_thread[$_] = threads->create(\&test_ZSWriteObject_update, 
                            $_,
			    $cguid[$_ / 2], 
			    $key_offset, 
			    $val_offset - 1, 
			    $update / $nthread);
	}

	for(0 .. $nthread - 1)
	{
            $update_thread[$_]->join();
	}

        for(0 .. $ncntr - 1)
        {
            foreach my $d(@data){
                $ret = ZSReadObject(
                            $node->conn(0),
                            cguid         => $cguid[$_],
                            key_offset    => $key_offset,
                            key_len       => $$d[0] + $_,
                            data_offset   => $val_offset - 1,
                            data_len      => $$d[1],
                            nops          => $update/$ncntr*$$d[2],
                            check         => "yes",
                            );
                like($ret, qr/OK.*/, "ZSReadObject: check update ".$update/$ncntr*$$d[2]." objects succeed on cguid=$cguid[$_]");
            }

            foreach my $d(@data){
                $ret = ZSReadObject(
                            $node->conn(0),
                            cguid         => $cguid[$_],
                            key_offset    => $key_offset+$update/$ncntr*$$d[2],
                            key_len       => $$d[0] + $_,
                            data_offset   => $val_offset+$update/$ncntr*$$d[2],
                            data_len      => $$d[1],
                            nops          => ($nobject-$update/$ncntr)*$$d[2],
                            check         => "yes",
                            );
                like($ret, qr/OK.*/, "ZSReadObject: check ".($nobject-$update/$ncntr)*$$d[2]." objects succeed on cguid=$cguid[$_]");
            }
        }

	for(0 .. $ncntr - 1)
	{
            $ret = ZSCloseContainer(
                            $node->conn(0),
                            cguid      => $cguid[$_],
                            );
            like($ret, qr/OK.*/, "ZSCloseContainer: cguid=$cguid[$_]");

            $ret = ZSDeleteContainer(
                            $node->conn(0),
                            cguid      => $cguid[$_],
                            );
            like($ret, qr/OK.*/, "ZSDeleteContainer: cguid=$cguid[$_]");
	}
    }

    return;
}

sub test_init {
    $node = Fdftest::Node->new(
                ip     => "127.0.0.1",
                port   => "24422",
                nconn  => $nthread,
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


