# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file: t/ZS_RangeQuery/16_single_cntr_10update_1range_rangequery.t
# author: shujing zhu
# email: shujingzhu@hengtiansoft.com
# date: June 17, 2013
# description: range query

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
use Test::More tests => 163;

my $node;
my $nthread = 12;

sub test_ZSWriteObject_update{
	my ($conn, $cguid, $key, $key_len, $val_offset, $val_len, $nops) = @_;
	
        my $ret = ZSWriteObject(
			            $conn,
			            cguid         => $cguid,
			            key           => $key,
			            key_len       => $key_len,
			            data_offset   => $val_offset,
			            data_len      => $val_len,
			            nops          => $nops,
			            flags         => "ZS_WRITE_MUST_EXIST",
			            );
	like($ret, qr/OK.*/, "ZSWriteObject, update $nops objects $key~$key+$nops on cguid=$cguid");
}

sub test_RangeQuery{
    my($conn, $cguid, $start_key, $key_len,$end_key,$flags,$nobject,) = @_;
    my $ret = ZSGetRange (
                $conn,
                cguid         => $cguid,
                keybuf_size   => 60,
                databuf_size  => 1024,
                keylen_start  => $key_len,
                keylen_end    => $key_len,
                start_key     => $start_key,
                end_key       => $end_key,
                flags         => $flags,
                );
    like($ret, qr/OK.*/,"ZSGetRange:Get $start_key ~ $end_key,flags=$flags -> $ret");

    $ret = ZSGetNextRange (
                $conn,
                n_in          => $nobject+100000,
                check         => "no",
                );
    my $n_out = $1 if($ret =~ /OK n_out=(\d+)/);
    like($ret, qr/OK n_out=$nobject.*/, "ZSGetNextRange:Get $n_out objects ,$ret");

    $ret = ZSGetRangeFinish($conn);
    like($ret, qr/OK.*/, "ZSGetRangeFinish");
}


sub test_run {
    my $ret;
    my $cguid;
    my $cname = "Tran_Cntr";
    my @update_thread;
    my $key  = 1;
    my $val_offset = $key;
    my $update = 100;
    my $size = 0;
    my $flags = 'ZS_RANGE_START_GE|ZS_RANGE_END_LE' ;
    my @prop = (["yes","no","yes","ZS_DURABILITY_HW_CRASH_SAFE","no"],);
    #my @data = ([50, 64000, 625], [100, 128000, 625], [150, 512, 375]);
    my @data = ([64, 16000, 300], [74, 32000, 300], [84, 64000, 300], [94, 128000, 300], [104, 48,6000]);

    $ret = $node->start(
               ZS_REFORMAT  => 1,
           );
    like($ret, qr/OK.*/, 'Node start');

    foreach my $p(@prop){
    $ret = ZSOpenContainer(
                $node->conn(0),
                cname            => $cname,
                fifo_mode        => "no",
                persistent       => $$p[0],
                writethru        => $$p[2],
                evicting         => $$p[1],
                size             => $size,
                durability_level => $$p[3],
                async_writes     => $$p[4],
                num_shards       => 1,
                flags            => "ZS_CTNR_CREATE"
                );
    $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
    like($ret, qr/OK.*/, "ZSopenContainer: $cname, cguid=$cguid");

    foreach my $d(@data){
        $ret = ZSWriteObject(
                    $node->conn(0),
                    cguid         => $cguid,
                    key           => $key,
                    key_len       => $$d[0],
                    data_offset   => $val_offset,
                    data_len      => $$d[1],
                    nops          => $$d[2],
                    flags         => "ZS_WRITE_MUST_NOT_EXIST",
                    );
        like($ret, qr/OK.*/, "ZSWriteObject: load $$d[2] objects keylen=$$d[0],datalen=$$d[1] to $cname, cguid=$cguid");

        my $object_per_thread = $update / ($nthread-2);
        for(0 .. $nthread - 3)
        {
            $update_thread[$_] = threads->create(\&test_ZSWriteObject_update, 
                    $node->conn($_), 
                    $cguid, 
                    $key + $_ * $object_per_thread, 
                    $$d[0], 
                    $val_offset +1 + $_ * $object_per_thread, 
                    $$d[1], 
                    $update / ($nthread-2));
        }
        
        $flags = 'ZS_RANGE_START_GE|ZS_RANGE_END_LE';
        $update_thread[10] = threads->create(\&test_RangeQuery,
                    $node->conn(10),
                    $cguid,
                    $key,
                    $$d[0],
                    $$d[2]+$key,
                    $flags,
                    $$d[2]);

        $flags = 'ZS_RANGE_START_LE|ZS_RANGE_END_GE';
        $update_thread[11] = threads->create(\&test_RangeQuery,
                    $node->conn(11),
                    $cguid,
                    $$d[2]+$key,
                    $$d[0],
                    $key,
                    $flags,
                    $$d[2]);

        for(0 .. $nthread - 1)
        {
            $update_thread[$_]->join();
        }


        $ret = ZSReadObject(
                    $node->conn(0),
                    cguid         => $cguid,
                    key           => $key,
                    key_len       => $$d[0],
                    data_offset   => $val_offset+1,
                    data_len      => $$d[1],
                    nops          => $update,
                    check         => "yes",
                    );
        like($ret, qr/OK.*/, "ZSReadObject: check update $update objects succeed on $cname, cguid=$cguid");
        
        $flags = 'ZS_RANGE_START_GE|ZS_RANGE_END_LE';
        test_RangeQuery($node->conn(0),$cguid, $key, $$d[0],$$d[2]+$key,$flags,$$d[2]);

        $flags = 'ZS_RANGE_START_LE|ZS_RANGE_END_GE';
        test_RangeQuery($node->conn(0),$cguid,$$d[2]+$key,$$d[0],$key,$flags,$$d[2]);
    }
    $ret = ZSCloseContainer(
                $node->conn(0),
                cguid      => $cguid,
                );
    like($ret, qr/OK.*/, 'ZSCloseContainer');

    $ret = $node->stop();
    like($ret, qr/OK.*/, 'Node stop');

    $ret = $node->start(
                    ZS_REFORMAT  => 0,
                    );
    like($ret, qr/OK.*/, 'Node restart');

    $ret = ZSOpenContainer(
                $node->conn(0),
                cname            => $cname,
                fifo_mode        => "no",
                persistent       => $$p[0],
                writethru        => $$p[2],
                evicting         => $$p[1],
                size             => $size,
                durability_level => $$p[3],
                async_writes     => $$p[4],
                num_shards       => 1,
                flags            => "ZS_CTNR_RW_MODE"
                );
    $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
    like($ret, qr/OK.*/, "ZSopenContainer: $cname, cguid=$cguid");

    foreach my $d(@data){
        $ret = ZSReadObject(
                    $node->conn(0),
                    cguid         => $cguid,
                    key           => $key,
                    key_len       => $$d[0],
                    data_offset   => $val_offset+1,
                    data_len      => $$d[1],
                    nops          => $update,
                    check         => "yes",
                    );
        like($ret, qr/OK.*/, "ZSReadObject: check update $update objects succeed after restart ZS on $cname, cguid=$cguid");

        $flags = 'ZS_RANGE_START_GE|ZS_RANGE_END_LE';
        test_RangeQuery($node->conn(0),$cguid, $key, $$d[0],$$d[2]+$key,$flags,$$d[2]);

        $flags = 'ZS_RANGE_START_LE|ZS_RANGE_END_GE';
        test_RangeQuery($node->conn(0),$cguid,$$d[2]+$key,$$d[0],$key,$flags,$$d[2]);
    }
	$ret = ZSCloseContainer(
                $node->conn(0),
                cguid      => $cguid,
                );
    like($ret, qr/OK.*/, 'ZSCloseContainer');

    $ret = ZSDeleteContainer(
                $node->conn(0),
                cguid      => $cguid,
                );
    like($ret, qr/OK.*/, 'ZSDeleteContainer');
    }
    return;
}

sub test_init {
    $node = Fdftest::Node->new(
                ip     => "127.0.0.1",
                port   => "24422",
                nconn  => 15,
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


