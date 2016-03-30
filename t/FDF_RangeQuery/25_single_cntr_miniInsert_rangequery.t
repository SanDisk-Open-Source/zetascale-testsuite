# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file: t/ZS_RangeQuery/25_single_cntr_miniInsert_rangequery.t
# author: shujing zhu
# email: shujingzhu@hengtiansoft.com
# date: June 17, 2013
# description: range query after minitrax insert objects

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
use Test::More tests => 45;

my $node;
my $nthread = 10;

sub test_ZSWriteObject_create{
	my ($conn, $cguid, $key, $key_len, $val_offset, $val_len, $nops) = @_;
	
        my $ret = ZSWriteObject(
			            $conn,
			            cguid         => $cguid,
			            key           => $key,
			            key_len       => $key_len,
			            data_offset   => $val_offset,
			            data_len      => $val_len,
			            nops          => $nops,
			            flags         => "ZS_WRITE_MUST_NOT_EXIST",
			            );
	like($ret, qr/OK.*/, "ZSWriteObject, update $nops objects on cguid=$cguid");
}

sub test_run {
    my $ret;
    my $cguid;
    my $cname = "Tran_Cntr";
    my @update_thread;
    my $key  = 1;
    my $val_offset = $key;
    my $re_nobject = 1000;
    my $size = 0;
    my $flags = 'ZS_RANGE_START_GE|ZS_RANGE_END_LE' ;
    my @prop = (["yes","no","yes","ZS_DURABILITY_HW_CRASH_SAFE","no"],);
    #my @data = ([50, 64000, 6250], [100, 128000, 6250], [150, 512, 37500]);
    my @data = ([64, 16000, 3000], [74, 32000, 3000], [84, 64000, 3000], [94, 128000, 3000], [104, 48,60000]);

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
                        nops          => ($$d[2]-$re_nobject),
                        flags         => "ZS_WRITE_MUST_NOT_EXIST",
                        );
            like($ret, qr/OK.*/, "ZSWriteObject: load $$d[2]-$re_nobject objects keylen=$$d[0],datalen=$$d[1] to $cname, cguid=$cguid");
        }
        $ret = ZSTransactionStart(
                    $node->conn(0),
                    );
        like($ret, qr/OK.*/, 'ZSTransactionStart');

        foreach my $d(@data){
            $ret = ZSWriteObject(
                        $node->conn(0),
                        cguid         => $cguid,
                        key           => $$d[2]-$re_nobject+$key,
                        key_len       => $$d[0],
                        data_offset   => $$d[2]-$re_nobject+$key,
                        data_len      => $$d[1],
                        nops          => $re_nobject,
                        flags         => "ZS_WRITE_MUST_NOT_EXIST",
                        );
            like($ret, qr/OK.*/, "ZSWriteObject, load $re_nobject objects keylen=$$d[0],datalen=$$d[1] on cguid=$cguid");
        }
        $ret = ZSTransactionCommit(
                    $node->conn(0)
                    );
        like($ret, qr/OK.*/, 'ZSTransactionCommit');

        foreach my $d(@data){
            $ret = ZSGetRange (
                        $node->conn(0),
                        cguid         => $cguid,
                        keybuf_size   => 60,
                        databuf_size  => 1024,
                        keylen_start  => $$d[0],
                        keylen_end    => $$d[0],
                        start_key     => $key,
                        end_key       => $$d[2]+$key,
                        flags         => $flags,
                        );
            like($ret, qr/OK.*/,"ZSGetRange:Get $key ~ $$d[2]+$key,,flags=$flags");

            $ret = ZSGetNextRange (
                        $node->conn(0),
                        n_in          => $$d[2]+$re_nobject,
                        check         => "yes",
                        );
            my $n_out = $1 if($ret =~ /OK n_out=(\d+)/);
            like($ret, qr/OK n_out=$$d[2].*/, "ZSGetNextRange:Get $n_out objects ,$ret");

            $ret = ZSGetRangeFinish($node->conn(0));
            like($ret, qr/OK.*/, "ZSGetRangeFinish");

            $ret = ZSReadObject(
                        $node->conn(0),
                        cguid         => $cguid,
                        key           => $key,
                        key_len       => $$d[0],
                        data_offset   => $val_offset,
                        data_len      => $$d[1],
                        nops          => $$d[2],
                        check         => "yes",
                        );
            like($ret, qr/OK.*/, "ZSReadObject: check $$d[2] objects succeed on $cname, cguid=$cguid");
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
                        data_offset   => $val_offset,
                        data_len      => $$d[1] ,
                        nops          => $$d[2],
                        check         => "yes",
                        );
            like($ret, qr/OK.*/, "ZSReadObject: check $$d[2] objects succeed after restart ZS on $cname, cguid=$cguid");
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


