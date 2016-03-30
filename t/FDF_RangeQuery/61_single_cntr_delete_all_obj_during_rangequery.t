# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file: t/ZS_RangeQuery/61_single_cntr_delete_all_obj_during_rangequery
# author: shujing zhu
# email: shujingzhu@hengtiansoft.com
# date: June 11, 2013
# description: range query cntr

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::BasicTest;
use Test::More tests => 54;
use threads;

my $node;

sub delete_allObj_during_Rangequery{

	my($conn, $cguid, $start_key, $keylen,$end_key,$flags,$nobject,$delete_key) = @_;

    my $ret = ZSGetRange (
                $conn,
                cguid         => $cguid,
                keybuf_size   => 50, 
                databuf_size  => 1024,
                keylen_start  => $keylen,
                keylen_end    => $keylen,           
                start_key     => $start_key,
                end_key       => $end_key,
				flags         => $flags
                );
    like($ret, qr/OK.*/,"ZSGetRange:Get $start_key ~ $end_key,keylen =$keylen ,flags=$flags");

    $ret = ZSDeleteObject(
                $conn,
                cguid         => "$cguid",
                key           => $delete_key,
                key_len       => $keylen,
                nops          => $nobject,
                );

	like($ret, qr/OK.*/, "ZSDeleteObject:delete $nobject objects , cguid=$cguid");

	$ret = ZSGetNextRange (
                $conn,
                n_in          => $nobject+10000, 
                check         => "yes",
                );
    like($ret, qr/SERVER_ERROR ZS_QUERY_DONE.*/, "ZSGetNextRange:Get 0 objects ->$ret");

    $ret = ZSGetRangeFinish($conn);
    like($ret, qr/OK.*/, "ZSGetRangeFinish");
    
}

sub test_run {

    my $ret;
    my $cguid;
    my $cname = "Tran_Cntr";  
    my $update_thread;
    my $key = 1;
    my $val_offset = $key;
    my $size = 0;
	my $flags;
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
                        nops          => $$d[2],
                        flags         => "ZS_WRITE_MUST_NOT_EXIST",
                        );
            like($ret, qr/OK.*/, "ZSWriteObject: load $$d[2] objects,keylen=$$d[0],datalen=$$d[1] to $cname, cguid=$cguid");

            $flags = 'ZS_RANGE_START_GE|ZS_RANGE_END_LT';
            my $end_key = $$d[2]+$key;
            delete_allObj_during_Rangequery($node->conn(0),$cguid,$key,$$d[0],$end_key,$flags,$$d[2],$key);
            
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
            like($ret, qr/OK.*/, "ZSWriteObject: load $$d[2] objects,keylen=$$d[0],datalen=$$d[1] to $cname, cguid=$cguid");
            
            $flags = 'ZS_RANGE_START_LT|ZS_RANGE_END_GT';
            delete_allObj_during_Rangequery($node->conn(0),$cguid,$end_key,$$d[0],$key,$flags,$$d[2],$key);
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


