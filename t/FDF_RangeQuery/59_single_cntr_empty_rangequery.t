# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file: t/ZS_RangeQuery/59_single_cntr_empty_rangequery.t
# author: shujing zhu
# email: shujingzhu@hengtiansoft.com
# date: June 11, 2013
# description: range query empty cntr

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::BasicTest;
use Test::More tests => 28;
use threads;

my $node;

sub test_Rangequery_get_0{
	my($conn, $cguid, $start_key, $startlen,$end_key,$endlen,$flags,$nobject) = @_;

	my $ret = ZSGetRange (
                $conn,
                cguid         => $cguid,
                keybuf_size   => 100, 
                databuf_size  => 1024,
                keylen_start  => $startlen,
                keylen_end    => $endlen,           
                start_key     => $start_key,
                end_key       => $end_key,
				flags         => $flags
                );
    like($ret, qr/OK.*/,"ZSGetRange:Get $start_key ~ $end_key,keylen =$startlen ,flags=$flags");
	$ret = ZSGetNextRange (
                $conn,
                n_in          => $nobject, 
                check         => "yes",
                );
    like($ret, qr/SERVER_ERROR ZS_QUERY_DONE/, "ZSGetNextRange:Get 0 objects ,$ret");

    $ret = ZSGetRangeFinish($conn);
    like($ret, qr/OK.*/, "ZSGetRangeFinish");
}


sub test_Rangequery_failure{
	my($conn, $cguid, $start_key, $startlen,$end_key,$endlen,$flags,$nobject) = @_;

	my $ret = ZSGetRange (
                $conn,
                cguid         => $cguid,
                keybuf_size   => 100, 
                databuf_size  => 1024,
                keylen_start  => $startlen,
                keylen_end    => $endlen,           
                start_key     => $start_key,
                end_key       => $end_key,
				flags         => $flags
                );
    like($ret, qr/OK.*/,"ZSGetRange:Get $start_key ~ $end_key,keylen =$startlen ,flags=$flags");
	$ret = ZSGetNextRange (
                $conn,
                n_in          => $nobject, 
                check         => "yes",
                );
    like($ret, qr/SERVER_ERROR ZS_FAILURE/, "ZSGetNextRange:Get 0 objects ,$ret");

    $ret = ZSGetRangeFinish($conn);
	like($ret, qr/OK.*/, "ZSGetRangeFinish");
}

sub test_run {

    my $ret;
    my $cguid;
    my $cname = "Tran_Cntr";  
    my $key = 0;
    my $key_len = 10;
    my $val_offset = $key;
    my $val_len = 50;
    my $size = 0;
    my $nobject = 1000;
	my $flags = 'ZS_RANGE_START_GE|ZS_RANGE_END_LE';
	my $flags_reverse = 'ZS_RANGE_START_LE|ZS_RANGE_END_GE';
	my $start_key;
	my $end_key;
    my @prop = (["yes","no","yes","ZS_DURABILITY_HW_CRASH_SAFE","no"],);

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

        $start_key = 0;
        $end_key = 1000;
        test_Rangequery_get_0($node->conn(0),$cguid,$start_key,$key_len,$end_key,$key_len,$flags,$nobject);

        $start_key = 1000;
        $end_key = 0;
        test_Rangequery_get_0($node->conn(0),$cguid,$start_key,$key_len,$end_key,$key_len,$flags_reverse,$nobject);

        $start_key = 2000;
        $end_key = 10000;
        test_Rangequery_get_0($node->conn(0),$cguid,$start_key,$key_len,$end_key,$key_len,$flags,$nobject);
        test_Rangequery_get_0($node->conn(0),$cguid,$start_key,$key_len,$end_key,$key_len,$flags_reverse,$nobject);

        $start_key = 10000;
        $end_key = 1000;
        test_Rangequery_get_0($node->conn(0),$cguid,$start_key,$key_len,$end_key,$key_len,$flags,$nobject);
        test_Rangequery_get_0($node->conn(0),$cguid,$start_key,$key_len,$end_key,$key_len,$flags_reverse,$nobject);

        $start_key = 10000;
        $end_key = 0;
        test_Rangequery_get_0($node->conn(0),$cguid,$start_key,$key_len,$end_key,$key_len,$flags,$nobject);

        $start_key = 0;
        $end_key = 10000;
        test_Rangequery_get_0($node->conn(0),$cguid,$start_key,$key_len,$end_key,$key_len,$flags_reverse,$nobject);

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


