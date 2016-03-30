# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file: t/ZS_RangeQuery/01_cguid_invalid_rangequery.t
# author: shujing zhu
# email: shujingzhu@hengtiansoft.com
# date: June 11, 2013
# description: range query cntr with cguid= invalid

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::BasicTest;
use Test::More tests => 7;
use threads;

my $node;

sub test_run {

    my $ret;
    my $cguid;
    my $cname = "Tran_Cntr";  
    my $update_thread;
    my $key = 0;
    my $key_len = 50;
    my $val_offset = $key;
    my $val_len = 50;
    my $update = 10;
    my $size = 1024 * 1024;
    my $nobject = 10;

    $ret = $node->start(
                ZS_REFORMAT  => 1,
           );
    like($ret, qr/OK.*/, 'Node start');

    $ret = ZSOpenContainer(
		        $node->conn(0),
		        cname            => $cname,
		        fifo_mode        => "no",
		        persistent       => "yes",
		        writethru        => "yes",
		        evicting         => "no",
		        size             => $size,
		        durability_level => "ZS_DURABILITY_PERIODIC",
		        async_writes     => "no",
		        num_shards       => 1,
		        flags            => "ZS_CTNR_CREATE"
		        );   
    $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
    like($ret, qr/OK.*/, "ZSopenContainer: $cname, cguid=$cguid");

    $ret = ZSWriteObject(
		        $node->conn(0),
		        cguid         => $cguid,
		        key           => $key,
		        data_offset   => $val_offset,
		        data_len      => $val_len,
		        nops          => $nobject,
		        flags         => "ZS_WRITE_MUST_NOT_EXIST",
		        );
    like($ret, qr/OK.*/, "ZSWriteObject: load $nobject objects to $cname, cguid=$cguid");

    $ret = ZSCloseContainer(
                $node->conn(0),
                cguid      => $cguid,
                );
    like($ret, qr/OK.*/, 'ZSCloseContainer');

    $ret = $node->stop();
    like($ret,qr/.*/,"Node kill");
    $ret = $node->start(ZS_REFORMAT => 0);
    like($ret,qr/OK.*/,"Node Start: REFORMAT=0");
    	
    $ret = ZSDeleteContainer(
                $node->conn(0),
                cguid      => $cguid,
                );
    like($ret, qr/SERVER_ERROR.*/, 'SERVER_ERROR ZSDeleteContainer');

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


