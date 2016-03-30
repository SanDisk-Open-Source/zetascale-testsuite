# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file:10548.t
# author: Jing Xu(Lee)
# email: leexu@hengtiansoft.com
# date: Mar 15, 2013
# description:you can choose sleep(30),sleep(50) or not sleep 

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Test::More tests => 9;

my $node;

sub test_run {

    my $ret;
    my $cguid;
    my $nops = 30000;

    $ret = $node->start (ZS_REFORMAT => 1,);
    like ($ret, qr/OK.*/, 'Node start');

    $ret = ZSOpenContainer (
        $node->conn (0),
        cname            => "cntr0",
        fifo_mode        => "no",
        persistent       => "yes",
        evicting         => "no",
        writethru        => "yes",
        async_writes     => "no",
        size             => 0,
        durability_level => "ZS_DURABILITY_HW_CRASH_SAFE",
        num_shards       => 1,
        flags            => "ZS_CTNR_CREATE",
    );
    $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
    like ($ret, qr/OK.*/,
        "ZSOpenContainer cname=cntr0,cguid=$cguid,fifo_mode=no,persistent=yes,evicting=no,writethru=yes,flags=CREATE");

    $ret = ZSWriteObject (
        $node->conn (0),
        cguid       => "$cguid",
        key_offset  => 0,
        key_len     => 250,
        data_offset => 1000,
        data_len    => 50000,
        nops        => "$nops",
        flags       => "ZS_WRITE_MUST_NOT_EXIST",
    );
    like ($ret, qr/OK.*/, "ZSWriteObject-->cguid=$cguid nops=$nops");

    $ret = ZSReadObject (
        $node->conn (0),
        cguid       => "$cguid",
        key_offset  => 0,
        key_len     => 250,
        data_offset => 1000,
        data_len    => 50000,
        nops        => "$nops",
        check       => "yes",
        keep_read   => "yes",
    );
    like ($ret, qr/OK.*/, "ZSReadObject->cguid=$cguid nops=$nops");

    $ret = ZSCloseContainer ($node->conn (0), cguid => "$cguid",);
    like ($ret, qr/OK.*/, "ZSCloseContainer->cguid=$cguid");
    $ret = ZSDeleteContainer ($node->conn (0), cguid => "$cguid");
    like ($ret, qr/OK.*/, "ZSDeleteContainer->cguid=$cguid ");

    $ret = ZSOpenContainer (
        $node->conn (0),
        cname            => "cntr0",
        fifo_mode        => "no",
        persistent       => "yes",
        evicting         => "no",
        writethru        => "yes",
        async_writes     => "no",
        size             => 0,
        durability_level => "ZS_DURABILITY_HW_CRASH_SAFE",
        num_shards       => 1,
        flags            => "ZS_CTNR_CREATE",
    );
    $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
    like ($ret, qr/OK.*/,
        "ZSOpenContainer cname=cntr0,cguid=$cguid,fifo_mode=no,persistent=yes,evicting=no,writethru=yes,flags=CREATE");

    print("sleep start\n");
#    sleep(30);
    sleep(50);
    print("sleep stop\n");

    $ret = ZSWriteObject (
        $node->conn (0),
        cguid       => "$cguid",
        key_offset  => 0,
        key_len     => 250,
        data_offset => 1000,
        data_len    => 50000,
        nops        => "$nops",
        flags       => "ZS_WRITE_MUST_NOT_EXIST",
    );
    like ($ret, qr/OK.*/, "ZSWriteObject-->cguid=$cguid nops=$nops");

    $ret = ZSReadObject (
        $node->conn (0),
        cguid       => "$cguid",
        key_offset  => 0,
        key_len     => 250,
        data_offset => 1000,
        data_len    => 50000,
        nops        => "$nops",
        check       => "yes",
        keep_read   => "yes",
    );
    like ($ret, qr/OK.*/, "ZSReadObject->cguid=$cguid nops=$nops");
    return;
}

sub test_init {
    $node = Fdftest::Node->new (
        ip    => "127.0.0.1",
        port  => "24422",
        nconn => 1,
    );
    $node->set_ZS_prop (ZS_FLASH_SIZE => 10);
}

sub test_clean {
    $node->stop ();
    $node->set_ZS_prop (ZS_REFORMAT => 1, ZS_FLASH_SIZE => 300);

    return;
}

#
# main
#
{
    test_init ();

    test_run ();

    test_clean ();
}

# clean ENV
END {
    $node->clean ();
}

