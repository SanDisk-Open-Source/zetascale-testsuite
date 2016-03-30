# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file:
# author: Jing Xu(Lee)
# email: leexu@hengtiansoft.com
# date: Jul 9, 2013
# description:

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Stress;
use Fdftest::BasicTest;
use Fdftest::Node;
use Test::More tests => 3077;

my $node;

sub test_run {
    my ($ret, $cguid);
    my $cname = 'Ctrn';
    my $size  = 0;
    my $nops  = 50 * 1024 / 2;
    my $key = 0;
    my $key_len    = 32;
    my $val_offset = $key;
    my $val_len    = 1024 * 1024;
    my $flags = 'ZS_RANGE_START_GE|ZS_RANGE_END_LE';

    $ret = $node->start (
        #gdb_switch   => 1,
        ZS_REFORMAT => 1,
    );
    like ($ret, qr/^OK.*/, 'Node start');

    $ret = ZSOpenContainer (
        $node->conn (0),
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
    $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
    like ($ret, qr/OK.*/, "ZSopenContainer: $cname, cguid=$cguid");

    $ret = ZSWriteObject(
                $node->conn(0),
                cguid         => $cguid,
                key           => $key,
                key_len       => $key_len,
                data_offset   => $val_offset,
                data_len      => $val_len,
                nops          => $nops,
                flags         => "ZS_WRITE_MUST_NOT_EXIST",
                );
    like($ret, qr/OK.*/, "ZSWriteObject: load $nops objects keylen= $key_len datalen=$val_len to $cname, cguid=$cguid");

    for (1..1024) {
        my $start_key = $key + 25 * ($_ - 1);
        my $end_key = $start_key + 25 - 1;
        $ret = ZSGetRange (
            $node->conn (0),
            cguid        => $cguid,
            keybuf_size  => 60,
            databuf_size => 1024,
            keylen_start => $key_len,
            keylen_end   => $key_len,
            start_key    => $start_key,
            end_key      => $end_key,
            flags        => $flags,
        );
        like ($ret, qr/OK.*/, "ZSGetRange:Get $start_key ~ $end_key,keylen=$key_len,flags=$flags");

        $ret = ZSGetNextRange (
            $node->conn (0),
            n_in  => $nops/1024,
            check => "yes",
        );
        my $n_out = $1 if ($ret =~ /OK n_out=(\d+)/);
        like ($ret, qr/OK.*/, "ZSGetNextRange:Get $n_out objects ,$ret");

        $ret = ZSGetRangeFinish ($node->conn (0));
        like ($ret, qr/OK.*/, "ZSGetRangeFinish");
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

    return;
}

sub test_init {
    $node = Fdftest::Node->new (
        ip    => "127.0.0.1",
        port  => "24422",
        nconn => 1,
        prop  => "$Bin/../../conf/stress.prop",
    );
    return;
}

sub test_clean {
    $node->stop ();
    $node->set_ZS_prop (ZS_REFORMAT => 1);

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
