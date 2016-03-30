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
use Test::More tests => 6;
use threads;

my $node;

sub write_obj {
    my ($conn,$cguid) = @_;
    ZSWriteObject (
            $node->conn ($conn),
            cguid       => "$cguid",
            key_offset  => 0,
            key_len     => 25,
            data_offset => 1000,
            data_len    => 50,
            nops        => 100000,
            flags       => "ZS_WRITE_MUST_NOT_EXIST",
            );
}

sub test_run {
    my $ret;
    my $cguid;
    my $obj_num = 0;
    my $read_failed = 0;
    my $diff;

    print "<<test with async_writes=no>>\n";
    $ret = $node->start (ZS_REFORMAT => 1,);
    like ($ret, qr/OK.*/, 'Node start');

    $ret = ZSOpenContainer (
        $node->conn (0),
        cname            => "demo0",
        fifo_mode        => "no",
        persistent       => "yes",
        evicting         => "no",
        writethru        => "yes",
        async_writes     => "no",
        size             => 1048576,
        durability_level => "ZS_DURABILITY_SW_CRASH_SAFE",
        num_shards       => 1,
        flags            => "ZS_CTNR_CREATE",
    );
    $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
    like ($ret, qr/OK.*/, "ZSOpenContainer canme=demo0,cguid=$cguid,async_writes=no,flags=CREATE");

    $ret = ZSGetContainerProps ($node->conn (0), cguid => "$cguid",);
    like ($ret, qr/.*durability_level=1.*/, "durability_level=ZS_DURABILITY_SW_CRASH_SAFE");

    threads->new(\&write_obj,1,$cguid);
    sleep(2);

    $node->kill_and_dump();
    $ret = $node->start (ZS_REFORMAT => 0,);
    like ($ret, qr/OK.*/, 'Node restart');

    $ret = ZSOpenContainer (
            $node->conn (0),
            cname            => "demo0",
            fifo_mode        => "no",
            persistent       => "yes",
            evicting         => "no",
            writethru        => "yes",
            async_writes     => "no",
            size             => 1048576,
            durability_level => "ZS_DURABILITY_SW_CRASH_SAFE",
            flags            => "ZS_CTNR_RW_MODE",
            );
    $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
    like ($ret, qr/OK.*/, "ZSOpenContainer cguid=$cguid flags=RW_MODE");

    $ret = ZSReadObject (
            $node->conn (0),
            cguid       => "$cguid",
            key_offset  => 0,
            key_len     => 25,
            data_offset => 1000,
            data_len    => 50,
            nops        => 100000,
            check       => "yes",
            keep_read   => "yes",
            );
    $read_failed = $1 if ($ret =~ /SERVER_ERROR (\d+)(\D+)(\d+)(\D+)/);
    print "read_failed=$read_failed\n";
    $ret = $node->dump_ctnr_success_set();
    $obj_num = $1 if ($ret =~ /\d+.*= (\d+)/);
    $diff = (100000-$read_failed)-$obj_num;
    if ( $diff >= 0 && $diff <= 5)
    {
        like (0, qr/0/, "ZSReadObject:diff is $diff , set obj number is $obj_num,read succeed num is ".(100000-$read_failed));
    }
    else
    {
        like (0, qr/1/, "ZSReadObject:diff is $diff , set obj number is $obj_num,read succeed num is ".(100000-$read_failed));
    }


    return;
}

sub test_init {
    $node = Fdftest::Node->new (
        ip    => "127.0.0.1",
        port  => "24422",
        nconn => 2,
    );

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

# clean ENV
END {
    $node->clean ();
}

