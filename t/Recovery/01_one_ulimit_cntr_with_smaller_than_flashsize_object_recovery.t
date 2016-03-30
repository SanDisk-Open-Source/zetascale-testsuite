# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file:01_one_ulimit_cntr_with_smaller_than_flashsize_object_recovery
# author: Jing Xu(Lee)
# email: leexu@hengtiansoft.com
# date: Feb 21, 2013
# description: recovery test for one unlimited container which is set with smaller than flash size objects

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Test::More tests => 25;

my $node;

sub test_run {

    my $ret;
    my $cguid;
    my @prop = (["yes","no","yes","ZS_DURABILITY_HW_CRASH_SAFE","no"],);
    #my @data = ([50, 64000, 30000], [100, 128000, 30000], [150, 512, 180000]);
    my @data = ([64, 16000, 15000], [74, 32000, 15000], [84, 64000, 15000], [94, 128000, 15000], [104, 48, 300000]);

    foreach my $p(@prop){
        $ret = $node->start (gdb_switch => 1, ZS_REFORMAT => 1,);
        like ($ret, qr/OK.*/, 'Node start');

        $ret = ZSOpenContainer(
            $node->conn(0),
	    cname            => "cntr0",
	    fifo_mode        => "no",
	    persistent       => $$p[0],
	    evicting         => $$p[1],
	    writethru        => $$p[2],
	    async_writes     => $$p[4],
	    size             => 0,
	    durability_level => $$p[3],
	    num_shards       => 1,
	    flags            => "ZS_CTNR_CREATE",
        );
        $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
        like ($ret, qr/OK.*/,
            "ZSOpenContainer cname=cntr0,cguid=$cguid,fifo_mode=no,persistent=$$p[0],evicting=$$p[1],writethru=$$p[2],flags=CREATE"
        );

        $ret = ZSGetContainerProps ($node->conn (0), cguid => "$cguid",);
        $ret =~ /.*size=(\d+) kb/;
        is ($1, 0, "ZSGetContainerProps:size=$1 kb");

        foreach my $d(@data){
            $ret = ZSWriteObject(
                $node->conn(0),
		cguid         => "$cguid",
		key_offset    => 0,
		key_len       => $$d[0],
		data_offset   => 1000,
		data_len      => $$d[1],
		nops          => $$d[2],
		flags         => "ZS_WRITE_MUST_NOT_EXIST",
            );
            like($ret, qr/OK.*/, "ZSWriteObject-->cguid=$cguid nops=$$d[2]");

            $ret = ZSReadObject(
                $node->conn(0),
		cguid         => "$cguid",
		key_offset    => 0,
		key_len       => $$d[0],
		data_offset   => 1000,
		data_len      => $$d[1],
		nops          => $$d[2],
		check         => "yes",
		keep_read     => "yes",
	    );
            like($ret, qr/OK.*/, "ZSReadObject->cguid=$cguid nops=$$d[2]");
        }

        $ret = ZSFlushContainer ($node->conn (0), cguid => "$cguid",);
        like ($ret, qr/OK.*/, "ZSFlushContainer->cguid=$cguid");

        $ret = ZSCloseContainer ($node->conn (0), cguid => "$cguid",);
        like ($ret, qr/OK.*/, "ZSCloseContainer->cguid=$cguid");

        $ret = $node->stop ();
        like ($ret, qr/OK.*/, 'Node stop');

        $ret = $node->start (ZS_REFORMAT => 0,);
        like ($ret, qr/OK.*/, 'Node restart');

        $ret = ZSOpenContainer(
            $node->conn(0),
            cname            => "cntr0",
            fifo_mode        => "no",
            persistent       => $$p[0],
            evicting         => $$p[1],
            writethru        => $$p[2],
            async_writes     => $$p[4],
            size             => 0,
            durability_level => $$p[3],
            num_shards       => 1,
            flags            => "ZS_CTNR_RW_MODE",
        );
        $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
        like ($ret, qr/OK.*/, "ZSOpenContainer cguid=$cguid flags=RW_MODE");

        foreach my $d(@data){
            $ret = ZSReadObject(
                $node->conn(0),
                cguid         => "$cguid",
                key_offset    => 0,
                key_len       => $$d[0],
                data_offset   => 1000,
                data_len      => $$d[1],
                nops          => $$d[2],
                check         => "yes",
                keep_read     => "yes",
            );
            like($ret, qr/OK.*/, "ZSReadObject->cguid=$cguid nops=$$d[2]");
        }

        $ret = ZSCloseContainer ($node->conn (0), cguid => "$cguid",);
        like ($ret, qr/OK.*/, "ZSCloseContainer->cguid=$cguid");
        $ret = ZSDeleteContainer ($node->conn (0), cguid => "$cguid");
        like ($ret, qr/OK.*/, "ZSDeleteContainer->cguid=$cguid ");

        $node->stop ();
        $node->set_ZS_prop (ZS_REFORMAT => 1);
    }
    return;
}

sub test_init {
    $node = Fdftest::Node->new (
        ip    => "127.0.0.1",
        port  => "24422",
        nconn => 1,
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

