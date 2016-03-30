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
use Test::More tests => 10;

my $node;

sub test_run {

    my $ret;
    my $cguid;

    foreach my $write ("yes"){
=disable async is yes        
        print "<<test with async_writes=yes , writherhtu=$write>>\n";
        $ret = $node->start (ZS_REFORMAT => 1,);
        like ($ret, qr/OK.*/, 'Node start');

        $ret = ZSOpenContainer (
                $node->conn (0),
                cname            => "demo0",
                fifo_mode        => "no",
                persistent       => "yes",
                evicting         => "no",
                writethru        => "$write",
                async_writes     => "yes",
                size             => 1048576,
                durability_level => "ZS_DURABILITY_SW_CRASH_SAFE",
                num_shards       => 1,
                flags            => "ZS_CTNR_CREATE",
                );
        $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
        like ($ret, qr/OK.*/, "ZSOpenContainer canme=demo0,cguid=$cguid,async_writes=yes,writethru=$write,flags=CREATE");

        $ret = ZSGetContainerProps ($node->conn (0), cguid => "$cguid",);
        like ($ret, qr/.*durability_level=1.*/, "durability_level=ZS_DURABILITY_SW_CRASH_SAFE");

        $ret = ZSWriteObject (
                $node->conn (0),
                cguid       => "$cguid",
                key_offset  => 0,
                key_len     => 25,
                data_offset => 1000,
                data_len    => 50,
                nops        => 640,
                flags       => "ZS_WRITE_MUST_NOT_EXIST",
                );
        like ($ret, qr/OK.*/, "ZSWriteObject-->cguid=$cguid nops=640");

        $ret = ZSReadObject (
                $node->conn (0),
                cguid       => "$cguid",
                key_offset  => 0,
                key_len     => 25,
                data_offset => 1000,
                data_len    => 50,
                nops        => 640,
                check       => "yes",
                keep_read   => "yes",
                );
        like ($ret, qr/OK.*/, "ZSReadObject->cguid=$cguid nops=640");

        $node->kill ();
        like (0, qr/0/, 'Node kill');
        
#        $node->stop();

        $ret = $node->start (ZS_REFORMAT => 0,);
        like ($ret, qr/OK.*/, 'Node restart');

        $ret = ZSOpenContainer (
                $node->conn (0),
                cname            => "demo0",
                fifo_mode        => "no",
                persistent       => "yes",
                evicting         => "no",
                writethru        => "$write",
                async_writes     => "yes",
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
                nops        => 640,
                check       => "yes",
                keep_read   => "yes",
                );
        like ($ret, qr/OK.*/, "ZSReadObject->cguid=$cguid nops=640");
        $ret = $node->stop ();
        like ($ret, qr/OK.*/, 'Node stop');

=cut
        print "<<test with async_writes=no,writethru=$write>>\n";

        $ret = $node->start (ZS_REFORMAT => 1,);
        like ($ret, qr/OK.*/, 'Node start');

        $ret = ZSOpenContainer (
                $node->conn (0),
                cname            => "demo0",
                fifo_mode        => "no",
                persistent       => "yes",
                evicting         => "no",
                writethru        => "$write",
                async_writes     => "no",
                size             => 1048576,
                durability_level => "ZS_DURABILITY_SW_CRASH_SAFE",
                num_shards       => 1,
                flags            => "ZS_CTNR_CREATE",
                );
        $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
        like ($ret, qr/OK.*/, "ZSOpenContainer canme=demo0,cguid=$cguid,async_writes=no,writethru=$write,flags=CREATE");

        $ret = ZSGetContainerProps ($node->conn (0), cguid => "$cguid",);
        like ($ret, qr/.*durability_level=1.*/, "durability_level=ZS_DURABILITY_SW_CRASH_SAFE");

        $ret = ZSWriteObject (
                $node->conn (0),
                cguid       => "$cguid",
                key_offset  => 0,
                key_len     => 25,
                data_offset => 1000,
                data_len    => 50,
                nops        => 640,
                flags       => "ZS_WRITE_MUST_NOT_EXIST",
                );
        like ($ret, qr/OK.*/, "ZSWriteObject-->cguid=$cguid nops=640");

        $ret = ZSReadObject (
                $node->conn (0),
                cguid       => "$cguid",
                key_offset  => 0,
                key_len     => 25,
                data_offset => 1000,
                data_len    => 50,
                nops        => 640,
                check       => "yes",
                keep_read   => "yes",
                );
        like ($ret, qr/OK.*/, "ZSReadObject->cguid=$cguid nops=640");

        $node->kill ();
        like (0, qr/0/, 'Node kill');
        
#        $node->stop();

        $ret = $node->start (ZS_REFORMAT => 0,);
        like ($ret, qr/OK.*/, 'Node restart');

        $ret = ZSOpenContainer (
                $node->conn (0),
                cname            => "demo0",
                fifo_mode        => "no",
                persistent       => "yes",
                evicting         => "no",
                writethru        => "$write",
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
                nops        => 640,
                check       => "yes",
                keep_read   => "yes",
                );
        like ($ret, qr/OK.*/, "ZSReadObject->cguid=$cguid nops=640");

        $ret = $node->stop ();
        like ($ret, qr/OK.*/, 'Node stop');
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
