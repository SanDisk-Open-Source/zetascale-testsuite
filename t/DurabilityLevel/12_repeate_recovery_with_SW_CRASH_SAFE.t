# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file:
# author: yiwen lu
# email: yiwenlu@hengtiansoft.com
# date: Jan 3, 2013
# description:

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::TestCase;
use Test::More tests => 34;

my $node;

sub test_run {
    my $cguid;
    my $ret;
    my $repeat_time = 4;
    my %keyoff_keylen;

=disable async=yes
    #async_writes=yes --------------------------------------------------------------------------------------------------------
    print '<<< test with async_writes=yes >>>' . "\n";
    $ret = $node->start (ZS_REFORMAT => 1);
    like ($ret, qr/OK.*/, "Node Start: ZS_REFORMAT=1");

    #OpenContainer {$conn,$ctrname,$flags,$size,$choice,$durab,$async_writes);
    $ret =
        OpenContainer ($node->conn (0), "ctr-1", "ZS_CTNR_CREATE", 10485760, 4, "ZS_DURABILITY_SW_CRASH_SAFE", "yes");
    $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);

    my $keyoffset = 0;
    my $keylen    = 50;
    foreach (0 .. $repeat_time - 1) {
        WriteReadObjects ($node->conn (0), $cguid, $keyoffset, $keylen, 1000, 50, 50000);
        $keyoff_keylen{$keyoffset} = $keylen;
        $keyoffset                 = $keyoffset + 100;
        $keylen                    = $keylen + 20;

        $ret = $node->kill ();
        like (0, qr/0/, "Node kill");
        $ret = $node->start (ZS_REFORMAT => 0);
        like ($ret, qr/OK.*/, "Node Start: REFORMAT=0");

        OpenContainer ($node->conn (0), "ctr-1", "ZS_CTNR_RW_MODE", 10485760, 4, "ZS_DURABILITY_SW_CRASH_SAFE",
            "yes");
        foreach (keys %keyoff_keylen) {
            ReadObjects ($node->conn (0), $cguid, $_, $keyoff_keylen{$_}, 1000, 50, 50000);
        }
    }
    CloseContainer ($node->conn (0), $cguid);
    DeleteContainer ($node->conn (0), $cguid);
    $ret = $node->stop ();
    like ($ret, qr/OK.*/, "Node Stop");
=cut
    #async_writes=no ----------------------------------------------------------------------------------------------------------
    my $keyoffset     = 0;
    my $keylen        = 50;
    %keyoff_keylen = ();

    print '<<< test with async_writes=no >>>' . "\n";
    $ret = $node->start (ZS_REFORMAT => 1);
    like ($ret, qr/OK.*/, "Node Start: ZS_REFORMAT=1");

    #OpenContainer {$conn,$ctrname,$flags,$size,$choice,$durab,$async_writes);
    $ret = OpenContainer ($node->conn (0), "ctr-1", "ZS_CTNR_CREATE", 10485760, 4, "ZS_DURABILITY_SW_CRASH_SAFE", "no");
    $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
    foreach (0 .. $repeat_time - 1) {
        WriteReadObjects ($node->conn (0), $cguid, $keyoffset, $keylen, 1000, 50, 50000);
        $keyoff_keylen{$keyoffset} = $keylen;
        $keyoffset                 = $keyoffset + 100;
        $keylen                    = $keylen + 20;

        $ret = $node->kill ();
        like (0, qr/0/, "Node kill");
        $ret = $node->start (ZS_REFORMAT => 0);
        like ($ret, qr/OK.*/, "Node Start: REFORMAT=0");

        OpenContainer ($node->conn (0), "ctr-1", "ZS_CTNR_RW_MODE", 10485760, 4, "ZS_DURABILITY_SW_CRASH_SAFE", "no");
        foreach (keys %keyoff_keylen) {
            ReadObjects ($node->conn (0), $cguid, $_, $keyoff_keylen{$_}, 1000, 50, 50000);
        }
    }
    CloseContainer ($node->conn (0), $cguid);
    DeleteContainer ($node->conn (0), $cguid);

    return;
}

sub test_init {
    $node = Fdftest::Node->new (
        ip    => "127.0.0.1",
        port  => "24422",
        nconn => 1,
        prop  => "$Bin/../../conf/stress.prop",
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

