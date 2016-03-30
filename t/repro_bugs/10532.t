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
use Test::More tests => 20;

my $node;

sub test_run {
    my $cguid;
    my $ret;
    my $keyoffset  = 0;
    my $keylen     = 50;
    my $dataoffset = 1000;
    my $datalen    = 50;
    my $nops       = 5000;
    my $loops      = 2;
    #async_writes=yes --------------------------------------------------------------------------------------------------------
    $ret = $node->start (ZS_REFORMAT => 1);
    like ($ret, qr/OK.*/, "Node Start: ZS_REFORMAT=1");

    for(my $i = 0; $i < $loops; $i++) {
        print "=CYCLE:$i=\n";
        $ret = OpenContainer ($node->conn (0), "ctr-1", "ZS_CTNR_CREATE", 10485760, 4, "ZS_DURABILITY_HW_CRASH_SAFE", "no");
        $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
        WriteReadObjects ($node->conn (0), $cguid, $keyoffset, $keylen, $dataoffset, $datalen, $nops);
        CloseContainer ($node->conn (0), $cguid);

        $ret = $node->stop ();
        like ($ret, qr/OK.*/, "Node Stop");
        $ret = $node->start (ZS_REFORMAT => 0);
        like ($ret, qr/OK.*/, "Node Start: REFORMAT=0");

        OpenContainer ($node->conn (0), "ctr-1", "ZS_CTNR_RW_MODE", 10485760, 4, "ZS_DURABILITY_HW_CRASH_SAFE", "no");
        CloseContainer ($node->conn (0), $cguid);
        DeleteContainer ($node->conn (0), $cguid);

    }
    $ret = $node->stop ();
    like ($ret, qr/OK.*/, "Node Stop");
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


