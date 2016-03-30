# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file: basic.pl
# author: yiwen sun
# email: yiwensun@hengtiansoft.com
# date: Oct 15, 2012
# description: basic sample for testcase

#!/usr/bin/perl

use strict;
use warnings;
use Switch;
use threads;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::UnifiedAPI;
use Fdftest::Node;
use Test::More tests => 480;

#tests =( 6*($nconn+1) + 2) * ($loop+1) + 4
my $node;
my $nconn = 256;
my $loop  = 10;
$nconn = 10;
$loop  = 1;

sub test_run {
    my $ret;
    my $cguid;
    my @threads;
    my $size = 0;
    my @prop = ([3, "ZS_DURABILITY_HW_CRASH_SAFE", "no"],);

    $ret = $node->start (
        ZS_REFORMAT => 1,
    );
    like ($ret, qr/^OK.*/, 'Node start');

        my $ctrname = 'ctrn-01';
        $ret = OpenContainer ($node->conn (0), $ctrname, 3, $size, "ZS_CTNR_CREATE", "no", "ZS_DURABILITY_HW_CRASH_SAFE", "LOGGING");
        like ($ret, qr/^OK.*/, $ret);

        if ($ret =~ /^OK cguid=(\d+)/) {
            $cguid = $1;
        }
        else {
            return;
        }

	my $nops =10;
    	$ret = WriteLogObjects ($node->conn (0), $cguid, 0, "pgaaaa0", "OSD_0", 100, 100, $nops, 0);
	like ($ret, qr/^OK.*/, $ret);

    	$ret = ReadLogObjects ($node->conn (0), $cguid, 0, "pgaaaa0", "OSD_0", 100, 100, $nops);
	like ($ret, qr/^OK.*/, $ret);

    	$ret = WriteLogObjects ($node->conn (0), $cguid, 0, "pgaaaa0", "OSD_0", 100, 100, $nops, "ZS_WRITE_TRIM");
	like ($ret, qr/^OK.*/, $ret);

    	$ret = ReadLogObjects ($node->conn (0), $cguid, 0, "pgaaaa0", "OSD_0", 100, 100, $nops);
	like ($ret, qr/^Error.*/, $ret);

    	$ret = WriteLogObjects ($node->conn (0), $cguid, 0, "pgaaaa0", "OSD_0", 100, 100, $nops, "ZS_WRITE_MUST_NOT_EXIST");
	like ($ret, qr/^OK.*/, $ret);

    	#$ret = DeleteLogObjects ($node->conn (0), $cguid, 0, "pgaaaa0", "OSD_0", $nops);
	#like ($ret, qr/^OK.*/, $ret);

    	$ret = ReadLogObjects ($node->conn (0), $cguid, 0, "pgaaaa0", "OSD_0", 100, 100, $nops);
	like ($ret, qr/^OK.*/, $ret);

        $ret = CloseContainer ($node->conn (0), $cguid);
        like ($ret, qr/^OK.*/, $ret);
        $ret = OpenContainer ($node->conn (0), $ctrname, 3, $size, "ZS_CTNR_RW_MODE", "no", "ZS_DURABILITY_HW_CRASH_SAFE", "LOGGING");
        like ($ret, qr/^OK.*/, $ret);
	
    	$ret = WriteLogObjects ($node->conn (0), $cguid, 0, "pgaaaa0", "OSD_0", 100, 100, $nops, 0);
	like ($ret, qr/^OK.*/, $ret);

    	$ret = ReadLogObjects ($node->conn (0), $cguid, 0, "pgaaaa0", "OSD_0", 100, 100, $nops);
	like ($ret, qr/^OK.*/, $ret);

	return;
}

sub test_init {
    $node = Fdftest::Node->new (
        ip    => "127.0.0.1",
        port  => "24422",
        nconn => $nconn,
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

# clean ENV
END {
    $node->clean ();
}

