#----------------------------------------------------------------------------
# ZetaScale
# Copyright (c) 2016, SanDisk Corp. and/or all its affiliates.
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU Lesser General Public License version 2.1 as published by the Free
# Software Foundation;
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License v2.1 for more details.
#
# A copy of the GNU Lesser General Public License v2.1 is provided with this package and
# can also be found at: http:#opensource.org/licenses/LGPL-2.1
# You should have received a copy of the GNU Lesser General Public License along with
# this program; if not, write to the Free Software Foundation, Inc., 59 Temple
# Place, Suite 330, Boston, MA 02111-1307 USA.
#----------------------------------------------------------------------------

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

	my ($counter, $pg, $osd, $dataoffset, $datalen, $nops) =(0, "pgaaaa", "OSD_0",100, 100, 1000);
    	$ret = WriteLogObjects ($node->conn (0), $cguid, $counter, $pg, $osd, $dataoffset, $datalen, $nops);
	like ($ret, qr/^OK.*/, $ret);
	sleep(15);
	system("tail -n 100 /tmp/zsstats.log | grep -E 'num_objs|used_space'");

    	$ret = DeleteLogObjects ($node->conn (0), $cguid, $counter, $pg, $osd, $nops);
	like ($ret, qr/^OK.*/, $ret);
	sleep(15);
	system("tail -n 100 /tmp/zsstats.log | grep -E 'num_objs|used_space'");

    	$ret = WriteLogObjects ($node->conn (0), $cguid, $counter + $nops , $pg, $osd, $dataoffset, $datalen, 1, "ZS_WRITE_TRIM");
	like ($ret, qr/^OK.*/, $ret);
	sleep(15);
	system("tail -n 100 /tmp/zsstats.log | grep -E 'num_objs|used_space'");

    	$ret = ReadLogObjects ($node->conn (0), $cguid, $counter, $pg, $osd, $dataoffset, $datalen, $nops);
	like ($ret, qr/^Error.*/, $ret);

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

