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
use Fdftest::Stress;
use Fdftest::Node;
#use Test::More tests => 3601;
use Test::More tests => 231;
#tests =( 6*($nconn) + 2*$ncntr) * ($loop) + $ncntr*2 + 1
my $node;
my $nconn = 500;
my $ncntr = 1000;
my $loop  = 1;
$nconn = 100;
$ncntr = 10;
$loop  = 20;

sub enumerate{
    my ($connid, $cguid, $keyoffset, $keylen, $datalen, $nops) = @_;
    my ($ret, $msg);
    my $size = int(rand(4194304));

    $ret = ZSEnumerate ($node->conn ($connid), $cguid);
    like ($ret, qr/^OK.*/, $ret);
}

sub setget{
    my ($connid, $cguid, $keyoffset, $keylen, $datalen, $nops) = @_;
    my ($ret, $msg);
    my $size = int(rand(4194304));

    $ret = ZSSetGet ($node->conn ($connid), $cguid, $keyoffset, $keylen, $datalen, $nops);
    like ($ret, qr/^OK.*/, $ret);
}

sub test_run {
    my ($ret, $msg);
    my ($cguid, @cguids, %chash, @threads);
    my $size = 4096;
    $ret = $node->start (
        gdb_switch   => 1,
        ZS_REFORMAT => 1,
    );
    like ($ret, qr/^OK.*/, 'Node start');

    # Create containers with $nconn connections
    for (1 .. $ncntr) {
        my $cname = 'ctrn-' . "$_";
        $ret = ZSOpen ($node->conn (0), $cname,3, $size, "ZS_CTNR_CREATE", "no","ZS_DURABILITY_HW_CRASH_SAFE");
        like ($ret, qr/^OK.*/, $ret);

        if ($ret =~ /^OK cguid=(\d+)/) {
            push(@cguids, $1);
            $chash{$cname} = $1;
        }
        else {
            return;
        }
    }
    my $keyoffset = int(rand(5000)) + 1;
    my ($keylen, $datalen, $maxops, $nops);
    @threads = ();
    for (0 .. $ncntr-1) {
        $keyoffset = $keyoffset + $_;
        $keylen    = int(rand(240)) + 1;
        $datalen   = int(rand(2048000)) + 1;
        $maxops    = int((5000) / (($datalen) / 1000000));
        $nops      = int(rand($maxops / $nconn));
		$datalen	= 1024000;
		$nops		= 500;
        push(@threads, threads->new (\&setget, $_, $cguids[$_], $keyoffset, $keylen, $datalen, $nops));
    }
    $_->join for (@threads);

    for (1 .. $loop) {
        @threads = ();
        for (0 .. $ncntr-1) {
            my $connid = $_;
            push(@threads, threads->new (\&enumerate, $_, $cguids[$_]));
        }
        $_->join for (@threads);
    }

    for (@cguids) {
        $ret = ZSClose ($node->conn (0), $_);
        like ($ret, qr/^OK.*/, $ret);
    }
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

