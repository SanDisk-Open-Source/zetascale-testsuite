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
use Test::More 'no_plan';

#tests =( 6*($nconn) + 2*$ncntr) * ($loop) + $ncntr*2 + 1
my $node;
my $nconn = 100;
my $loop  = 2;
#my @data = ([50, 64000, 1], [100, 128000, 1], [150, 512, 6]);
my @data = ([64, 16000, 1], [74, 32000, 1], [84, 64000, 1], [94, 128000, 1], [104, 48, 20]);

sub worker {
    my ($connid, $cguid, $keyoffset, $nops, $ncntr) = @_;
    my ($ret, $msg);

    foreach my $d(@data){
        $ret = ZSSetGet ($node->conn ($connid), $cguid, $keyoffset, $$d[0], $$d[1], $nops*$$d[2]);
        like ($ret, qr/^OK.*/, $ret);
    }
    $ret = ZSFlushRandom ($node->conn ($connid), $cguid, $keyoffset);
    like ($ret, qr/^OK.*/, $ret);
    $ret = ZSEnumerate ($node->conn ($connid), $cguid);
    like ($ret, qr/^OK.*/, $ret);
    $ret = ZSGetProps ($node->conn ($connid), $cguid);
    $msg = substr($ret, 0, index($ret, "ZSGet"));
    like ($ret, qr/^OK.*/, $msg . "ZSGetContainerProps");
    $ret = ZSGetConts ($node->conn ($connid), $ncntr);
    $msg = substr($ret, 0, index($ret, "ZSGet"));
    like ($ret, qr/^OK.*/, $msg . "ZSGetContainers");
}

sub reopen {
    my ($connid, $cguid, $cname, $size, $choice, $async_writes, $durability) = @_;
    my $ret;
    $ret = ZSClose ($node->conn ($connid), $cguid);
    like ($ret, qr/^OK.*/, $ret);
    $ret = ZSOpen ($node->conn ($connid), $cname, $choice, $size, "ZS_CTNR_RW_MODE", $async_writes, $durability);
    like ($ret, qr/^OK.*/, $ret);
}

sub recreate {
    my ($connid, $cguid, $cname, $size, $choice, $async_writes, $durability) = @_;
    my $ret;
    $ret = ZSClose ($node->conn ($connid), $cguid);
    like ($ret, qr/^OK.*/, $ret);
    $ret = ZSDelete ($node->conn ($connid), $cguid);
    like ($ret, qr/^OK.*/, $ret);
    $ret = ZSOpen ($node->conn ($connid), $cname, $choice, $size, "ZS_CTNR_RW_MODE", $async_writes, $durability);
    like ($ret, qr/^OK.*/, $ret);
}

sub test_run {
    my ($ret, $msg);
    my ($cguid, @cguids, %chash, @threads, $ncntr);
    my $size = 0;
    my @cnums = (2000, 4000, 8000, 16000);
    my @prop = ([3, "ZS_DURABILITY_HW_CRASH_SAFE", "no"],);

    $ret = $node->set_ZS_prop (ZS_MAX_NUM_CONTAINERS => 64000);
    like ($ret, qr//, 'set ZS_MAX_NUM_CONTAINERS to 64K');

    $ret = $node->start (
        gdb_switch   => 1,
        ZS_REFORMAT => 1,
	threads		=> $nconn,
    );
    like ($ret, qr/^OK.*/, 'Node start');

    foreach my $p(@prop){
        @cguids = ();
        @threads = ();  
        $ncntr = $cnums[ rand(@cnums) ];
        print "Cntr num=$ncntr\n";
        
        # Create containers with $nconn connections
        for (1 .. $ncntr) {
            my $cname = 'ctrn-' . "$_";
            $ret = ZSOpen ($node->conn (0), $cname, $$p[0], $size, "ZS_CTNR_CREATE", $$p[2], $$p[1]);
	    like ($ret, qr/^OK.*/, $ret);

	    if ($ret =~ /^OK cguid=(\d+)/) {
                push(@cguids, $1);
		$chash{$cname} = $1;
	    }
	    else {
                return;
	    }
	}

	for (1 .. $loop) {
            my $keyoffset = int(rand(5000)) + 1;
	    my ($keylen, $datalen, $maxops, $nops);
	    @threads = ();
	    for (1 .. $nconn) {
                $keyoffset = $keyoffset + $_;
		$keylen    = int(rand(240)) + 1;
		$datalen   = int(rand(2048000)) + 1;
		$maxops    = int((5000) / (($datalen) / 1000000));
		$nops      = int(rand($maxops / $nconn));
		my $connid = $_;
		push(@threads, threads->new (\&worker, $_, $cguids[ rand(@cguids) ], $keyoffset, $nops, $ncntr));
	    }
	    $_->join for (@threads);

	    @threads = ();
	    for (1 .. $ncntr) {
                my $cname = 'ctrn-' . "$_";
		push(@threads, threads->new (\&reopen, $_%$nconn, $chash{$cname}, $cname, $size, $$p[0], $$p[2], $$p[1]));
	    }
	    $_->join for (@threads);

	}

	for (@cguids) {
            $ret = ZSClose ($node->conn (0), $_);
	    like ($ret, qr/^OK.*/, $ret);
            $ret = ZSDelete ($node->conn (0), $_);
            like ($ret, qr/^OK.*/, $ret);
	}
    }
    return;
}

sub test_init {
    $node = Fdftest::Node->new (
        ip    => "127.0.0.1",
        port  => "24422",
        nconn => $nconn+1,
    );
    return;
}

sub test_clean {
    $node->stop ();
    $node->set_ZS_prop (ZS_REFORMAT => 1, ZS_MAX_NUM_CONTAINERS => 6000);

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

