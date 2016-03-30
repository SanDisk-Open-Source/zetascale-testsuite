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
use Fdftest::Stress;
use Fdftest::Node;
use Test::More tests => 4151;

#tests =( 6*($nconn) + 2*$ncntr) * ($loop) + $ncntr*2 + 1
my $node;
my $nconn = 500;
my $ncntr = 1000;
my $loop  = 1;
$nconn = 100;
$ncntr = 50;
$loop  = 5;
my @data = ([50, 64000, 1], [100, 128000, 1], [150, 512, 6]);

sub worker {
    my ($connid, $cguid, $keyoffset, $nops) = @_;
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
    my ($cguid, @cguids, %chash, @threads);
    my $size = 0;
    my @prop = ([3, "ZS_DURABILITY_HW_CRASH_SAFE", "no"],);

    $ret = $node->start (
        gdb_switch   => 1,
        ZS_REFORMAT => 1,
	threads		=> $nconn,
    );
    like ($ret, qr/^OK.*/, 'Node start');

    foreach my $p(@prop){
        @cguids = ();
        @threads = ();          
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
		push(@threads, threads->new (\&worker, $_, $cguids[ rand(@cguids) ], $keyoffset, $nops));
	    }
	    $_->join for (@threads);

            #=comment
	    @threads = ();
	    for (1 .. $ncntr) {
                my $cname = 'ctrn-' . "$_";
		push(@threads, threads->new (\&reopen, $_, $chash{$cname}, $cname, $size, $$p[0], $$p[2], $$p[1]));
	    }
	    $_->join for (@threads);

            #=cut
	}

        #$ret = ZSGetConts($node->conn(0),$ncntr);
        #$msg = substr($ret, 0, index($ret,"ZSGet"));
        #like($ret, qr/^OK.*/, $msg."ZSGetContainers");
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

