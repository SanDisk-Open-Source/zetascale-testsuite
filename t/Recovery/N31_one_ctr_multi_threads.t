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
use Test::More tests => 245;

my $node;
my $nconn = 16;
#my @data = ([64000, 7000], [128000, 7000], [512, 42000]);
my @data = ([16000, 3000], [32000, 3000], [64000, 3000], [128000, 3000], [48, 60000]);

sub worker_write {
    my ($connid, $cguid, $keyoffset, $keylen) = @_;
    my $i = 0;

    foreach my $d(@data){
        my $ret = ZSSet ($node->conn ($connid), $cguid, $keyoffset, $keylen+$i, $$d[0], $$d[1], "ZS_WRITE_MUST_NOT_EXIST");
        like ($ret, qr/^OK.*/, $ret);
        $i++;
    }
}

sub worker_read {
    my ($connid, $cguid, $keyoffset, $keylen) = @_;
    my $i = 0;

    foreach my $d(@data){
        my $ret = ZSGet ($node->conn ($connid), $cguid, $keyoffset, $keylen+$i, $$d[0], $$d[1]);
        like ($ret, qr/^OK.*/, $ret);
        $i++;
    }
}

sub test_run {
    my ($ret, $msg);
    my @threads;
    my $cname= "demo0";
    my ($keyoffset, $keylen);
    my $size = 0;
    my @prop = ([3, "ZS_DURABILITY_HW_CRASH_SAFE", "no"],);

    $ret = $node->start (
#        gdb_switch   => 1,
        ZS_REFORMAT => 1,
    );
    like ($ret, qr/^OK.*/, 'Node start');

    foreach my $p(@prop){
        $ret = ZSOpen ($node->conn (0), $cname, $$p[0], $size, "ZS_CTNR_CREATE", $$p[2], $$p[1]);
	like ($ret, qr/^OK.*/, $ret);
	my $cguid = $1 if ($ret =~ /^OK cguid=(\d+)/) ; 

	@threads = ();
	($keyoffset, $keylen) = (1000,50);
	for (1 .. $nconn) {
            push(@threads, threads->new (\&worker_write, $_, $cguid, $keyoffset, $keylen));
            #$keyoffset = $keyoffset + 2;
	    $keylen = $keylen + 5;
	}
	$_->join for (@threads);

	@threads = ();
	($keyoffset, $keylen) = (1000,50);
	for (1 .. $nconn) {
            push(@threads, threads->new (\&worker_read, $_, $cguid, $keyoffset, $keylen));
	    #$keyoffset = $keyoffset + 2;
            $keylen = $keylen + 5;
	}
	$_->join for (@threads);

	$ret = ZSClose($node->conn (0),$cguid);
	like ($ret, qr/^OK.*/,$ret);
	$node->stop ();
	$ret = $node->start (
            ZS_REFORMAT => 0,
	);
   
	$ret = ZSOpen ($node->conn(0), $cname, $$p[0], $size, "ZS_CTNR_RW_MODE", $$p[2], $$p[1]);
	@threads = ();
	($keyoffset, $keylen) = (1000,50);
	for (1 .. $nconn) {
            push(@threads, threads->new (\&worker_read, $_, $cguid, $keyoffset, $keylen));
            #$keyoffset = $keyoffset + 2;
            $keylen = $keylen + 5;
	}
	$_->join for (@threads);

        $ret = ZSClose($node->conn (0),$cguid);
        like ($ret, qr/^OK.*/,$ret);
        $ret = ZSDelete($node->conn (0),$cguid);
        like ($ret, qr/^OK.*/,$ret);
    }

    return;
}

sub test_init {
    $node = Fdftest::Node->new (
        ip    => "127.0.0.1",
        port  => "24422",
        nconn => $nconn,
		prop  => "$Bin/../../conf/stress.prop",
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

