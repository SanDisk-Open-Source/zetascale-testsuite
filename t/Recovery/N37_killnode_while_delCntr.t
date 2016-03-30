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
use threads::shared;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::BasicTest;
use Fdftest::Node;
use Test::More 'no_plan';

my %hname:shared;
my $node;
my $ncntr = 100;


sub open{
    my ($conn, $cname, $flag, $size) = @_;
    my ($ret, $cguid);

    $ret=OpenContainer($conn, $cname,$flag,$size,4,"ZS_DURABILITY_HW_CRASH_SAFE","no");
    $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
    $hname{$cname} = $cguid;
}

sub worker{
    my $ret;

    for (0 .. $ncntr-1){
        my $cname = "c-$_";
        $ret = ZSDeleteContainer($node->conn($_),cguid=>$hname{$cname});
        if($ret =~ qr/OK.*/){
            like($ret, qr/OK.*/, "ZSDeleteContainer: delete cname=$cname with cguid=$hname{$cname} success.");
        }else {
            like($ret, qr/.*/, "ZSDeleteContainer: delete cname=$cname with cguid=$hname{$cname} will failed after kill the node.");
        }
    }
}

sub test_run {
    my $ret;
    my @threads;
    my $size = 0;
    my $keyoff = 1000;
    my $valoff = 1000;

    $ret = $node->start (
#        gdb_switch   => 1,
        ZS_REFORMAT => 1,
        threads     => $ncntr
    );
    like ($ret, qr/^OK.*/, 'Node start');

    @threads = ();
    for (0 .. $ncntr-1){
        push(@threads, threads->new(\&open, $node->conn($_), "c-$_", "ZS_CTNR_CREATE", $size));
    }
    $_->join for (@threads);

    for (0 .. $ncntr-1){
        CloseContainer($node->conn(0), $hname{"c-$_"});
    }

    @threads = ();
    $threads[0] = threads->create(\&worker, );
    sleep(1);
    $node->kill();
    sleep(10);

    $ret = $node->start (
        ZS_REFORMAT => 0,
    );
    like($ret, qr/OK.*/, "Node Restart");

    $ret = ZSGetContainers($node->conn(0));
    my $cnum = $1 if($ret =~ /OK n_cguids=(\d+)/);
    like($ret, qr/OK.*/, "ZSGetContainers: n_cguid=$cnum");

    @threads = ();
    for ($ncntr-$cnum .. $ncntr-1){
        push(@threads, threads->new(\&open, $node->conn($_), "c-$_", "ZS_CTNR_RW_MODE", $size));
    }
    $_->join for (@threads);

    return;
}

sub test_init {
    $node = Fdftest::Node->new (
        ip    => "127.0.0.1",
        port  => "24422",
        nconn => $ncntr,
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

