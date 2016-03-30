# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file:
# author: Jing Xu(Lee)
# email: leexu@hengtiansoft.com
# date: Sep 9, 2014
# description:


#!/usr/bin/perl

use strict;
use warnings;
use threads;
use threads::shared;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::MultiNode;
use Fdftest::BasicTest;
use Test::More tests => 672;

my @cguids:shared;
my @nodes; 
my $ncntr = 5;
my $nnode = 4;
#my @data = ([50, 64000, 0.125], [100, 128000, 0.125], [150, 512, 0.75]);
my @data = ([64, 16000, 0.05], [74, 32000, 0.05], [84, 64000, 0.05], [94, 128000, 0.05], [104, 48, 1]);


sub open{
    my ($conn, $cname, $flag, $size) = @_;
    my ($ret, $cguid);

    $ret=OpenContainer($conn, $cname,$flag,$size,4,"ZS_DURABILITY_HW_CRASH_SAFE","no");
    $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
    push(@cguids, $cguid);
}

sub write{
    my ($conn, $cguid, $keyoff, $valoff, $nops) = @_;

    foreach my $d(@data){
        WriteReadObjects($conn, $cguid, $keyoff, $$d[0], $valoff, $$d[1], $nops*$$d[2]);
    }
}

sub read{
    my ($conn, $cguid, $keyoff, $valoff, $nops) = @_;

    foreach my $d(@data){
        ReadObjects($conn, $cguid, $keyoff, $$d[0], $valoff, $$d[1], $nops*$$d[2]);
    }
}

sub worker{
    my ($conn, $cguid, $keyoff, $valoff, $nops) = @_;
    my $ret;

    $ret = ZSTransactionStart($conn,);
    like($ret, qr/OK.*/, 'ZSTransactionStart');

    foreach my $d(@data){
        WriteObjects($conn, $cguid, $keyoff, $$d[0], $valoff, $$d[1], $nops*$$d[2], "ZS_WRITE_MUST_EXIST");
    }

    $ret = ZSTransactionRollback($conn,);
    like ($ret, qr/SERVER_ERROR ZS_UNSUPPORTED_REQUEST.*/, 'ZSTransactionRollback: SERVER_ERROR ZS_UNSUPPORTED_REQUEST');
}

sub test_run {
    my $ret;
    my @threads;
    my $size = 0;
    my $keyoff = 1000;
    my $valoff = 1000;
    my $nops = 1000;
    my $update = 800;

    for(1 .. $nnode){
        $ret = $nodes[$_-1]->start(ZS_REFORMAT => 1,threads => $ncntr,);
        like($ret, qr/OK.*/, "$_"."th Instance Start");
    }

    @threads = ();
    for my $i(0 .. $nnode-1){
        for my $j(0 .. $ncntr-1){
            push(@threads, threads->new(\&open, $nodes[$i]->conn($j), "c$i-$j", "ZS_CTNR_CREATE", $size));
        }
    }
    $_->join for (@threads);

    @threads = ();
    for my $i(0 .. $nnode-1){
        for my $j(0 .. $ncntr-1){
            push(@threads, threads->new(\&write, $nodes[$i]->conn($j), $cguids[$i*$ncntr+$j], $keyoff, $valoff, $nops));
        }
    }
    $_->join for (@threads);

    @threads = ();
    for my $i(0 .. $nnode-1){
        for my $j(0 .. $ncntr-1){
            push(@threads, threads->new(\&worker,
                $nodes[$i]->conn($j),
                $cguids[$i*$ncntr+$j],
                $keyoff + $j,
                $valoff + $j - 1,
                $update/$ncntr,
            ));
        }
    }
    $_->join for (@threads);

    @threads = ();
    for my $i(0 .. $nnode-1){
        for my $j(0 .. $ncntr-1){
            push(@threads, threads->new(\&read, $nodes[$i]->conn($j), $cguids[$i*$ncntr+$j], $keyoff+$j, $valoff+$j-1, $update/$ncntr));
        }
    }
    $_->join for (@threads);

    for my $i(0 .. $nnode-1){
        for my $j(0 .. $ncntr-1){
            FlushContainer($nodes[$i]->conn(0), $cguids[$i*$ncntr+$j]);
            CloseContainer($nodes[$i]->conn(0), $cguids[$i*$ncntr+$j]);
        }
    }

    for(1 .. $nnode){
        $ret = $nodes[$_-1]->stop();
        like($ret, qr/OK.*/, "$_"."th Instance Stop");
        $ret = $nodes[$_-1]->start(ZS_REFORMAT => 0,);
        like($ret, qr/OK.*/, "$_"."th Instance Restart");
    }

    @threads = ();
    for my $i(0 .. $nnode-1){
        for my $j(0 .. $ncntr-1){
            push(@threads, threads->new(\&open, $nodes[$i]->conn($j), "c$i-$j", "ZS_CTNR_RW_MODE", $size));
        }
    }
    $_->join for (@threads);

    @threads = ();
    for my $i(0 .. $nnode-1){
        for my $j(0 .. $ncntr-1){
            push(@threads, threads->new(\&read, $nodes[$i]->conn($j), $cguids[$i*$ncntr+$j], $keyoff+$j, $valoff+$j, $update/$ncntr));
        }
    }
    $_->join for (@threads);

    for my $i(0 .. $nnode-1){
        for my $j(0 .. $ncntr-1){
            CloseContainer($nodes[$i]->conn(0), $cguids[$i*$ncntr+$j]);
            DeleteContainer($nodes[$i]->conn(0), $cguids[$i*$ncntr+$j]);
        }
    }

    return;
}

sub test_init {
    for(1 .. $nnode){
        my $port = 24421 + $_;
        my $node = Fdftest::MultiNode->new(
                     ip          => "127.0.0.1", 
                     port        => "$port",
                     nconn       => $ncntr,
                     stats_log   => "/tmp/$port/zsstats.log",
                     zs_log      => "/tmp/$port/zs.log",
                     unix_socket => "/tmp/$port/sock",
                   );
        push(@nodes, $node);       
    }
}

sub test_clean {
    for(@nodes){
        $_->stop();
        $_->set_ZS_prop(ZS_REFORMAT  => 1);
    }
    return;
}

#
# main
#
{
    # export multi instance flag
    $ENV{ZS_TEST_FRAMEWORK_SHARED} = 1;

    test_init();
    test_run();
    test_clean();
}


# clean ENV
END {
    for(@nodes){
        $_->clean();
    }
}
