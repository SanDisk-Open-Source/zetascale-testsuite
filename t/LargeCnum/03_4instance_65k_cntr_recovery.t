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
use Test::More tests => 3900012;

my @cguids:shared;
my @nodes; 
my $ncntr = 64000;
my $nthread = 100;
my $nnode = 2;
#my @data = ([50, 64000, 1], [100, 128000, 1], [150, 512, 6]);
my @data = ([64, 16000, 1], [74, 32000, 1], [84, 64000, 1], [94, 128000, 1], [104, 48,20]);


sub open{
    my ($i, $j, $flag, $size) = @_;
    my ($ret, $cguid);
    my $start = $i*$ncntr+$j*$ncntr/$nthread;

    for($start..$start+$ncntr/$nthread-1){
        $ret=OpenContainer($nodes[$i]->conn($j), "c$i-$_",$flag,$size,4,"ZS_DURABILITY_HW_CRASH_SAFE","no");
        $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
        $cguids[$_] = $cguid;
    }
}

sub worker{
    my ($i, $j, $keyoff, $valoff) = @_;
    my $start = $i*$ncntr+$j*$ncntr/$nthread;

    for($start..$start+$ncntr/$nthread-1){
        foreach my $d(@data){
            WriteReadObjects($nodes[$i]->conn($j), $cguids[$_], $keyoff, $$d[0], $valoff, $$d[1], $$d[2]);
        }
    }
}

sub read{
    my ($i, $j, $keyoff, $valoff) = @_;
    my $start = $i*$ncntr+$j*$ncntr/$nthread;

    for($start..$start+$ncntr/$nthread-1){
        foreach my $d(@data){
            ReadObjects($nodes[$i]->conn($j), $cguids[$_], $keyoff, $$d[0], $valoff, $$d[1], $$d[2]);
        }
    }
}

sub test_run {
    my $ret;
    my @threads;
    my $size = 0;
    my $keyoff = 1000;
    my $valoff = 1000;

    for(1 .. $nnode){
        $ret = $nodes[$_-1]->set_ZS_prop (ZS_MAX_NUM_CONTAINERS => 64000);
        like ($ret, qr//, "$_"."th Instance set ZS_MAX_NUM_CONTAINERS to 64K");

        $ret = $nodes[$_-1]->start(ZS_REFORMAT => 1,threads => $nthread,);
        like($ret, qr/OK.*/, "$_"."th Instance Start");
    }

    @threads = ();
    for my $i(0 .. $nnode-1){
        for my $j(0 .. $nthread-1){
            push(@threads, threads->new(\&open, $i, $j, "ZS_CTNR_CREATE", $size));
        }
    }
    $_->join for (@threads);

    @threads = ();
    for my $i(0 .. $nnode-1){
        for my $j(0 .. $nthread-1){
            push(@threads, threads->new(\&worker, $i, $j, $keyoff, $valoff));
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
        for my $j(0 .. $nthread-1){
            push(@threads, threads->new(\&open, $nodes[$i]->conn($j), "c$i-$j", "ZS_CTNR_RW_MODE", $size));
        }
    }
    $_->join for (@threads);

    @threads = ();
    for my $i(0 .. $nnode-1){
        for my $j(0 .. $nthread-1){
            push(@threads, threads->new(\&read, $nodes[$i]->conn($j), $cguids[$i*$ncntr+$j], $keyoff, $valoff));
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
                     nconn       => $nthread,
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
        $_->set_ZS_prop (ZS_REFORMAT => 1, ZS_MAX_NUM_CONTAINERS => 6000);
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