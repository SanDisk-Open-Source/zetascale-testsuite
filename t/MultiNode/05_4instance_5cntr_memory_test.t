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
use Test::More tests => 264;

my @cguids:shared;
my $max:shared;
my $now:shared;
my @nodes; 
my $ncntr = 5;
my $nnode = 4;
#my @data = ([50, 64000, 1000], [100, 128000, 1000], [150, 512, 6000]);
my @data = ([64, 16000, 500], [74, 32000, 500], [84, 64000, 500], [94, 128000, 500], [104, 48,10000]);


sub open{
    my ($conn, $cname, $flag, $size) = @_;
    my ($ret, $cguid);

    $ret=OpenContainer($conn, $cname,$flag,$size,4,"ZS_DURABILITY_HW_CRASH_SAFE","no");
    $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
    push(@cguids, $cguid);
}

sub worker{
    my ($conn, $cguid, $keyoff, $valoff) = @_;

    foreach my $d(@data){
        WriteReadObjects($conn, $cguid, $keyoff, $$d[0], $valoff, $$d[1], $$d[2]);
        $now = `free -g|awk '/Mem/{print \$3}'`;
        if($now > $max){
            $max = $now;
        }
    }
    print "Max used memory: $max \n";
}

sub test_run {
    my $ret;
    my @threads;
    my $size = 0;
    my $keyoff = 1000;
    my $valoff = 1000;
    $max = 0;
    $now = 0;

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
            push(@threads, threads->new(\&worker, $nodes[$i]->conn($j), $cguids[$i*$ncntr+$j], $keyoff, $valoff));
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
