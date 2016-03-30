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

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::MultiNode;
use Fdftest::BasicTest;
use Test::More 'no_plan';


my @nodes;
my $ncntr = 5;
my $nnode = 4;
#my @data = ([50, 64000, 0.125], [100, 128000, 0.125], [150, 512, 0.75]);
my @data = ([64, 16000, 0.05], [74, 32000, 0.05], [84, 64000, 0.05], [94, 128000, 0.05], [104, 48, 1]);

sub worker{
    my ($conn, $cguid, $key_offset, $val_offset, $nops) = @_;
    my $ret;

    $ret = ZSTransactionStart($conn,);
    like($ret, qr/OK.*/, 'ZSTransactionStart');

    foreach my $d(@data){
        $ret = ZSWriteObject(
            $conn,
            cguid         => $cguid,
            key_offset    => $key_offset,
            key_len       => $$d[0],
            data_offset   => $val_offset,
            data_len      => $$d[1],
            nops          => $nops*$$d[2],
            flags         => "ZS_WRITE_MUST_EXIST",
            );
        like($ret, qr//, "ZSWriteObject: update ".$nops*$$d[2]." objects on cguid=$cguid will failed after kill the node.");
    }
}

sub write{
    my ($conn, $cguid, $key_offset, $val_offset, $nops) = @_;

    foreach my $d(@data){
        WriteObjects($conn, $cguid, $key_offset, $$d[0], $val_offset, $$d[1], $nops*$$d[2]);
    }
}

sub read{
    my ($conn, $cguid, $key_offset, $val_offset, $nops) = @_;

    foreach my $d(@data){
        ReadObjects($conn, $cguid, $key_offset, $$d[0], $val_offset, $$d[1], $nops*$$d[2]);
    }
}

sub test_run{
    my ($ret, $cguid, @cguids);
    my (@threads, @upthreads);
    my $size = 0;
    my $key_offset = 1000;
    my $val_offset = 1000;
    my $nops = 10000;
    my $update = 8000;

    for(1 .. $nnode){
        $ret = $nodes[$_-1]->start(ZS_REFORMAT => 1,threads => $ncntr+1,);
        like($ret, qr/OK.*/, "$_"."th Instance Start");
    }

    @threads = ();
    for my $i(1 .. $nnode){
        for my $j(1 .. $ncntr){
            $ret=OpenContainer($nodes[$i-1]->conn(0), "c$i-$j","ZS_CTNR_CREATE",$size,4,"ZS_DURABILITY_HW_CRASH_SAFE","no");
            $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
            push(@cguids, $cguid);

            push(@threads, threads->new(\&write, $nodes[$i-1]->conn($j), $cguid, $key_offset, $val_offset, $nops));
        }
    }
    $_->join for (@threads);

    for my $i(1 .. $nnode){
        @upthreads = ();    
        for my $j(1 .. $ncntr){
            $upthreads[$j-1] = threads->create(\&worker, 
                $nodes[$i-1]->conn($j),
                $cguids[($i-1)*$ncntr+$j-1],
                $key_offset + $j,
                $val_offset + $j - 1,
                $update/$ncntr,
            );
        }

        sleep(4);
        $nodes[$i-1]->kill_ins();
        sleep(2);
    }

    for(1 .. $nnode){
        $ret = $nodes[$_-1]->start(ZS_REFORMAT => 0,);
        like($ret, qr/OK.*/, "$_"."th Instance Restart");
    }

    @threads = ();
    for my $i(1 .. $nnode){
        for my $j(1 .. $ncntr){
            $ret=OpenContainer($nodes[$i-1]->conn(0), "c$i-$j","ZS_CTNR_RW_MODE",$size,4,"ZS_DURABILITY_HW_CRASH_SAFE","no");
            $cguid = $1 if($ret =~ /OK cguid=(\d+)/);

            push(@threads, threads->new(\&read, $nodes[$i-1]->conn($j), $cguid, $key_offset+$j, $val_offset+$j, $update/$ncntr));
        }
    }
    $_->join for (@threads);

    for my $i(1 .. $nnode){
        for my $j(1 .. $ncntr){
            CloseContainer($nodes[$i-1]->conn(0), $cguids[($i-1)*$ncntr+$j-1]);
            DeleteContainer($nodes[$i-1]->conn(0), $cguids[($i-1)*$ncntr+$j-1]);
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
            nconn       => $ncntr + 1,
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
