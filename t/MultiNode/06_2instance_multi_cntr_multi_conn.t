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
use Fdftest::MultiNode;
use Test::More tests => 10202;

#tests =( 6*($nconn) + 2*$ncntr) * ($loop) + $ncntr*2 + 1
my @nodes;
my $nnode = 2;
my $nconn = 100;
my $ncntr = 50;
my $loop  = 5;
#my @data = ([50, 64000, 1], [100, 128000, 1], [150, 512, 6]);
my @data = ([64, 16000, 1], [74, 32000, 1], [84, 64000, 1], [94, 128000, 1], [104, 48,20]);


sub worker {
    #my ($conn, $cguid, $keyoffset, $keylen, $datalen, $nops) = @_;
    my ($conn, $cguid, $keyoffset, $nops) = @_;
    my ($ret, $msg);
    

    foreach my $d(@data){
    	$ret = ZSSetGet ($conn, $cguid, $keyoffset, $$d[0], $$d[1], $nops*$$d[2]);
    like ($ret, qr/^OK.*/, $ret);
    }
    $ret = ZSFlushRandom ($conn, $cguid, $keyoffset);
    like ($ret, qr/^OK.*/, $ret);
    $ret = ZSEnumerate ($conn, $cguid);
    like ($ret, qr/^OK.*/, $ret);
    $ret = ZSGetProps ($conn, $cguid);
    $msg = substr($ret, 0, index($ret, "ZSGet"));
    like ($ret, qr/^OK.*/, $msg . "ZSGetContainerProps");
    $ret = ZSGetConts ($conn, $ncntr);
    $msg = substr($ret, 0, index($ret, "ZSGet"));
    like ($ret, qr/^OK.*/, $msg . "ZSGetContainers");
}

sub reopen {
    my ($conn, $cguid, $cname, $size) = @_;
    my $ret;
    $ret = ZSClose ($conn, $cguid);
    like ($ret, qr/^OK.*/, $ret);
    $ret = ZSOpen ($conn, $cname, 3, $size, "ZS_CTNR_RW_MODE", "no", "ZS_DURABILITY_HW_CRASH_SAFE");
    like ($ret, qr/^OK.*/, $ret);
}

sub test_run {
    my ($ret, @cguids, %chash, @threads);
    my $size = 0;

    for(1 .. $nnode){
        $ret = $nodes[$_-1]->start(
            gdb_switch    => 1,
            ZS_REFORMAT   => 1,
            threads       => $nconn,);
        like($ret, qr/OK.*/, "$_"."th Instance Start");
    }

    # Create containers with $nconn connections
    for my $i(0 .. $nnode-1){
        for my $j(0 .. $ncntr-1){
            my $cname = "c$i-$j";
            $ret = ZSOpen ($nodes[$i]->conn (0), $cname, 3, $size, "ZS_CTNR_CREATE", "no", "ZS_DURABILITY_HW_CRASH_SAFE");
            like ($ret, qr/^OK.*/, $ret);

            if ($ret =~ /^OK cguid=(\d+)/) {
                push(@cguids, $1);
                $chash{$cname} = $1;
            }
            else {
                return;
            }
        }
    }

    for (1 .. $loop) {
        my $keyoffset = int(rand(5000)) + 1;
        my ($keylen, $datalen, $maxops, $nops);

        @threads = ();
        for my $i(0 .. $nnode-1){
            for (0 .. $nconn-1) {
                $keyoffset = $keyoffset + $_;
                $keylen    = int(rand(240)) + 1;
                $datalen   = int(rand(2048000)) + 1;
                $maxops    = int((5000) / (($datalen) / 1000000));
                $nops      = int(rand($maxops / $nconn));
	        push(@threads, threads->new (\&worker, $nodes[$i]->conn ($_), $cguids[ $i*$ncntr+rand($ncntr) ], $keyoffset, $nops));
            }
        }
        $_->join for (@threads);

        @threads = ();
        for my $i(0 .. $nnode-1){
            for my $j(0 .. $ncntr-1) {
                my $cname = "c$i-$j";
                push(@threads, threads->new (\&reopen, $nodes[$i]->conn ($j), $chash{$cname}, $cname, $size));
            }
        }
        $_->join for (@threads);
    }

    for my $i(0 .. $nnode-1){
        for my $j(0 .. $ncntr-1){
            $ret = ZSClose ($nodes[$i]->conn (0), $cguids[$i*$ncntr+$j]);
            like ($ret, qr/^OK.*/, $ret);
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
                     nconn       => $nconn,
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
