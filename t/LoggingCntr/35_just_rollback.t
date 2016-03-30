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

#clean file:
# author: Jing Xu(Lee)
# email: leexu@hengtiansoft.com
# date: Jul 8, 2014
# description:
 
#!/usr/bin/perl

use strict;
use warnings;
use threads;
use Switch;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::UnifiedAPI;
use Test::More tests => 1149;


my $node;
my $nthread = 10;
my $ncntr = 5;
my @data = ([50, "aa", "bbb", 160, 0.25], [100, "cdefm", "z", 320, 0.25], [150, "hhg", "os", 640, 0.25], [200, "w", "n", 1280, 0.25]);#counter,pg,osd,vallen,nops

sub worker{
    my ($conn, $j, $cguid0, $cguid, $valoff, $nops) = @_;
    my $ret;

    my $mode = ZSTransactionGetMode(
        $conn,
    );
    chomp($mode);
    like($mode, qr/OK.*/, "ZSTransactionGetMode: $mode");

    foreach my $d(@data){
        $ret = WriteLogObjects($conn, $cguid0, $$d[0], $$d[1], $$d[2], $valoff, $$d[3], 1);
        like($ret, qr/^OK.*/, $ret);
    }

    foreach my $d(@data){
        $ret = WriteLogObjects($conn, $cguid, $$d[0]+$nops*$$d[4]*$j, $$d[1], $$d[2], $valoff+$nops*$$d[4]*$j, $$d[3], $nops*$$d[4], "ZS_WRITE_MUST_EXIST");
        like($ret, qr/^OK.*/, $ret);
    }

    $ret = ZSTransactionRollback(
        $conn,
    );
    if ($mode =~ /.*mode=1.*/){
        like($ret, qr/SERVER_ERROR ZS_UNSUPPORTED_REQUEST.*/, 'ZSTransactionRollback, expect return SERVER_ERROR ZS_UNSUPPORTED_REQUEST');
    }elsif ($mode =~ /.*mode=2.*/){
        like($ret, qr/SERVER_ERROR ZS_FAILURE_NO_TRANS.*/, 'ZSTransactionRollback, expect return SERVER_ERROR ZS_FAILURE_NO_TRANS');
    }
}

sub read{
    my ($conn, $j, $cguid, $valoff, $off, $nops) = @_;
    my $ret;
  
    foreach my $d(@data){
        $ret = ReadLogObjects($conn, $cguid, $$d[0]+$nops*$$d[4]*$j+$off*$$d[4], $$d[1], $$d[2], $valoff+$nops*$$d[4]*$j+$off*$$d[4], $$d[3], $nops*$$d[4]);
        like($ret, qr/^OK.*/, $ret);
    }
}

sub test_run {
    my ($ret, $cguid0, $cguid);
    my (@cguids, @threads);
    my $size = 0;
    my $valoff = 100;
    my $nobject = 5000;
    my $update = 4000;
    my @ctype = ("BTREE", "LOGGING");
    my @prop = ([3, "no", "ZS_DURABILITY_HW_CRASH_SAFE"],);

    $ret = $node->start(
        ZS_REFORMAT  => 1,
        threads      => $nthread*$ncntr,
    );
    like($ret, qr/OK.*/, 'Node start');

    foreach my $p(@prop){
        $ret = OpenContainer($node->conn(0), "BC-0", $$p[0], $size, "ZS_CTNR_CREATE", $$p[1], $$p[2], $ctype[0]);
        $cguid0 = $1 if($ret =~ /OK cguid=(\d+)/);
        like($ret, qr/^OK.*/, $ret);

        @cguids = ();
        for(0 .. $ncntr - 1){
            $ret = OpenContainer($node->conn(0), "LC-$_", $$p[0], $size, "ZS_CTNR_CREATE", $$p[1], $$p[2], $ctype[1]);
            $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
            like($ret, qr/^OK.*/, $ret);
            push(@cguids, $cguid);

            foreach my $d(@data){
                $ret = WriteLogObjects($node->conn(0), $cguid, $$d[0], $$d[1], $$d[2], $valoff, $$d[3], $nobject*$$d[4], "ZS_WRITE_MUST_NOT_EXIST");
                like($ret, qr/^OK.*/, $ret);
            }
        }

        my $up_per_th = $update/$ncntr/$nthread;
        @threads = ();
        for my $i(0 .. $ncntr - 1){
            for my $j(0 .. $nthread - 1){
                push(@threads, threads->new(\&worker,
                    $node->conn($i*$nthread+$j),
                    $j,
                    $cguid0,
                    $cguids[$i],
                    $valoff-1-$i,
                    $up_per_th,
                ));
            }
        }
        $_->join for (@threads);

        @threads = ();
        for my $i(0 .. $ncntr - 1){
            for my $j(0 .. $nthread - 1){
                push(@threads, threads->new(\&read, $node->conn($i*$nthread+$j),$j,$cguids[$i],$valoff-1-$i,0,$up_per_th));
            }
        }
        $_->join for (@threads);

        for(0 .. $ncntr - 1){
            $ret = CloseContainer($node->conn(0), $cguids[$_]);
            like($ret, qr/^OK.*/, $ret);
        }

        $ret = $node->stop();
        like($ret, qr/OK.*/, 'Node stop');
        $ret = $node->start(ZS_REFORMAT  => 0,);
        like($ret, qr/OK.*/, 'Node restart');

        for(0 .. $ncntr -1){
            $ret = OpenContainer($node->conn(0), "LC-$_", $$p[0], $size, "ZS_CTNR_RW_MODE", $$p[1], $$p[2], $ctype[1]);
            $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
            like($ret, qr/^OK.*/, $ret);
        }

        @threads = ();
        for my $i(0 .. $ncntr - 1){
            for my $j(0 .. $nthread - 1){
                push(@threads, threads->new(\&read, $node->conn($i*$nthread+$j),$j,$cguids[$i],$valoff-1-$i,0,$up_per_th));
            }
        }
        $_->join for (@threads);

        my $other_per_th = ($nobject-$update/$ncntr)/$nthread;
        @threads = ();
        for my $i(0 .. $ncntr - 1){
            for my $j(0 .. $nthread - 1){
                push(@threads, threads->new(\&read, $node->conn($i*$nthread+$j),$j,$cguids[$i],$valoff,$update/$ncntr,$other_per_th));
            }
        }
        $_->join for (@threads);

        for(0 .. $ncntr - 1){
            $ret = CloseContainer($node->conn(0), $cguids[$_]);
            like($ret, qr/^OK.*/, $ret);
            $ret = DeleteContainer($node->conn(0), $cguids[$_]);
            like($ret, qr/^OK.*/, $ret);
        }
    }

    return;
}

sub test_init {
    $node = Fdftest::Node->new(
        ip     => "127.0.0.1",
        port   => "24422",
        nconn  => $nthread*$ncntr,
    );
}

sub test_clean {
    $node->stop();
    $node->set_ZS_prop(ZS_REFORMAT  => 1);

    return;
}

#
# main
#
{
    test_init();

    test_run();

    test_clean();
}


# clean ENV
END {
    $node->clean();
}


