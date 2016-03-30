# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
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
use Test::More tests => 710;


my $node;
my $nthread = 5;
my $ncntr = 5;
my @data = ([50, "aa", "bbb", 160, 0.25], [100, "cdefm", "z", 320, 0.25], [150, "hhg", "os", 640, 0.25], [200, "w", "n", 1280, 0.25]);#counter,pg,osd,vallen,nops

sub worker{
    my ($connid, $cguid0, $cguids_ref, $valoff, $nops, $mode) = @_;
    my $ret;

    $ret = ZSTransactionStart(
        $node->conn($connid),
    );
    like($ret, qr/OK.*/, 'ZSTransactionStart');

    foreach my $d(@data){
        $ret = WriteLogObjects($node->conn($connid), $cguid0, $$d[0], $$d[1], $$d[2], $valoff, $$d[3], 1);
        like($ret, qr/^OK.*/, $ret);
    }

    my $i = 0;
    for (@$cguids_ref){
        foreach my $d(@data){
            $ret = WriteLogObjects($node->conn($connid), $_, $$d[0]+$nops*$$d[4]*$connid, $$d[1], $$d[2], $valoff+$nops*$$d[4]*$connid-$i, $$d[3], $nops*$$d[4], "ZS_WRITE_MUST_EXIST");
            like($ret, qr/^OK.*/, $ret);
        }
        $i = $i + 1;
    }

    $ret = ZSTransactionRollback(
        $node->conn($connid),
    );
    if ($mode =~ /.*mode=1.*/){
        like ($ret, qr/SERVER_ERROR ZS_UNSUPPORTED_REQUEST.*/, 'Btree type container not support ZSTransactionRollback '.$ret);
    }elsif ($mode =~ /.*mode=2.*/){
        like($ret, qr/SERVER_ERROR ZS_TRANS_ABORTED/, "ZSTransactionRollback: Update_ops > 100K, Rollback will fail, expect return $ret");
    }
}

sub read{
    my ($conn, $j, $cguid, $valoff, $nops) = @_;
    my $ret;
  
    foreach my $d(@data){
        $ret = ReadLogObjects($conn, $cguid, $$d[0]+$nops*$$d[4]*$j, $$d[1], $$d[2], $valoff+$nops*$$d[4]*$j, $$d[3], $nops*$$d[4]);
        like($ret, qr/^OK.*/, $ret);
    }
}

sub test_run {
    my ($ret, $cguid0, $cguid);
    my (@cguids, @threads);
    my $size = 0;
    my $valoff = 100;
    my $nobject = 130000;
    my $update = 120000;
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

        my $mode = ZSTransactionGetMode(
            $node->conn(0),
        );
        chomp($mode);
        like($mode, qr/OK.*/, "ZSTransactionGetMode: $mode");

        my $up_per_th = $update/$nthread;
        @threads = ();
        for(0 .. $nthread - 1){
            push(@threads, threads->new(\&worker, 
                $_, 
                $cguid0, 
                \@cguids,
                $valoff-1, 
                $up_per_th,
                $mode,
            ));
        }
        $_->join for (@threads);

        @threads = ();
        for my $i(0 .. $ncntr - 1){
            for my $j(0 .. $nthread - 1){
                if ($mode =~ /.*mode=1.*/){
                    push(@threads, threads->new(\&read, $node->conn($i*$nthread+$j),$j,$cguids[$i],$valoff-1-$i,$up_per_th));
                }elsif ($mode =~ /.*mode=2.*/){
                    push(@threads, threads->new(\&read, $node->conn($i*$nthread+$j),$j,$cguids[$i],$valoff,$up_per_th));
                }
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

        my $nops_per_th = $nobject/$nthread; 
        @threads = ();
        for my $i(0 .. $ncntr - 1){
            for my $j(0 .. $nthread - 1){
                push(@threads, threads->new(\&read, $node->conn($i*$nthread+$j),$j,$cguids[$i],$valoff,$nops_per_th));
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


