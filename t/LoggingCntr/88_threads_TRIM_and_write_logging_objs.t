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
use Test::More tests => 105;


my $node;
my $nthread = 1;
my $ncntr = 5;
my @data = ([50, "aa", "bbb", 2000, 0.25], [100, "cdefm", "z", 2500, 0.25], [150, "hhg", "os", 3000, 0.25], [200, "w", "n", 3500, 0.25]);#counter,pg,osd,vallen,nops

sub trim{
    my ($connid, $cguids_ref, $valoff, $nops) = @_;
    my $ret;

    my $i = 0;
    for (@$cguids_ref){
        foreach my $d(@data){
            $ret = WriteLogObjects($node->conn($connid), $_, $$d[0]+$nops*$$d[4]*$connid, $$d[1], $$d[2], $valoff+$nops*$$d[4]*$connid-$i, $$d[3], $nops*$$d[4], "ZS_WRITE_TRIM");
            like($ret, qr/.*/, $ret);
        }
        $i = $i + 1;
    }
}

sub write{
    my ($connid, $cguids_ref, $valoff, $nops) = @_;
    my $ret;

    my $i = 0;
    for (@$cguids_ref){
        foreach my $d(@data){
            $ret = WriteLogObjects($node->conn($connid), $_, $$d[0]+$nops*$$d[4]*$connid, $$d[1], $$d[2], $valoff+$nops*$$d[4]*$connid-$i, $$d[3], $nops*$$d[4], "ZS_WRITE_MUST_NOT_EXIST");
            like($ret, qr/.*/, $ret);
        }
        $i = $i + 1;
    }
}

sub read{
    my ($conn, $j, $cguid, $valoff, $nops) = @_;
    my $ret;
  
    foreach my $d(@data){
        $ret = ReadLogObjects($conn, $cguid, $$d[0]+$nops*$$d[4]*$j, $$d[1], $$d[2], $valoff+$nops*$$d[4]*$j, $$d[3], $nops*$$d[4]);
        like($ret, qr/^Error.*/, $ret);
    }
}

sub test_run {
    my ($ret, $cguid);
    my (@cguids, @threads, @threads1);
    my $size = 0;
    my $valoff = 100;
    my $nops = 5000;
    my @ctype = ("BTREE", "LOGGING");
    my @prop = ([3, "no", "ZS_DURABILITY_HW_CRASH_SAFE"],);

    $ret = $node->start(
        ZS_REFORMAT  => 1,
        threads      => $nthread*$ncntr+1,
    );
    like($ret, qr/OK.*/, 'Node start');

    foreach my $p(@prop){
        @cguids = ();
        for(0 .. $ncntr - 1){
            $ret = OpenContainer($node->conn(0), "LC-$_", $$p[0], $size, "ZS_CTNR_CREATE", $$p[1], $$p[2], $ctype[1]);
            $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
            like($ret, qr/^OK.*/, $ret);
            push(@cguids, $cguid);
        }

        my $up_per_th = $nops/$ncntr/$nthread;
        @threads = ();
        for(0 .. $nthread - 1){
            $threads[$_] = threads->create(\&write,
                $_,
                \@cguids,
                $valoff-1,
                $up_per_th,
            );
        }

        @threads1 = ();
        for(0 .. $nthread - 1){
            $threads1[$_] = threads->create(\&trim,
                $_,
                \@cguids,
                $valoff-1,
                $up_per_th,
            );
        }
        $_->join for (@threads);
        $_->join for (@threads1);
        
        @threads = ();
        for my $i(0 .. $ncntr - 1){
            for my $j(0 .. $nthread - 1){
                push(@threads, threads->new(\&read, $node->conn($i*$nthread+$j),$j,$cguids[$i],$valoff-1-$i,$up_per_th));
            }
        }
        $_->join for (@threads);

        for(1 .. $ncntr - 1){
            $ret = CloseContainer($node->conn(0), $cguids[$_]);
            like($ret, qr/^OK.*/, $ret);
        }

        $ret = $node->stop();
        like($ret, qr/OK.*/, 'Node stop');
        $ret = $node->start(ZS_REFORMAT  => 0,);
        like($ret, qr/OK.*/, 'Node restart');

        for(0 .. $ncntr -1){
            $ret = OpenContainer($node->conn(0), "LC-$_", $$p[0], $size, "ZS_CTNR_RW_MODE", $$p[1], $$p[2], $ctype[1]);
            like($ret, qr/^OK.*/, $ret);
        }

        @threads = ();
        for my $i(0 .. $ncntr - 1){
            for my $j(0 .. $nthread - 1){
                push(@threads, threads->new(\&read, $node->conn($i*$nthread+$j),$j,$cguids[$i],$valoff-1-$i,$up_per_th));
            }
        }
        $_->join for (@threads);

        for(1 .. $ncntr - 1){
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
        nconn  => $nthread*$ncntr+1,
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


