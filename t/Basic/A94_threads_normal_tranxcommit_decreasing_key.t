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
use Fdftest::Node;
use Fdftest::BasicTest;
use Test::More tests => 958;

my $node; 
my $ncntr = 5;
my $nthread = 5; #thread num per cntr
#my @data = ([50, 64000, 0.125], [100, 128000, 0.125], [150, 512, 0.75]);
my @data = ([64, 16000, 1/24], [74, 32000, 1/24], [84, 64000, 1/24], [94, 128000, 1/24], [104, 48, 5/6]);


sub write{
    my ($conn, $cguid, $keyoff, $nops) = @_;
    my $ret;

    foreach my $d(@data){
        $ret = ZSWriteObject(
            $conn,
            cguid           =>$cguid,
            key             =>$keyoff,
            key_len         =>$$d[0],
            data_offset     =>$keyoff,
            data_len        =>$$d[1],
            nops            =>$nops*$$d[2],
            flags           =>"ZS_WRITE_MUST_NOT_EXIST",
            );
        like ($ret, qr/^OK.*/, "ZSWriteObject cguid=$cguid key=$keyoff,keylen=$$d[0],datalen=$$d[1],nops=".$nops*$$d[2]);
    }
}

sub read{
    my ($conn, $cguid, $keyoff, $valoff, $nops) = @_;
    my $ret;

    foreach my $d(@data){
        $ret = ZSReadObject(
            $conn,
            cguid           =>$cguid,
            key             =>$keyoff,
            key_len         =>$$d[0],
            data_offset     =>$valoff,
            data_len        =>$$d[1],
            nops            =>$nops*$$d[2],
            check           =>"yes",
            );
        like ($ret, qr/^OK.*/, "ZSReadObject cguid=$cguid key=$keyoff,keylen=$$d[0],datalen=$$d[1],nops=".$nops*$$d[2]);
    }
}

sub read_after_del{
    my ($conn, $cguid, $keyoff, $valoff, $nops) = @_;
    my $ret;

    foreach my $d(@data){
        $ret = ZSReadObject(
            $conn,
            cguid           =>$cguid,
            key             =>$keyoff,
            key_len         =>$$d[0],
            data_offset     =>$valoff,
            data_len        =>$$d[1],
            nops            =>$nops*$$d[2],
            check           =>"yes",
            );
        like ($ret, qr/^SERVER_ERROR ZS_OBJECT_UNKNOWN.*/, "ZSReadObject ZS_OBJECT_UNKNOWN cguid=$cguid key=$keyoff,keylen=$$d[0],datalen=$$d[1],nops=".$nops*$$d[2]);
    }
}

sub del{
    my ($conn, $cguid, $keyoff, $nops) = @_;
    my $ret;

    foreach my $d(@data){
        $ret = ZSDeleteObject(
            $conn,
            cguid           =>$cguid,
            key             =>$keyoff,
            key_len         =>$$d[0],
            nops            =>$nops*$$d[2],
            );
        like($ret,qr/^OK.*/,"ZSDeleteObject cguid=$cguid key=$keyoff,keylen=$$d[0],nops=".$nops*$$d[2]);
    }
}

sub worker{
    my ($conn, $cguid, $keyoff, $valoff, $nops) = @_;
    my $ret;

    $ret = ZSTransactionStart($conn,);
    like($ret, qr/OK.*/, 'ZSTransactionStart');

    foreach my $d(@data){
        $ret = ZSWriteObject(
            $conn,
            cguid           =>$cguid,
            key             =>$keyoff,
            key_len         =>$$d[0],
            data_offset     =>$valoff,
            data_len        =>$$d[1],
            nops            =>$nops*$$d[2],
            flags           =>"ZS_WRITE_MUST_EXIST",
            );
        like ($ret, qr/^OK.*/, "ZSWriteObject cguid=$cguid key=$keyoff,keylen=$$d[0],datalen=$$d[1],nops=".$nops*$$d[2]);
    }

    $ret = ZSTransactionCommit($conn, );
    like($ret, qr/OK.*/, 'ZSTransactionCommit');
}

sub test_run {
    my ($ret, $cguid);
    my (@threads, @cguids);
    my $size = 0;
    my $keyoff = 900000;
    my $nops = -1200;
    my $update = -960;

    $ret = $node->start(ZS_REFORMAT => 1,threads => $ncntr*$nthread,);
    like($ret, qr/OK.*/, "Node Start");

    for (0 .. $ncntr-1){
        $ret=OpenContainer($node->conn(0), "c$_","ZS_CTNR_CREATE",$size,4,"ZS_DURABILITY_HW_CRASH_SAFE","no");
        $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
        push(@cguids, $cguid);
    }

    @threads = ();
    $keyoff = 900000;
    for my $i(0 .. $ncntr-1){
        for my $j(0 .. $nthread-1){
            push(@threads, threads->new(\&write, $node->conn($i*$nthread+$j), $cguids[$i], $keyoff, $nops));
            $keyoff = $keyoff+$nops+1;
        }
    }
    $_->join for (@threads);

    @threads = ();
    $keyoff = 900000;
    for my $i(0 .. $ncntr-1){
        for my $j(0 .. $nthread-1){
            push(@threads, threads->new(\&read, $node->conn($i*$nthread+$j), $cguids[$i], $keyoff, $keyoff, $nops));
            $keyoff = $keyoff+$nops+1;
        }
    }
    $_->join for (@threads);

    @threads = ();
    $keyoff = 900000;
    for my $i(0 .. $ncntr-1){
        for my $j(0 .. $nthread-1){
            push(@threads, threads->new(\&worker, 
                $node->conn($i*$nthread+$j), 
                $cguids[$i], 
                $keyoff, 
                $keyoff - 1,
                $update,
            ));
            $keyoff = $keyoff+$nops+1;
        }
    }
    $_->join for (@threads);

    @threads = ();
    $keyoff = 900000;
    for my $i(0 .. $ncntr-1){
        for my $j(0 .. $nthread-1){
            push(@threads, threads->new(\&read, $node->conn($i*$nthread+$j), $cguids[$i], $keyoff, $keyoff-1, $update));
            $keyoff = $keyoff+$nops+1;           
        }
    }
    $_->join for (@threads);

    for (0 .. $ncntr-1){
        FlushContainer($node->conn(0), $cguids[$_]);
        CloseContainer($node->conn(0), $cguids[$_]);
    }

    $ret = $node->stop();
    like($ret, qr/OK.*/, "Node Stop");
    $ret = $node->start(ZS_REFORMAT => 0,);
    like($ret, qr/OK.*/, "Node Restart");

    for (0 .. $ncntr-1){
        $ret=OpenContainer($node->conn(0), "c$_","ZS_CTNR_RW_MODE",$size,4,"ZS_DURABILITY_HW_CRASH_SAFE","no");
    }

    @threads = ();
    $keyoff = 900000;
    for my $i(0 .. $ncntr-1){
        for my $j(0 .. $nthread-1){
            push(@threads, threads->new(\&read, $node->conn($i*$nthread+$j), $cguids[$i], $keyoff, $keyoff-1, $update));
            $keyoff = $keyoff+$nops+1;
        }
    }
    $_->join for (@threads);

    @threads = ();
    $keyoff = 900000;
    for my $i(0 .. $ncntr-1){
        for my $j(0 .. $nthread-1){
            push(@threads, threads->new(\&del, $node->conn($i*$nthread+$j), $cguids[$i], $keyoff, $nops));
            $keyoff = $keyoff+$nops+1;
        }
    }
    $_->join for (@threads);

    @threads = ();
    $keyoff = 900000;
    for my $i(0 .. $ncntr-1){
        for my $j(0 .. $nthread-1){
            push(@threads, threads->new(\&read_after_del, $node->conn($i*$nthread+$j), $cguids[$i], $keyoff, $keyoff-1, $nops));
            $keyoff = $keyoff+$nops+1;
        }
    }
    $_->join for (@threads);

    for (0 .. $ncntr-1){
        CloseContainer($node->conn(0), $cguids[$_]);
        DeleteContainer($node->conn(0), $cguids[$_]);
    }

    return;
}

sub test_init {
    $node = Fdftest::Node->new(
                ip     => "127.0.0.1",
                port   => "24422",
                nconn  => $ncntr*$nthread,
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
