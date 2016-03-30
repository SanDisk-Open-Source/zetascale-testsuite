# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
#clean file:
# author: Jie Wang(Grace)
# email: JieWang@hengtiansoft.com
# date: Apr 25, 2015
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
use Test::More tests => 333;


my $node;
my $nthread = 5;
my $ncntr = 5;
my @data = ([64, 16000, 1/24], [74, 32000, 1/24], [84, 64000, 1/24], [94, 128000, 1/24], [104, 48,5/6]);

sub worker{
    my ($conn, $cguid, $keyoff, $valoff, $nops) = @_;
    my $ret;

    $ret = ZSTransactionStart(
        $conn,
    );
    like($ret, qr/OK.*/, 'ZSTransactionStart');


    foreach my $d(@data){
        $ret = ZSWriteObject(
                $conn,
                cguid         => $cguid,
                key_offset    => $keyoff,
                key_len       => $$d[0],
                data_offset   => $valoff,
                data_len      => $$d[1],
                nops          => $nops*$$d[2],
                flags         => "ZS_WRITE_MUST_EXIST",
            );
        like($ret, qr/OK.*/, "ZSWriteObject, update ".$nops*$$d[2]." objects on cguid=$_");
    }
}

sub read{
    my ($conn, $j, $cguid, $keyoff, $valoff, $nops) = @_;
    my $ret;
  
    foreach my $d(@data){
        $ret = ZSReadObject(
                        $conn,
                        cguid         => $cguid,
                        key_offset    => $keyoff+$nops*$$d[2]*$j,
                        key_len       => $$d[0],
                        data_offset   => $valoff+$nops*$$d[2]*$j,
                        data_len      => $$d[1],
                        nops          => $nops*$$d[2],
                        check         => "yes",
                    );
        like($ret, qr/OK.*/, "ZSReadObject: check update ".$nops*$$d[2]." objects succeed on cguid=$cguid");
    }
}

sub test_run {
    my ($ret, $cguid0, $cguid);
    my (@cguids, @threads);
    my $size = 0;
    my $keyoff = 1000;
    my $valoff = 100;
    my $nobject = 180000;
    my $update = 120000;
    my @prop = (["yes","yes","no","ZS_DURABILITY_HW_CRASH_SAFE","no"],);

    $ret = $node->start(
        ZS_REFORMAT  => 1,
        threads      => $nthread*$ncntr,
    );
    like($ret, qr/OK.*/, 'Node start');

    foreach my $p(@prop){
        @cguids = ();
        for(0 .. $ncntr - 1)
        {
            my $cname = 'Tran_Cntr' . "$_";
            $ret = ZSOpenContainer(
                $node->conn(0),
                cname            => $cname,
                fifo_mode        => "no",
                persistent       => $$p[0],
                writethru        => $$p[1],
                evicting         => $$p[2],
                size             => $size,
                durability_level => $$p[3],
                async_writes     => $$p[4],
                num_shards       => 1,
                flags            => "ZS_CTNR_CREATE",
            );
            $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
            like($ret, qr/OK.*/, "ZSopenContainer: cname=$cname, cguid=$cguid");
            push(@cguids, $cguid);

            foreach my $d(@data){
                $ret = ZSWriteObject(
                    $node->conn(0),
                    cguid         => $cguid,
                    key_offset    => $keyoff,
                    key_len       => $$d[0],
                    data_offset   => $valoff,
                    data_len      => $$d[1],
                    nops          => $nobject*$$d[2],
                    flags         => "ZS_WRITE_MUST_NOT_EXIST",
                );
                like($ret, qr/OK.*/, "ZSWriteObject: load ".$nobject*$$d[2]." objects to Tran_Cntr$_, cguid=$cguid");
            }
        }

        for(0 .. $nthread - 1){
            push(@threads, threads->new(\&worker,
                $node->conn($_),
                $cguids[$_],
                $keyoff,
                $valoff-1-$_,
                $update,
            ));
        }
        $_->join for (@threads);

        my $up_per_th = $update/$nthread;
        @threads = ();
        for my $i(0 .. $ncntr - 1){
            for my $j(0 .. $nthread - 1){
                push(@threads, threads->new(\&read, $node->conn($i*$nthread+$j),$j,$cguids[$i],$keyoff,$valoff-1-$i,$up_per_th));
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

        for(0 .. $ncntr -1)
        {
            my $cname = 'Tran_Cntr' . "$_";
            $ret = ZSOpenContainer(
                $node->conn(0),
                cname            => $cname,
                fifo_mode        => "no",
                persistent       => $$p[0],
                writethru        => $$p[1],
                evicting         => $$p[2],
                size             => $size,
                durability_level => $$p[3],
                async_writes     => $$p[4],
                num_shards       => 1,
                flags            => "ZS_CTNR_RW_MODE",
            );
            $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
            like($ret, qr/OK.*/, "ZSopenContainer: cname=$cname, cguid=$cguid");
            push(@cguids, $cguid);
        }

        my $nops_per_th = $nobject/$nthread;
        @threads = ();
        for my $i(0 .. $ncntr - 1){
            for my $j(0 .. $nthread - 1){
                push(@threads, threads->new(\&read, $node->conn($i*$nthread+$j),$j,$cguids[$i],$keyoff,$valoff,$nops_per_th));
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

