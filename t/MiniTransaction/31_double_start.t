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
use Test::More tests => 1204;


my $node;
my $nthread = 10;
my $ncntr = 5;
my @data = ([64, 16000, 0.05], [74, 32000, 0.05], [84, 64000, 0.05], [94, 128000, 0.05], [104, 48, 1]);

sub worker{
    my ($conn, $j, $cguid, $keyoff, $valoff, $nops, $mode) = @_;
    my $ret;

    $ret = ZSTransactionStart(
        $conn,
    );
    like($ret, qr/OK.*/, 'ZSTransactionStart');
    foreach my $d(@data)
    {
        $ret = ZSWriteObject(
                $conn,
                cguid         => $cguid,
                key_offset    => $keyoff+$nops*$$d[2]*$j,
                key_len       => $$d[0],
                data_offset   => $valoff+$nops*$$d[2]*$j,
                data_len      => $$d[1],
                nops          => $nops*$$d[2],
                flags         => "ZS_WRITE_MUST_EXIST",
            );
            like($ret, qr/OK.*/, "ZSWriteObject, update ".$nops*$$d[2]." objects on cguid=$cguid");

    }

    $ret = ZSTransactionStart(
        $conn,
    );
    if ($mode =~ /.*mode=1.*/){
        like($ret, qr/OK.*/, 'ZSTransactionStart again success');
    }elsif ($mode =~ /.*mode=2.*/){
        like($ret, qr/SERVER_ERROR ZS_FAILURE.*/, 'ZSTransactionStart again, expect return SERVER_ERROR ZS_FAILURE');
    }

    $ret = ZSTransactionCommit(
        $conn,
    );
    like($ret, qr/OK.*/, 'ZSTransactionCommit success');
}

sub read{
    my ($conn, $j, $cguid, $keyoff, $valoff, $off, $nops) = @_;
    my $ret;
    #print "==== $conn,$j,$cguid,$keyoff,$valoff,$off,$nops====\n" ; 
    foreach my $d(@data)
    {
        $ret = ZSReadObject(
                        $conn,
                        cguid         => $cguid,
                        key_offset    => $keyoff+$nops*$$d[2]*$j+$off*$$d[2],
                        key_len       => $$d[0],
                        data_offset   => $valoff+$nops*$$d[2]*$j+$off*$$d[2],
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
    my $nobject = 5000;
    my $update = 4000;
    my @prop = (["yes","yes","no","ZS_DURABILITY_HW_CRASH_SAFE","no"],);

    $ret = $node->start(
        ZS_REFORMAT  => 1,
        threads      => $nthread*$ncntr,
    );
    like($ret, qr/OK.*/, 'Node start');

    foreach my $p(@prop)
    {

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

            foreach my $d(@data)
            {
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

        my $mode = ZSTransactionGetMode(
            $node->conn(0),
        );
        chomp($mode);
        like($mode, qr/OK.*/, "ZSTransactionGetMode: $mode");

        my $up_per_th = $update/$ncntr/$nthread;
        @threads = ();
        for my $i(0 .. $ncntr - 1){
            for my $j(0 .. $nthread - 1){
                push(@threads, threads->new(\&worker,
                    $node->conn($i*$nthread+$j),
                    $j,
                    $cguids[$i],
                    $keyoff,
                    $valoff-1-$i,
                    $up_per_th,
                    $mode,
                ));
            }
        }
        $_->join for (@threads);

        @threads = ();
        for my $i(0 .. $ncntr - 1){
            for my $j(0 .. $nthread - 1){
                push(@threads, threads->new(\&read, $node->conn($i*$nthread+$j), $j, $cguids[$i], $keyoff, $valoff-1-$i, 0, $up_per_th));
            }
        }
        $_->join for (@threads);

        for(0 .. $ncntr - 1){
            $ret = ZSCloseContainer(
                $node->conn(0),
                cguid      => $cguids[$_],
            );
            like($ret, qr/OK.*/, "ZSCloseContainer: cguid=$cguids[$_]");
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

        @threads = ();
        for my $i(0 .. $ncntr - 1){
            for my $j(0 .. $nthread - 1){
                if ($mode =~ /.*mode=1.*/){
                    push(@threads, threads->new(\&read, $node->conn($i*$nthread+$j),$j,$cguids[$i], $keyoff, $valoff,0,$up_per_th));
                }elsif ($mode =~ /.*mode=2.*/){
                    push(@threads, threads->new(\&read, $node->conn($i*$nthread+$j),$j,$cguids[$i], $keyoff, $valoff-1-$i,0,$up_per_th));
                }
            }
        }
        $_->join for (@threads);

        my $other_per_th = ($nobject-$update/$ncntr)/$nthread;
        @threads = ();
        for my $i(0 .. $ncntr - 1){
            for my $j(0 .. $nthread - 1){
                push(@threads, threads->new(\&read, $node->conn($i*$nthread+$j),$j,$cguids[$i], $keyoff, $valoff,$update/$ncntr,$other_per_th));
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


