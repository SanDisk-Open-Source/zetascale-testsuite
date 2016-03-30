# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file:
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
use Fdftest::Stress;
use Test::More tests => 2803;

my $node;
my $nthread = 100;
my $ncntr = 5;
#my @data = ([50, 64000, 0.125], [100, 128000, 0.125], [150, 512, 0.75]);
my @data = ([64, 16000, 0.05], [74, 32000, 0.05], [84, 64000, 0.05], [94, 128000, 0.05], [104, 48,1]);

sub worker{
    my ($connid, $cguids_ref, $keyoff, $valoff, $nops) = @_;
    my $ret;

    my $mode = ZSTransactionGetMode(
        $node->conn($connid), 
    );
    chomp($mode);

    $ret = ZSTransactionStart(
        $node->conn($connid),
    );
    like($ret, qr/OK.*/, 'ZSTransactionStart');

    my $i = 0;
    for (@$cguids_ref){
        foreach my $d(@data){
            $ret = ZSWriteObject(
                $node->conn($connid),
                cguid         => $_,
                key_offset    => $keyoff+$nops*$$d[2]*$connid,
                key_len       => $$d[0],
                data_offset   => $valoff+$nops*$$d[2]*$connid-$i,
                data_len      => $$d[1],
                nops          => $nops*$$d[2],
                flags         => "ZS_WRITE_MUST_EXIST",
            );
            like($ret, qr/OK.*/, "ZSWriteObject, update ".$nops*$$d[2]." objects on cguid=$_");
        }
        $i = $i + 1;
    }

    $ret = ZSTransactionRollback(
        $node->conn($connid),
    );
    if ($mode =~ /.*mode=1.*/){
        like($ret, qr/SERVER_ERROR ZS_UNSUPPORTED_REQUEST.*/, 'ZSTransactionRollback, expect return SERVER_ERROR ZS_UNSUPPORTED_REQUEST.');
    }elsif ($mode =~ /.*mode=2.*/){
        like($ret, qr/OK.*/, 'ZSTransactionRollback succeed.');
    }
}

sub test_run {
    my ($ret, $cguid);
    my (@cguids, @threads);
    my $keyoff = 1000;
    my $valoff = 100;
    my $size = 0;
    my $nobject = 50000;
    my $update = 100000;
    my @prop = (["yes","yes","no","ZS_DURABILITY_HW_CRASH_SAFE","no"],);

    $ret = $node->start(
        ZS_REFORMAT  => 1,
        threads      => $nthread,
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

        for(0 .. $nthread - 1)
        {
            $threads[$_] = threads->create(\&worker, 
                $_, 
                \@cguids, 
                $keyoff, 
                $valoff-1, 
                $update/$ncntr/$nthread,
            );
        }

        for(0 .. $nthread - 1)
        {
            $threads[$_]->join();
        } 

        my $update_per_cntr = $update / $ncntr;
        for(0 .. $ncntr - 1)
        {
            foreach my $d(@data){
                $ret = ZSReadObject(
                    $node->conn(0),
                    cguid         => $cguids[$_],
                    key_offset    => $keyoff,
                    key_len       => $$d[0],
                    data_offset   => $valoff-1-$_,
                    data_len      => $$d[1],
                    nops          => $update_per_cntr*$$d[2],
                    check         => "yes",
                );
                like($ret, qr/OK.*/, "ZSReadObject: check update ".$update_per_cntr*$$d[2]." objects succeed on cguid=$cguids[$_]");
            }
        }

        for(0 .. $ncntr - 1)
        {
            $ret = ZSCloseContainer(
	        $node->conn(0),
	        cguid      => $cguids[$_],
            );
            like($ret, qr/OK.*/, "ZSCloseContainer: cguid=$cguids[$_]");
        }

        $ret = $node->stop();
        like($ret, qr/OK.*/, 'Node stop');

        $ret = $node->start(
            ZS_REFORMAT  => 0,
        );
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

        for(0 .. $ncntr - 1)
        {
            foreach my $d(@data){
                $ret = ZSReadObject(
                    $node->conn(0),
                    cguid         => $cguids[$_],
                    key_offset    => $keyoff,
                    key_len       => $$d[0],
                    data_offset   => $valoff,
                    data_len      => $$d[1],
                    nops          => $nobject*$$d[2],
                    check         => "yes",
                );
                like($ret, qr/OK.*/, "ZSReadObject: check rollback ".$nobject*$$d[2]." objects succeed after restart ZS on cguid=$cguids[$_]");
            }
        }

        for(0 .. $ncntr - 1)
        {
            $ret = ZSCloseContainer(
                $node->conn(0),
                cguid      => $cguids[$_],
            );
            like($ret, qr/OK.*/, "ZSCloseContainer: cguid=$cguids[$_]");

            $ret = ZSDeleteContainer(
                $node->conn(0),
                cguid      => $cguids[$_],
            );
            like($ret, qr/OK.*/, "ZSDeleteContainer: cguid=$cguids[$_]");
        }
    }

    return;
}

sub test_init {
    $node = Fdftest::Node->new(
        ip     => "127.0.0.1",
        port   => "24422",
        nconn  => $nthread,
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

