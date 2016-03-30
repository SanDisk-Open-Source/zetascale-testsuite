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
use Fdftest::Stress;
use Test::More tests => 2828;

my $node;
my $nthread = 100;
my $ncntr = 5;
#my @data = ([50, 64000, 0.125], [100, 128000, 0.125], [150, 512, 0.75]);
my @data = ([64, 16000, 0.05], [74, 32000, 0.05], [84, 64000, 0.05], [94, 128000, 0.05], [104, 48,1]);

sub worker{
    my ($connid, $cguids_ref, $keyoff, $valoff, $nops, $mode) = @_;
    my $ret;

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

    $ret = ZSTransactionCommit(
        $node->conn($connid),
    );
    chomp($ret);
    if ($mode =~ /.*mode=1.*/){
        like($ret, qr/OK.*/, "ZSTransactionCommit: Update_ops > 100K ,Btree type container will OK");
    }elsif ($mode =~ /.*mode=2.*/){
        like($ret, qr/SERVER_ERROR ZS_TRANS_ABORTED/, "ZSTransactionCommit: Update_ops > 100K,Hash type container will fail, expect return $ret");
    }
}

sub test_run {
    my ($ret, $cguid);
    my (@cguids, @threads);
    my $keyoff = 1000;
    my $valoff = 100;
    my $size = 0;
    my $nobject = 30000;
    my $update = 120000;
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

        my $mode  = ZSTransactionGetMode (
            $node->conn(0),
        );
        chomp($mode);

        for(0 .. $nthread - 1)
        {
            $threads[$_] = threads->create(\&worker, 
                $_, 
                \@cguids, 
                $keyoff, 
                $valoff-1, 
                $update/$ncntr/$nthread,
                $mode,
            );
        }

        for(0 .. $nthread - 1)
        {
            $threads[$_]->join();
        } 

        my $update_per_cntr = $update / $ncntr;
        for(0 .. $ncntr - 1)
        {
            if ($mode =~ /.*mode=1.*/){
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
                    like($ret, qr/OK.*/, "ZSReadObject: check btree type container data update on cguid=$cguids[$_]");
                }
            }elsif ($mode =~ /.*mode=2.*/){
                foreach my $d(@data){
                    $ret = ZSReadObject(
                        $node->conn(0),
                        cguid         => $cguids[$_],
                        key_offset    => $keyoff,
                        key_len       => $$d[0],
                        data_offset   => $valoff,
                        data_len      => $$d[1],
                        nops          => $update_per_cntr*$$d[2],
                        check         => "yes",
                    );
                    like($ret, qr/OK.*/, "ZSReadObject: check hash type container data rollback  on cguid=$cguids[$_]");
                }
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
            if ($mode =~ /.*mode=1.*/){
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
                    like($ret, qr/OK.*/, "ZSReadObject: check update ".$update_per_cntr*$$d[2]." objects succeed after restart ZS on cguid=$cguids[$_]");
                }

                foreach my $d(@data){
                    $ret = ZSReadObject(
                        $node->conn(0),
                        cguid         => $cguids[$_],
                        key_offset    => $keyoff+$update_per_cntr*$$d[2],
                        key_len       => $$d[0],
                        data_offset   => $valoff+$update_per_cntr*$$d[2],
                        data_len      => $$d[1],
                        nops          => ($nobject-$update_per_cntr)*$$d[2],
                        check         => "yes",
                    );
                    like($ret, qr/OK.*/, "ZSReadObject: check ".($nobject-$update_per_cntr)*$$d[2]." objects succeed after restart ZS on cguid=$cguids[$_]");
                }
            }elsif ($mode =~ /.*mode=2.*/){
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


