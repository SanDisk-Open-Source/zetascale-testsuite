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

# file: t/ZS_RangeQuery/16_single_cntr_10update_1range_rangequery.t
# author: shujing zhu
# email: shujingzhu@hengtiansoft.com
# date: June 17, 2013
# description: range query

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
use Test::More tests => 228;

my $node;
my $nthread = 10;


sub test_RangeQuery{
    my($conn, $cguid, $start_key, $key_len,$end_key,$flags,$nobject,) = @_;
    my $ret = ZSGetRange (
                $conn,
                cguid         => $cguid,
                keybuf_size   => 60,
                databuf_size  => 1024,
                keylen_start  => $key_len,
                keylen_end    => $key_len,
                start_key     => $start_key,
                end_key       => $end_key,
                flags         => $flags,
                );
    like($ret, qr/OK.*/,"ZSGetRange:Get $start_key ~ $end_key,flags=$flags -> $ret");

    $ret = ZSGetNextRange (
                $conn,
                n_in          => $nobject+10000,
                check         => "yes",
                );
    my $n_out = $1 if($ret =~ /OK n_out=(\d+)/);
    like($ret, qr/OK n_out=$nobject.*/, "ZSGetNextRange:Get $n_out objects ,$ret");

    $ret = ZSGetRangeFinish($conn);
    like($ret, qr/OK.*/, "ZSGetRangeFinish");
}


sub test_run {
    my $ret;
    my $cguid;
    my $cname = "Tran_Cntr";
    my @update_thread;
    my $key  = 1;
    my $val_offset = $key;
    my $update = 1000;
    my $size = 0;
    my $flags = 'ZS_RANGE_START_GE|ZS_RANGE_END_LE' ;
    my @prop = (["yes","no","yes","ZS_DURABILITY_HW_CRASH_SAFE","no"],);
    #my @data = ([50, 64000, 6250], [100, 128000, 6250], [150, 512, 37500]);
    my @data = ([64, 16000, 3000], [74, 32000, 3000], [84, 64000, 3000], [94, 128000, 3000], [104, 48,60000]);

    $ret = $node->start(
               ZS_REFORMAT  => 1,
           );
    like($ret, qr/OK.*/, 'Node start');

    foreach my $p(@prop){
        $ret = ZSOpenContainer(
                    $node->conn(0),
                    cname            => $cname,
                    fifo_mode        => "no",
                    persistent       => $$p[0],
                    writethru        => $$p[2],
                    evicting         => $$p[1],
                    size             => $size,
                    durability_level => $$p[3],
                    async_writes     => $$p[4],
                    num_shards       => 1,
                    flags            => "ZS_CTNR_CREATE"
                    );
        $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
        like($ret, qr/OK.*/, "ZSopenContainer: $cname, cguid=$cguid");

        foreach my $d(@data){
            $ret = ZSWriteObject(
                        $node->conn(0),
                        cguid         => $cguid,
                        key           => $key,
                        key_len       => $$d[0],
                        data_offset   => $val_offset,
                        data_len      => $$d[1],
                        nops          => $$d[2],
                        flags         => "ZS_WRITE_MUST_NOT_EXIST",
                        );
            like($ret, qr/OK.*/, "ZSWriteObject: load $$d[2] objects datalen=$$d[1],keylen=$$d[0] to $cname, cguid=$cguid");

            my $bject_per_thread = $$d[2]*2/$nthread;
            my $start_key = $key;
            my $end_key = $start_key + $$d[2]*2/$nthread -1;
            for(0 .. $nthread/2 - 1)
            {
                $flags = 'ZS_RANGE_START_GE|ZS_RANGE_END_LE';
                $update_thread[$_] = threads->create(\&test_RangeQuery,
                        $node->conn($_), 
                        $cguid, 
                        $start_key, 
                        $$d[0], 
                        $end_key, 
                        $flags, 
                        $$d[2]*2/$nthread);

                $flags = 'ZS_RANGE_START_LE|ZS_RANGE_END_GE';
                $update_thread[$_ + $nthread/2] = threads->create(\&test_RangeQuery,
                        $node->conn($_+ $nthread/2), 
                        $cguid, 
                        $end_key, 
                        $$d[0], 
                        $start_key, 
                        $flags, 
                        $$d[2]*2/$nthread);

                $start_key = $end_key;
                $end_key = $start_key + $$d[2]*2/$nthread -1;
            }
            
            for(0 .. $nthread - 1)
            {
                $update_thread[$_]->join();
            }


            $flags = 'ZS_RANGE_START_GE|ZS_RANGE_END_LE';
            test_RangeQuery($node->conn(0),$cguid, $key, $$d[0],$$d[2],$flags,$$d[2]);

            $flags = 'ZS_RANGE_START_LE|ZS_RANGE_END_GE';
            test_RangeQuery($node->conn(0),$cguid,$$d[2],$$d[0],$key,$flags,$$d[2]);
        }
        $ret = ZSCloseContainer(
                    $node->conn(0),
                    cguid      => $cguid,
                    );
        like($ret, qr/OK.*/, 'ZSCloseContainer');

        $ret = $node->stop();
        like($ret, qr/OK.*/, 'Node stop');

        $ret = $node->start(
                        ZS_REFORMAT  => 0,
                        );
        like($ret, qr/OK.*/, 'Node restart');

        $ret = ZSOpenContainer(
                    $node->conn(0),
                    cname            => $cname,
                    fifo_mode        => "no",
                    persistent       => $$p[0],
                    writethru        => $$p[2],
                    evicting         => $$p[1],
                    size             => $size,
                    durability_level => $$p[3],
                    async_writes     => $$p[4],
                    num_shards       => 1,
                    flags            => "ZS_CTNR_RW_MODE"
                    );
        $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
        like($ret, qr/OK.*/, "ZSopenContainer: $cname, cguid=$cguid");

        foreach my $d(@data){
            $ret = ZSReadObject(
                        $node->conn(0),
                        cguid         => $cguid,
                        key           => $key,
                        key_len       => $$d[0],
                        data_offset   => $val_offset,
                        data_len      => $$d[1],
                        nops          => $$d[2],
                        check         => "yes",
                        );
            like($ret, qr/OK.*/, "ZSReadObject: check $$d[2] objects succeed after restart ZS on $cname, cguid=$cguid");

            $flags = 'ZS_RANGE_START_GE|ZS_RANGE_END_LE';
            test_RangeQuery($node->conn(0),$cguid, $key, $$d[0],$$d[2],$flags,$$d[2]);

            $flags = 'ZS_RANGE_START_LE|ZS_RANGE_END_GE';
            test_RangeQuery($node->conn(0),$cguid,$$d[2],$$d[0],$key,$flags,$$d[2]);
        }
        $ret = ZSCloseContainer(
                    $node->conn(0),
                    cguid      => $cguid,
                    );
        like($ret, qr/OK.*/, 'ZSCloseContainer');

        $ret = ZSDeleteContainer(
                    $node->conn(0),
                    cguid      => $cguid,
                    );
        like($ret, qr/OK.*/, 'ZSDeleteContainer');
    }
    return;
}

sub test_init {
    $node = Fdftest::Node->new(
                ip     => "127.0.0.1",
                port   => "24422",
                nconn  => 10,
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


