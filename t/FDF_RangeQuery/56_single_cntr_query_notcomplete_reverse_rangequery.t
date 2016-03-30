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

# file: t/ZS_RangeQuery/56_single_cntr_query_notcomplete_reverse_rangequery.t
# author: shujing zhu
# email: shujingzhu@hengtiansoft.com
# date: June 11, 2013
# description: range query cntr

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::BasicTest;
use Test::More tests => 69;
use threads;

my $node;

sub test_run {

    my $ret;
    my $cguid;
    my $cname = "Tran_Cntr";  
    my $key = 1;
    my $val_offset = $key;
    my $size = 0;
    my $flags_num = 4;
    my $flag ;
    my @flags = ('ZS_RANGE_START_LE|ZS_RANGE_END_GE',
				 'ZS_RANGE_START_LT|ZS_RANGE_END_GE|ZS_RANGE_KEYS_ONLY',
				 'ZS_RANGE_START_LE|ZS_RANGE_END_GT|ZS_RANGE_INDEX_USES_DATA',
				 'ZS_RANGE_START_LT|ZS_RANGE_END_GT|ZS_RANGE_PRIMARY_KEY') ;
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
            like($ret, qr/OK.*/, "ZSWriteObject: load $$d[2] objects keylen= $$d[0] datalen=$$d[1] to $cname, cguid=$cguid");

            my $end_key = $$d[2] - 20;
            my @out_num = ($$d[2]-20,$$d[2]-21,$$d[2]-21,$$d[2]-22);
            for (0 .. $flags_num-1){
                $flag = $flags[$_];
                my $key1 = $key + $_;
                my $end_key1 = $end_key + $_;
                $ret = ZSGetRange (
                            $node->conn(0),
                            cguid         => $cguid,
                            keybuf_size   => 60, 
                            databuf_size  => 1024,
                            keylen_start  => $$d[0],
                            keylen_end    => $$d[0],           
                            start_key     => $end_key1,
                            end_key       => $key1,
                            flags         => $flag,
                            );
                like($ret, qr/OK.*/,"ZSGetRange:Get $end_key1 ~ $key1,keylen=$$d[0],flags=$flag");

                $ret = ZSGetNextRange (
                            $node->conn(0),
                            n_in          => $$d[2]+10000, 
                            check         => "yes",
                            );
                my $n_out = $1 if($ret =~ /OK n_out=(\d+)/);
                like($ret, qr/OK n_out=$out_num[$_].*/, "ZSGetNextRange:Get $n_out objects ,$ret");

                $ret = ZSGetRangeFinish($node->conn(0));
                like($ret, qr/OK.*/, "ZSGetRangeFinish");
            }
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
                nconn  => 1,
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


