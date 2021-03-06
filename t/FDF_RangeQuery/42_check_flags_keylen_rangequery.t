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

# file: t/ZS_RangeQuery/42_check_flags_keylen_rangequery.t
# author: shujing zhu
# email: shujingzhu@hengtiansoft.com
# date: June 20, 2013
# description: range query cntr

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::BasicTest;
use Test::More tests => 84;
use threads;

my $node;
sub test_RangeQuery{
    my($conn, $cguid, $start_key, $startlen,$endlen,$end_key,$flags,$out) = @_;
    my $ret = ZSGetRange (
                $conn,
                cguid         => $cguid,
                keybuf_size   => 60,
                databuf_size  => 1024,
                keylen_start  => $startlen,
                keylen_end    => $endlen,
                start_key     => $start_key,
                end_key       => $end_key,
                flags         => $flags,
                );
    like($ret, qr/OK.*/,"ZSGetRange:Get $start_key ~ $end_key,startlen=$startlen,endlen=$endlen,flag=$flags ");

    $ret = ZSGetNextRange (
                $conn,
                n_in          => $end_key+100000,
                check         => "yes",
                );
    my $n_out = $1 if($ret =~ /OK n_out=(\d+)/);
    if($ret =~ /SERVER_ERROR ZS_QUERY_DONE/ && $out==0){
        like($ret, qr/SERVER_ERROR ZS_QUERY_DONE.*/, "ZSGetNextRange:Get 0 objects,flag=$flags -> $ret");
    }
    else{
        like($ret, qr/OK n_out=$out.*/, "ZSGetNextRange:Get $n_out objects ,$ret");
    }

    $ret = ZSGetRangeFinish($conn);
    like($ret, qr/OK.*/, "ZSGetRangeFinish");
 }

sub test_run {

    my $ret;
    my $cguid;
    my $cname = "Tran_Cntr";  
    my $key = 1;
    my $val_offset = $key;
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
            like($ret, qr/OK.*/, "ZSWriteObject: load $$d[2] objects keylen= $$d[0] datalen=$$d[1] to $cname, cguid=$cguid");

            my $end_key = $$d[2]+$key;
            my $keylen_start = $$d[0];
            my $keylen_end = $$d[0]+5;
            
            test_RangeQuery( $node->conn(0), $cguid,$key, $keylen_start,$keylen_end,$end_key,$flags,0);

            
            $keylen_start = $$d[0]+5;
            $keylen_end = $$d[0];
            test_RangeQuery( $node->conn(0), $cguid,$key, $keylen_start,$keylen_end,$end_key,$flags,$$d[2]);

            $keylen_start = $$d[0];
            test_RangeQuery( $node->conn(0), $cguid,$key, $keylen_start,$keylen_end,$end_key,$flags,$$d[2]);

            $keylen_start = $$d[0]+5;
            $keylen_end = $$d[0]+5;
            test_RangeQuery( $node->conn(0), $cguid,$key, $keylen_start,$keylen_end,$end_key,$flags,0);
            
            $keylen_start = $$d[0]+60;
            $keylen_end = $$d[0];
            test_RangeQuery( $node->conn(0), $cguid,$key, $keylen_start,$keylen_end,$end_key,$flags,$$d[2]);
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


