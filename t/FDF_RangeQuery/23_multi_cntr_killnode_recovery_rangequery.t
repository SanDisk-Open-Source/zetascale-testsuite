# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file: 23_multi_cntr_killnode_recovery_recovery_rangequery.t
# author: Jing Xu(Lee)
# email: leexu@hengtiansoft.com
# date: June 21, 2013
# description: range query cntr

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::BasicTest;
use Test::More tests => 202;
use threads;

my $node;

sub test_run {

    my $ret;
    my $cguid;
    my @cguid;
    my $cname = "Cntr";  
    my $ncntr = 5;
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
        for(my $i =0;$i< $ncntr;$i++){
            $ret = ZSOpenContainer(
                        $node->conn(0),
                        cname            => 'cntr'.$i,
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
            like($ret, qr/OK.*/, "ZSopenContainer: cntr$i, cguid=$cguid");
            push(@cguid, $cguid);

            foreach my $d(@data){
                $ret = ZSWriteObject(
                            $node->conn(0),
                            cguid         => $cguid[$i],
                            key           => $key,
                            key_len       => $$d[0],
                            data_offset   => $val_offset,
                            data_len      => $$d[1],
                            nops          => $$d[2],
                            flags         => "ZS_WRITE_MUST_NOT_EXIST",
                            );
                like($ret, qr/OK.*/, "ZSWriteObject: load $$d[2] objects keylen=$$d[0],datalen=$$d[1] to cntr$i, cguid=$cguid[$i]");
            }  
        }

        foreach my $d(@data){
            my $end_key = $$d[2] + $key;
            for(0 .. $ncntr - 1){
                $ret = ZSGetRange (
                            $node->conn(0),
                            cguid         => $cguid[$_],
                            keybuf_size   => 60, 
                            databuf_size  => 1024,
                            keylen_start  => $$d[0],
                            keylen_end    => $$d[0],           
                            start_key     => $key,
                            end_key       => $end_key,
                            flags         => $flags,
                            );
                like($ret, qr/OK.*/,"ZSGetRange:Get $key ~ $end_key,cntr$_,cguid=$cguid[$_],keylen_start=$$d[0],flags=$flags");

                $ret = ZSGetNextRange (
                        $node->conn(0),
                        n_in          => $$d[2]+10, 
                        check         => "yes",
                        );
                my $n_out = $1 if($ret =~ /OK n_out=(\d+)/);
                like($ret, qr/OK n_out=$$d[2].*/, "ZSGetNextRange:Get $n_out objects ,$ret");

                $ret = ZSGetRangeFinish($node->conn(0));
                like($ret, qr/OK.*/, "ZSGetRangeFinish");
            }
        }

        for(0 .. $ncntr - 1){
            $ret = ZSCloseContainer(
                        $node->conn(0),
                        cguid      => $cguid[$_],
                        );
            like($ret, qr/OK.*/, 'ZSCloseContainer');
        }

        $ret = $node->kill ();
        #like ($ret, qr/OK.*/, 'Node stop');

        $ret = $node->start (ZS_REFORMAT => 0,);
        like ($ret, qr/OK.*/, 'Node restart');

        for(0 .. $ncntr - 1){
            $ret = ZSOpenContainer(
                        $node->conn(0),
                        cname            => 'cntr'.$_,
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
            like($ret, qr/OK.*/, "ZSopenContainer: cntr$_, cguid=$cguid flags=RW_MODE");
        }

        foreach my $d(@data){
            my $end_key = $$d[2] + $key;
            for(0 .. $ncntr - 1){
                $ret = ZSGetRange (
                            $node->conn(0),
                            cguid         => $cguid[$_],
                            keybuf_size   => 60,
                            databuf_size  => 1024,
                            keylen_start  => $$d[0],
                            keylen_end    => $$d[0],
                            start_key     => $key,
                            end_key       => $end_key,
                            flags         => $flags,
                            );
                like($ret, qr/OK.*/,"ZSGetRange:Get $key ~ $end_key,cntr$_,cguid=$cguid[$_],keylen_start=$$d[0],flags=$flags");

                $ret = ZSGetNextRange (
                        $node->conn(0),
                        n_in          => $$d[2]+2,
                        check         => "yes",
                        );
                my $n_out = $1 if($ret =~ /OK n_out=(\d+)/);
                like($ret, qr/OK n_out=$$d[2].*/, "ZSGetNextRange:Get $n_out objects ,$ret");

                $ret = ZSGetRangeFinish($node->conn(0));
                like($ret, qr/OK.*/, "ZSGetRangeFinish");
            }
        }

        for(my $i=$ncntr - 1; $i>=0;$i--){
            $ret = ZSCloseContainer(
                        $node->conn(0),
                        cguid      => $cguid[$i],
                        );
            like($ret, qr/OK.*/, 'ZSCloseContainer');

            $ret = ZSDeleteContainer(
                        $node->conn(0),
                        cguid      => $cguid[$i],
                        );
            like($ret, qr/OK.*/, 'ZSDeleteContainer');
            pop(@cguid);
        }
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


