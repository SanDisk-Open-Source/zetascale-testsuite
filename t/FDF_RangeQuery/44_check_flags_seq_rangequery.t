# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file: t/ZS_RangeQuery/44_check_flags_seq_rangequery.t
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
use Test::More tests => 244;
use threads;

my $node;
sub test_RangeQuery_with_seq{
    my($conn, $cguid, $start_key, $key_len,$end_key,$start_seq,$end_seq,$flags,$out) = @_;
    my $ret;
    if($start_seq == -1){
        $ret = ZSGetRange (
                    $conn,
                    cguid         => $cguid,
                    keylen_start  => $key_len,
                    keylen_end    => $key_len,
                    start_key     => $start_key,
                    end_key       => $end_key,
                    end_seq       => $end_seq,
                    flags         => $flags,
                    );
    }elsif($end_seq == -1){
        $ret = ZSGetRange (
                    $conn,
                    cguid         => $cguid,
                    keylen_start  => $key_len,
                    keylen_end    => $key_len,
                    start_key     => $start_key,
                    end_key       => $end_key,
                    start_seq     => $start_seq,
                    flags         => $flags,
                    );
    }else{
        $ret = ZSGetRange (
                    $conn,
                    cguid         => $cguid,
                    keylen_start  => $key_len,
                    keylen_end    => $key_len,
                    start_key     => $start_key,
                    end_key       => $end_key,
                    start_seq     => $start_seq,
                    end_seq       => $end_seq,
                    flags         => $flags,
                    );
    }
    if($ret =~ /CLIENT_ERROR/ && $out == -1){
        like($ret, qr/CLIENT_ERROR.*/,"ZSGetRange:Get $start_key ~ $end_key,startlen=$key_len,endlen=$key_len,
                                                      start_seq=$start_seq,end_seq=$end_seq,flags=$flags->$ret");
    }else{
        like($ret, qr/OK.*/,"ZSGetRange:Get $start_key ~ $end_key,startlen=$key_len,endlen=$key_len,
                                             start_seq=$start_seq,end_seq=$end_seq,flags=$flags");

        $ret = ZSGetNextRange (
                    $conn,
                    n_in          => $out +1000000,
                    check         => "yes",
                    );
        if($ret =~ /SERVER_ERROR ZS_QUERY_DONE/){
            like($ret, qr/SERVER_ERROR ZS_QUERY_DONE.*/, "ZSGetNextRange:Get 0 objects,flag=$flags -> $ret");
        }
        else{
            my $n_out = $1 if($ret =~ /OK n_out=(\d+)/);
            like($ret, qr/OK.*/, "ZSGetNextRange:Get $n_out objects ,$ret");
        }

        $ret = ZSGetRangeFinish($conn);
        like($ret, qr/OK.*/, "ZSGetRangeFinish");
    }
 }

sub test_RangeQuery_only_seq{
    my($conn, $cguid,$start_seq,$end_seq,$flags,$out) = @_;
    my $ret;
    if($start_seq == -1){
        $ret = ZSGetRange (
                    $conn,
                    cguid         => $cguid,
                    end_seq       => $end_seq,
                    flags         => $flags,
                    );
    }elsif($end_seq == -1){
        $ret = ZSGetRange (
                    $conn,
                    cguid         => $cguid,
                    start_seq     => $start_seq,
                    flags         => $flags,
                    );
    }else{
        $ret = ZSGetRange (
                    $conn,
                    cguid         => $cguid,
                    start_seq     => $start_seq,
                    end_seq       => $end_seq,
                    flags         => $flags,
                    );
    }
    if($ret =~ /CLIENT_ERROR/ && $out == -1){
        like($ret, qr/CLIENT_ERROR.*/,"ZSGetRange:Get start_seq=$start_seq,end_seq=$end_seq,flags=$flags->$ret");
    }else{
        like($ret, qr/OK.*/,"ZSGetRange:Get start_seq=$start_seq,end_seq=$end_seq,flags=$flags");

        $ret = ZSGetNextRange (
                    $conn,
                    n_in          => $out +1000000,
                    check         => "yes",
                    );
        if($ret =~ /SERVER_ERROR ZS_QUERY_DONE/){
            like($ret, qr/SERVER_ERROR ZS_QUERY_DONE.*/, "ZSGetNextRange:Get 0 objects,flag=$flags -> $ret");
        }
        else{
            my $n_out = $1 if($ret =~ /OK n_out=(\d+)/);
            like($ret, qr/OK.*/, "ZSGetNextRange:Get $n_out objects ,$ret");
        }

        $ret = ZSGetRangeFinish($conn);
        like($ret, qr/OK.*/, "ZSGetRangeFinish");
    }
}

sub test_RangeQuery_without_seq{
    my($conn, $cguid, $start_key, $key_len,$end_key,$flags,$out) = @_;
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
    if($ret =~ /CLIENT_ERROR/ && $out== -1){
        like($ret, qr/CLIENT_ERROR.*/,"ZSCetRange: Get $start_key~$end_key,keylen=$key_len,falgs=$flags->$ret");
    }else{
        like($ret, qr/OK.*/,"ZSGetRange:Get $start_key ~ $end_key,startlen=$key_len,endlen=$key_len,flags=$flags->$ret");

        $ret = ZSGetNextRange (
                    $conn,
                    n_in          => $out +1000000,
                    check         => "yes",
                    );
        if($ret =~ /SERVER_ERROR ZS_QUERY_DONE/&& $out==0){
            like($ret, qr/SERVER_ERROR ZS_QUERY_DONE.*/, "ZSGetNextRange:Get 0 objects,flag=$flags -> $ret");
        }
        else{
            my $n_out = $1 if($ret =~ /OK n_out=(\d+)/);
            like($ret, qr/OK n_out=$out.*/, "ZSGetNextRange:Get $n_out objects ,$ret");
        }

        $ret = ZSGetRangeFinish($conn);
        like($ret, qr/OK.*/, "ZSGetRangeFinish");
    }
}


sub test_run {

    my $ret;
    my $cguid;
    my $cname = "Tran_Cntr";  
    my $key = 1;
    my $val_offset = $key;
    my $size = 0;
    my $flags = 'ZS_RANGE_START_GE|ZS_RANGE_END_LE' ;
    my $flags1 = 'ZS_RANGE_START_GE|ZS_RANGE_END_LE|ZS_RANGE_SEQNO_LE' ;
    my $flags2 = 'ZS_RANGE_START_GE|ZS_RANGE_END_LE|ZS_RANGE_SEQNO_GT_LE';
    my $flags3 = 'ZS_RANGE_START_GE|ZS_RANGE_END_LE|ZS_RANGE_SEQNO_GT_LE|ZS_RANGE_SEQNO_LE';
    my $flags4 = 'ZS_RANGE_SEQNO_LE' ;
    my $flags5 = 'ZS_RANGE_SEQNO_GT_LE';
    my $start_seq;
    my $end_seq;
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
        test_RangeQuery_without_seq( $node->conn(0), $cguid,$key, $$d[0],$end_key,$flags,$$d[2]);
        test_RangeQuery_without_seq( $node->conn(0), $cguid,$key, $$d[0],$end_key,$flags1,-1);
        test_RangeQuery_without_seq( $node->conn(0), $cguid,$key, $$d[0],$end_key,$flags2,-1);
        
        test_RangeQuery_only_seq( $node->conn(0), $cguid,1,100,$flags4,-1);
        test_RangeQuery_only_seq( $node->conn(0), $cguid,-1,1,$flags4,0);
        test_RangeQuery_only_seq( $node->conn(0), $cguid,1,-1,$flags4,-1);
        test_RangeQuery_only_seq( $node->conn(0), $cguid,$end_key,-1,100,$flags4,0);
        test_RangeQuery_only_seq( $node->conn(0), $cguid,$$d[2],1,$flags5,0);
        test_RangeQuery_only_seq( $node->conn(0), $cguid,1,$$d[2]+10000,$flags5,0);
        test_RangeQuery_only_seq( $node->conn(0), $cguid,$$d[2],-1,$flags5,-1);
         
        test_RangeQuery_with_seq( $node->conn(0), $cguid,$key, $$d[0],$end_key,$$d[2],$key,$flags,-1);
        test_RangeQuery_with_seq( $node->conn(0), $cguid,$key, $$d[0],$end_key,100,1,$flags1,-1);
        test_RangeQuery_with_seq( $node->conn(0), $cguid,$key, $$d[0],$end_key,1,100,$flags1,-1);
        test_RangeQuery_with_seq( $node->conn(0), $cguid,$key, $$d[0],$end_key,-1,1,$flags1,0);
        test_RangeQuery_with_seq( $node->conn(0), $cguid,$key, $$d[0],$end_key,1,-1,$flags1,-1);
        test_RangeQuery_with_seq( $node->conn(0), $cguid,$key, $$d[0],$end_key,-1,100,$flags1,0);
        test_RangeQuery_with_seq( $node->conn(0), $cguid,$key, $$d[0],$end_key,$$d[2],1,$flags2,0);
        test_RangeQuery_with_seq( $node->conn(0), $cguid,$key, $$d[0],$end_key,1,$$d[2]+10000,$flags2,0);
        test_RangeQuery_with_seq( $node->conn(0), $cguid,$key, $$d[0],$end_key,$$d[2],-1,$flags2,-1);
        test_RangeQuery_with_seq( $node->conn(0), $cguid,$key, $$d[0],$end_key,-1,-1,$flags2,-1);

        $ret = ZSGetRange (
                    $node->conn(0),
                    cguid         => $cguid,
                    keybuf_size   => 60,
                    databuf_size  => 1024,
                    keylen_start  => $$d[0],
                    keylen_end    => $$d[0],
                    start_key     => $key,
                    end_key       => $end_key,
                    start_seq     => $1,
                    end_seq       => $$d[2]+$$d[2],
                    flags         => $flags3,
                    );
        like($ret, qr/SERVER_ERROR ZS_FAILURE.*/,"ZSGetRange:Get $key ~ $end_key,flags=$flags,-> $ret");
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


