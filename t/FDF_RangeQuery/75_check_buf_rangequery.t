# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file: t/ZS_RangeQuery/03_single_cntr_rangequery.t
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
use Test::More tests => 86;
use threads;

my $node;
sub test_RangeQuery_buf_invalid{
    my($conn, $cguid, $keybuf,$databuf,$start_key, $startlen,$endlen,$end_key,$flags,) = @_;
    my $ret = ZSGetRange (
                $conn,
                cguid         => $cguid,
                keybuf_size   => $keybuf,
                databuf_size  => $databuf,
                keylen_start  => $startlen,
                keylen_end    => $endlen,
                start_key     => $start_key,
                end_key       => $end_key,
                flags         => $flags,
                );
    
	if($keybuf ==0 || 0 == $databuf){
		like($ret, qr/CLIENT_ERROR.*/,"ZSGetRange:Get $start_key ~ $end_key,startlen=$startlen,endlen=$endlen,
                         keybuf = $keybuf,databuf=$databuf,flags=$flags->$ret");
	}
	else{
		like($ret, qr/OK.*/, "ZSRange:Get $start_key ~ $end_key,startlen=$startlen,endlen=$endlen,
					      keybuf = $keybuf,databuf=$databuf,flags=$flags->$ret");
		$ret = ZSGetNextRange(
				$conn,
				n_in          => $end_key+10,
				check         => "yes",
				);

		like($ret, qr/SERVER_ERROR.*/,"ZSGetNextRange:Get flags=$flags->$ret");

		$ret = ZSGetRangeFinish($conn);
		like($ret, qr/OK.*/, "ZSGetRangeFinish->$ret");

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
    my $keybuf = 60;
    my $databuf = 1024;
    my @prop = (["yes","no","yes","ZS_DURABILITY_HW_CRASH_SAFE","no"],);
    #my @data = ([50, 64000, 6250], [100, 128000, 6250], [150, 512, 37500]);
    my @data = ([64, 16000, 3000], [74, 32000, 3000], [84, 64000, 3000], [94, 128000, 3000], [104, 48,60000]);

    $ret = $node->start(
                ZS_REFORMAT  => 1,
           );
    like($ret, qr/OK.*/, 'Node start');

    foreach my $p(@prop){
            foreach my $d(@data){
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
            my $keylen = $$d[0];
            $keybuf = 0;
            $databuf = 1024;
            my $flags1 = 'ZS_RANGE_START_LE|ZS_RANGE_END_GE|ZS_RANGE_BUFFER_PROVIDED' ;
            test_RangeQuery_buf_invalid( $node->conn(0), $cguid,$keybuf,$databuf,$end_key, $keylen,$keylen,$key,$flags1,);

            $keybuf = 1024;
            $databuf = 0;
            test_RangeQuery_buf_invalid( $node->conn(0), $cguid,$keybuf,$databuf,$end_key, $keylen,$keylen,$key,$flags1,);
            
            $keybuf = 0;
            $databuf = 0;
            test_RangeQuery_buf_invalid( $node->conn(0), $cguid,$keybuf,$databuf,$end_key, $keylen,$keylen,$key,$flags1,);
            
            $keybuf = 1;
            $databuf = 1024;
            test_RangeQuery_buf_invalid( $node->conn(0), $cguid,$keybuf,$databuf,$end_key, $keylen,$keylen,$key,$flags1,);
            
            $keybuf = 1024;
            $databuf = 1;
            test_RangeQuery_buf_invalid( $node->conn(0), $cguid,$keybuf,$databuf,$end_key, $keylen,$keylen,$key,$flags1,);
            
            $keybuf = 1;
            $databuf = 1;
            test_RangeQuery_buf_invalid( $node->conn(0), $cguid,$keybuf,$databuf,$end_key, $keylen,$keylen,$key,$flags1,);
            
            $keybuf = 0;
            $databuf = 0;
            my $flags2 = 'ZS_RANGE_START_LE|ZS_RANGE_END_GE|ZS_RANGE_BUFFER_PROVIDED|ZS_RANGE_ALLOC_IF_TOO_SMALL' ;
            test_RangeQuery_buf_invalid( $node->conn(0), $cguid,$keybuf,$databuf,$end_key, $keylen,$keylen,$key,$flags2,);
            

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


