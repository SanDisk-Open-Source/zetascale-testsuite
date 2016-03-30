# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file:
# author: yiwen lu
# email: yiwenlu@hengtiansoft.com
# date: Jan 3, 2013
# description:

#!/usr/bin/perl

use strict;
use warnings;
use Switch;
use threads;


use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::Stress;
use Test::More tests => 7;

my $node; 
my $nconn = 1;
sub worker_write{
    my ($con,$cguid) = @_;
    my $res;
    $res = ZSSet($node->conn($con), $cguid, 50, 100, 100, 5000, "ZS_WRITE_MUST_NOT_EXIST");
    like ($res, qr/^OK.*/, $res);
}

sub worker_read{
    my ($con,$cguid) = @_;
    my $res;
    $res = ZSGet($node->conn($con), $cguid, 50, 100, 100, 5000);
    like ($res, qr/^OK.*/, $res);
}

sub test_run {
    my $cguid;
    my $ret;
    $ret = $node->start(ZS_REFORMAT => 1);    
    like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=1");
   
    $ret = ZSOpen($node->conn(0),"ctr-hash",3,0,"ZS_CTNR_CREATE","yes","ZS_DURABILITY_SW_CRASH_SAFE","HASH");
    like ($ret, qr/^OK.*/, $ret);
    $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);

    $ret = ZSMPut (
        $node->conn (0),
        cguid       => $cguid,
        key_offset  => 50,
        key_len     => 50,
        data_offset => 50,
        data_len    => 100,
        num_objs    => 100,
        flags       => "ZS_WRITE_MUST_NOT_EXIST",
    );
    like ($ret, qr/SERVER_ERROR ZS_FAILURE_INVALID_CONTAINER_TYPE/, "ZSMPut");

    $ret = ZSGetRange (
        $node->conn(0),
        cguid       => $cguid,
        keybuf_size   => 50,
        databuf_size  => 1024,
        keylen_start  => 8,
        keylen_end    => 8,
        start_key     => 50,
        end_key       => 8,
    );
    like ($ret, qr/SERVER_ERROR ZS_FAILURE_INVALID_CONTAINER_TYPE/, "ZSGetRange");

    $ret = ZSCreateContainerSnapshot($node->conn (0),cguid  => $cguid);
    like ($ret, qr/SERVER_ERROR ZS_FAILURE_INVALID_CONTAINER_TYPE/, "ZSSnapShot");


    $ret = ZSClose($node->conn(0), $cguid);
    like ($ret, qr/^OK.*/, $ret);
    $ret = ZSDelete($node->conn(0), $cguid);
    like ($ret, qr/^OK.*/, $ret);
    return;
}

sub test_init {
    $node = Fdftest::Node->new(
                ip     => "127.0.0.1", 
                port   => "24422",
                nconn  => $nconn,
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


