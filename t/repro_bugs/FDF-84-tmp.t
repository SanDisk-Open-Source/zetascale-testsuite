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
my $nconn = 128;

sub test_run {
    my $cguid;
    my $ret;
    my @cguids;
    my @threads;    
    my %cguid_cname;
    my $nctr = 2;
    my @ctr_type = ("BTREE","HASH");
    my @ctr_type2 = ("HASH","BTREE");


    $ret = $node->set_ZS_prop (ZS_FLASH_SIZE => 12);
    like ($ret, qr//, 'set ZS_FLASH_SIZE to 12G');
    $ret = $node->start(ZS_REFORMAT => 1);
    like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=1");
    @cguids = [];
    foreach(0..$nctr-1)
    {
        $ret = ZSOpen($node->conn(0),"ctr-$_",3,0,"ZS_CTNR_CREATE","yes","ZS_DURABILITY_SW_CRASH_SAFE",$ctr_type2[$_%2]);
        like ($ret, qr/^OK.*/, $ret);
        $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
        $cguids[$_]=$cguid;
        $cguid_cname{$cguid}="ctr-$_";
    }

    $ret = ZSSet($node->conn(0), $cguids[0], 150, 150, 900000, 10000, "ZS_WRITE_MUST_NOT_EXIST");
    like ($ret, qr/SERVER_ERROR.*/, $ret);
    
    $ret = ZSSet($node->conn(0), $cguids[1], 150, 150, 900000, 10000, "ZS_WRITE_MUST_NOT_EXIST");
    like ($ret, qr/SERVER_ERROR.*/, $ret);
    $ret = $node->stop();
    like($ret,qr/OK.*/,"Node Stop");
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
    $node->set_ZS_prop(ZS_FLASH_SIZE => 512);
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


