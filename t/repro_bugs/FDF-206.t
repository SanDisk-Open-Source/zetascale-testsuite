# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file: 
# author: Yiwen Lu
# email: yiwenlu@hengtiansoft.com
# date: Dec 10, 2014
# description: 

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::Stress;
use Test::More 'no_plan';

my $node; 
my @ctr_type = ("BTREE","BTREE");

sub test_run {
    my $ret;
    my $cguid;
    
    $ret = $node->set_ZS_prop (ZS_MAX_NUM_CONTAINERS => 64000);
    like ($ret, qr//, 'set ZS_MAX_NUM_CONTAINERS to 64K');

    $ret = $node->start(
               ZS_REFORMAT  => 1,
           );
    like($ret, qr/OK.*/, 'Node start');

    for(my $j=1; $j<=65535; $j++){
       if ($j<=64000){
        $ret = ZSOpen($node->conn(0),"ctr-$j",4,0,"ZS_CTNR_CREATE","yes","ZS_DURABILITY_SW_CRASH_SAFE",$ctr_type[$j%2]);
        like ($ret, qr/^OK.*/, $ret);
      	$cguid = $1 if($ret =~ /OK cguid=(\d+)/);
     }else {
    
    $ret = ZSOpen($node->conn(0),"ctr-64001",4,0,"ZS_CTNR_CREATE","yes","ZS_DURABILITY_SW_CRASH_SAFE","BTREE");
    like ($ret, qr/^SERVER_ERROR ZS_TOO_MANY_CONTAINERS.*/, $ret);
    #print "ret = $ret\n";
    }
  }
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
    $node->set_ZS_prop (ZS_REFORMAT => 1, ZS_MAX_NUM_CONTAINERS => 6000);

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


