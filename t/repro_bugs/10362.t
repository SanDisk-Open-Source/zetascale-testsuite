# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file:
# author: yiwen lu
# email: yiwenlu@hengtiansoft.com
# date: Jan 16, 2013
# description:

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::TestCase;
use Test::More tests => 15;

my $node; 
sub test_run {
  
    my ($cguid,@cguids);
    my $ret; 
    
    print'<<< test with async_writes=no >>>'."\n";
    $ret = $node->start(ZS_REFORMAT => 1);    
    like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=1");
    
    $ret = OpenContainer($node->conn(0),"ctr-0","ZS_CTNR_CREATE",1048576,4,"ZS_DURABILITY_HW_CRASH_SAFE","no");
    $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
    push(@cguids,$cguid);
    $ret = OpenContainer($node->conn(0),"ctr-1","ZS_CTNR_CREATE",1048576,4,"ZS_DURABILITY_HW_CRASH_SAFE","no");
    $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
    push(@cguids,$cguid);
    $ret = OpenContainer($node->conn(0),"ctr-2","ZS_CTNR_CREATE",1048576,4,"ZS_DURABILITY_HW_CRASH_SAFE","no");
    $cguid = $1 if($ret =~ /OK cguid=(\d+)/);   
    push(@cguids,$cguid);
    foreach (@cguids){
    CloseContainer($node->conn(0),$_);
    #CloseContainer($node->conn(0),$_);
    DeleteContainer($node->conn(0),$_);
    #DeleteContainer($node->conn(0),$cguid);
    } 
    
    $ret = $node->stop();
    like($ret,qr/OK.*/,"Node Stop");
    $ret = $node->start(ZS_REFORMAT => 0);
    like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=0");
    OpenContainer($node->conn(0),"ctr-0","ZS_CTNR_CREATE",1048576,4,"ZS_DURABILITY_HW_CRASH_SAFE","no");
    
    
    $ret = $node->stop();
    like($ret,qr/OK.*/,"Node Stop");
    $ret = $node->start(ZS_REFORMAT => 0);
    like($ret,qr/OK.*/,"Node Start: REFORMAT=0");
  

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

