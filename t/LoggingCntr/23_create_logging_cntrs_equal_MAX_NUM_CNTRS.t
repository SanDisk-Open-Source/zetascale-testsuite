# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file: 
# author: Youyou Cai 
# email: youyoucai@hengtiansoft.com
# date: April 10, 2015
# description: 

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
#use Fdftest::BasicTest;
use Fdftest::UnifiedAPI;
#use Test::More tests =>873;
use Test::More 'no_plan';

my $node; 

sub test_run {
    my ($ret, $cguid, @cguids, $zs_max_num_lc);
    my $size = 0;
    my $ncntr = 600;
    my $cname;
    my $ctr_type = "LOGGING";
    my @prop = ([3, "no", "ZS_DURABILITY_HW_CRASH_SAFE"],);


    #change ZS_MAX_NUM_LC
    $zs_max_num_lc = $ncntr;
    $node -> set_ZS_prop(ZS_MAX_NUM_LC => $zs_max_num_lc);

    $ret = $node->start(
               ZS_REFORMAT  => 1,
           );
    like($ret, qr/OK.*/, 'Node start');


    foreach my $p(@prop){
        for(1 .. $ncntr)
        {
            $cname = "ctr-$_";
	    $ret=OpenContainer($node->conn(0), $cname, $$p[0], $size, "ZS_CTNR_CREATE", $$p[1], $$p[2], $ctr_type);
	    like($ret, qr/OK.*/, $ret);
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
    $node->set_ZS_prop(ZS_MAX_NUM_LC => 256);

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


