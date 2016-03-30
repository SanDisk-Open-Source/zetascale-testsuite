# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file: 
# author: Shanshan shen 
# email: ssshen@hengtiansoft.com
# date: Jan 10, 2013
# description: 

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::BasicTest;
use Test::More tests => 5;

my $node; 

sub test_run {

    my $ret;
    my $cguid;
    my $i = 0;
    my @prop = ([4, "ZS_DURABILITY_HW_CRASH_SAFE", "no"],);

    $ret = $node->start(
           ZS_REFORMAT  => 1,
           );
    like($ret, qr/OK.*/, 'Node start');

    foreach my $p(@prop){
        $ret=OpenContainer($node->conn(0), "c$i","ZS_CTNR_CREATE",0,$$p[0],$$p[1],$$p[2]);
        $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
        CloseContainer($node->conn(0),$cguid);
        DeleteContainer($node->conn(0),$cguid);

        $ret = ZSDeleteContainer(
	        $node->conn(0),
            cguid      => "$cguid",
            );
        like($ret, qr/SERVER_ERROR ZS_FAILURE.*/, 'ZSDeleteContainer SERVER_ERROR ZS_FAILURE');
        $i++;
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

