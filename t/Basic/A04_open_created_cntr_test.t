# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file: 
# author: Shanshan Shen 
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
use Test::More tests => 3;

my $node; 

sub test_run {

    my $ret;
    my $cguid;
    my @prop = ([4, "ZS_DURABILITY_HW_CRASH_SAFE", "no"]);
    my $length = @prop;

    $ret = $node->start(
               ZS_REFORMAT  => 1,
           );
    like($ret, qr/OK.*/, 'Node start');

    for (my $i=0; $i < $length; $i++){
		$ret=OpenContainer($node->conn(0), "c$i","ZS_CTNR_CREATE",0,$prop[$i][0],$prop[$i][1],$prop[$i][2]);
        $cguid = $1 if($ret =~ /OK cguid=(\d+)/);

        $ret=OpenContainer($node->conn(0), "c$i","ZS_CTNR_RW_MODE",0,$prop[$i][0],$prop[$i][1],$prop[$i][2]);
        $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
	
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


