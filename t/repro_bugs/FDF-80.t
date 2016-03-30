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
use Test::More tests => 9;

my $node; 

sub test_run {

    my $ret;
    my $cguid;
##########################################
#         Asynce_write=yes               #
##########################################
    print "<<<<<< Test with async_write=yes >>>.\n";
    $ret = $node->start(
               ZS_REFORMAT  => 1,
           );
    like($ret, qr/OK.*/, 'Node start');

    for(my $i=0; $i<=7; $i++){

        subtest "Test with ctnr c$i" => sub {

        	$ret=OpenContainer($node->conn(0), "$i","ZS_CTNR_CREATE",1048576,$i,"ZS_DURABILITY_PERIODIC","no");
        	$cguid = $1 if($ret =~ /OK cguid=(\d+)/);

        	$ret=OpenContainer($node->conn(0), "$i","ZS_CTNR_RW_MODE",1048576,$i,"ZS_DURABILITY_PERIODIC","yes");
        	$cguid = $1 if($ret =~ /OK cguid=(\d+)/);
        }
    }
    return;
}


sub test_run_2 {

    my $ret;
    my $cguid;
##########################################
#         Asynce_write=no                # 
##########################################
    print "<<<<<< Test with async_write=yes >>>.\n";
    $ret = $node->start(
               ZS_REFORMAT  => 1,
           );
    like($ret, qr/OK.*/, 'Node start');

    for(my $i=0; $i<=7; $i++){

        subtest "Test with ctnr c$i" => sub {

        	$ret=OpenContainer($node->conn(0), "c$i","ZS_CTNR_CREATE",1048576,$i,"ZS_DURABILITY_PERIODIC","no");
        	$cguid = $1 if($ret =~ /OK cguid=(\d+)/);

        	$ret=OpenContainer($node->conn(0), "c$i","ZS_CTNR_RW_MODE",1048576,$i,"ZS_DURABILITY_PERIODIC","no");
        	$cguid = $1 if($ret =~ /OK cguid=(\d+)/);
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

