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
#use Test::More tests => 14;
use Test::More 'no_plan';

my $node; 


sub test_run {
    print "<<<<<< Test with async_write=no  >>>.\n";
    my $ret;
    my $cguid;
    my $i=0;
   # nops=30000000
    $ret = $node->start(
               ZS_REFORMAT  => 1,gdb_switch => 1,
           );
    like($ret, qr/OK.*/, 'Node start');

    #foreach $i(0,1,2,3){

     #   subtest "Test with ctnr c$i" => sub {

        	$ret=OpenContainer($node->conn(0), "c$i","ZS_CTNR_CREATE",0,4,"ZS_DURABILITY_SW_CRASH_SAFE","no");
        	$cguid = $1 if($ret =~ /OK cguid=(\d+)/);
            WriteReadObjects($node->conn(0),$cguid,0,250,1000,5000,300000);
       # }
   # }
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


