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
=pod
sub test_run {
##########################################
#         Asynce_write=yes               #
##########################################
    print "<<<<<< Test with async_write=yes >>>.\n";
    my $ret;
    my $cguid;

    $ret = $node->start(
               ZS_REFORMAT  => 1,
           );
    like($ret, qr/OK.*/, 'Node start');

    for(my $i=0; $i<=7; $i++){

        subtest "Test with ctnr c$i" => sub {
            for(my $j=1; $j<=10; $j++){
            	$ret=OpenContainer($node->conn(0), "c$i\_$j","ZS_CTNR_CREATE",2097152,$i,"ZS_DURABILITY_PERIODIC","yes");
            	$cguid = $1 if($ret =~ /OK cguid=(\d+)/);
                for(my $k=1; $k<=10; $k++){
                    WriteReadObjects($node->conn(0),$cguid,0,25,1000,8300000,120);
                    DeleteObjects($node->conn(0),$cguid,0,25,120);
                }
                CloseContainer($node->conn(0),$cguid);
                DeleteContainer($node->conn(0),$cguid);
            }
        }
    }
    return;
}

=cut
sub test_run_2 {
##########################################
#         Asynce_write=no                #
##########################################
    print "<<<<<< Test with async_write=no >>>.\n";
    my $ret;
    my $cguid;

    $ret = $node->start(
               ZS_REFORMAT  => 1,
           );
    like($ret, qr/OK.*/, 'Node start');

   # for(my $i=0; $i<=7; $i++){

       # subtest "Test with ctnr c$i" => sub {
            for(my $j=1; $j<=10; $j++){
            	$ret=OpenContainer($node->conn(0), "c_$j","ZS_CTNR_CREATE",0,4,"ZS_DURABILITY_HW_CRASH_SAFE","no");
            	$cguid = $1 if($ret =~ /OK cguid=(\d+)/);
                for(my $k=1; $k<=10; $k++){
                    WriteReadObjects($node->conn(0),$cguid,0,25,1000,8300000,120);
                    DeleteObjects($node->conn(0),$cguid,0,25,120);
                }
                CloseContainer($node->conn(0),$cguid);
                DeleteContainer($node->conn(0),$cguid);
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
    #test_init();
    
   # test_run();

    #test_clean();

    test_init();
    
    test_run_2();

    test_clean();
}


# clean ENV
END {
    $node->clean();
}


