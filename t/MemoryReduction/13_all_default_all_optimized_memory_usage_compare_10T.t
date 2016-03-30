# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file:
# author: YouyouCai
# email: youyoucai@hengtiansoft.com
# date: Mar 31, 2015
# description:

#!/usr/bin/perl

use strict;
use warnings;
use threads;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::Stress;
use Test::More 'no_plan';

my $node; 
sub test_run {
    my ($ret, $cguid, @cguids,);
    my $zs_flash_size = "10000G";


    #set ZS_FLASH_SIZE to 128G
    $node -> set_ZS_prop(ZS_FLASH_SIZE => $zs_flash_size);

    #TEST: all_default
    $node -> set_ZS_prop(ZS_BTREE_L1CACHE_SIZE => 5884480000);
    $node -> set_ZS_prop(ZS_CACHE_SIZE => 1000000000);
    $node -> set_ZS_prop(ZS_BTREE_OVERAGE => 1.01);
    $node -> set_ZS_prop(ZS_BTREE_MAX_NODES_PER_OBJECT => 67000);

    $ret = $node->start(ZS_REFORMAT  => 1,);
    like($ret, qr/OK.*/, 'Node started');

    my $top_default_cmd = "top -b -n 1|grep zs_test_engine |awk '{print \$6}'&";
    my $all_default_usage = `$top_default_cmd`;
    #my $all_default_usage = "2.3m\n";
    my @all_default;
    print "==== all_default_usage is: $all_default_usage";
    for(my $i=0; $i<length($all_default_usage)-1; $i++){
        $all_default[$i] = substr($all_default_usage,$i,1 );
    }
    #print "==== the unit of all_default_usage is: $all_default[-1]\n";
    system("pkill top");

    $ret = $node->stop();
    like($ret, qr/OK.*/,"Node Stop");

    $node -> set_ZS_prop(ZS_BTREE_L1CACHE_SIZE => 1000000000);
    $node -> set_ZS_prop(ZS_CACHE_SIZE => 17000000);
    $node -> set_ZS_prop(ZS_BTREE_OVERAGE => 0.9);
    $node -> set_ZS_prop(ZS_BTREE_MAX_NODES_PER_OBJECT => 6700);



    $ret = $node->start(ZS_REFORMAT => 1,);
    like($ret, qr/OK.*/, 'Node Started');


    #TEST: all_optimized
    my $top_optimized_cmd = "top -b -n 1|grep zs_test_engine |awk '{print \$6}'&";
    my $all_optimized_usage = `$top_optimized_cmd`;
    my @all_optimized;
    print "==== all_optimized_usage is: $all_optimized_usage";
    for(my $i=0;$i< length($all_optimized_usage)-1; $i++){
        $all_optimized[$i] = substr($all_optimized_usage,$i,1);
    }
    #print "==== the unit of all_optimized is: $all_optimized[-1]\n";
    system("pkill top");

    my $result;

    #TEST: Compare
    if($all_default[-1] eq $all_optimized[-1]){
        $result = $all_default_usage cmp $all_optimized_usage;
        #print "==== the unit of default and optimized is equal\n";
        like($result, qr/^0|^1/, "the memory usage of optimized prop is smaller than default,OK"); 
    }
    elsif($all_default[-1] lt $all_optimized[-1]){
        $result = 1;
        print "==== all_default unit is larger than all_optimized\n";
        like($result, qr/^1/, "the memory usage of optimized prop is smaller than default,OK");
     }
    else{
        $result = -1;
        #print "==== all_default unit is smaller than all_optimized\n";
        like($result, qr/^1|^0/, "the memory usage of optimized prop is smaller than default, OK");
    }
}
 

sub test_init {
    $node = Fdftest::Node->new(
        ip     => "127.0.0.1",
        port   => "24422",
        nconn  => 10,
    );
}

sub test_clean {
    $node->stop();
    $node->set_ZS_prop(ZS_REFORMAT  => 1);
    $node->set_ZS_prop(ZS_FLASH_SIZE => "128G");
    

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
                
