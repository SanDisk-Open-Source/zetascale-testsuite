# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file:
# author: YouyouCai
# email: youyoucai@hengtiansoft.com
# date: Mar 25, 2015
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
use Test::More tests => 14;

my $node; 
sub test_run {
    my ($ret, $cguid, @cguids,);
    my $zs_flash_size = "10000G";

    #set ZS_FLASH_SIZE
    $node->set_ZS_prop(ZS_FLASH_SIZE => $zs_flash_size);

    $ret = $node->start(ZS_REFORMAT  => 1,);
    like($ret, qr/OK.*/, 'Node started');

    #TEST: optimized
    my $top_optimized_cmd = "top -b -n 1|grep zs_test_engine|awk '{print \$6}'&";
    my $optimized_usage = `$top_optimized_cmd`;
    print "==== optimized_value is: $optimized_usage";
    my @optimized;
    for(my $i=0; $i<length($optimized_usage)-1; $i++){
        $optimized[$i] = substr($optimized_usage, $i, 1);
    }
    print "==== the unit of optimized_usage is: $optimized[-1]\n";
    system("pkill top");

    $ret = $node->stop();
    like($ret, qr/OK.*/,"Node Stop");


    #TEST: ZS_L1CACHE_SIZE default
    $node->set_ZS_prop(ZS_BTREE_L1CACHE_SIZE => 5884480000);

    $ret = $node->start(ZS_REFORMAT  => 1,);
    like($ret, qr/OK.*/, 'Node started');

    print "==== ZS_BTREE_L1CACHE_SIZE ====\n";
    my $top_default_l1cache_size_cmd = "top -b -n 1|grep zs_test_engine|awk '{print \$6}'&";
    my $default_l1cache_size_usage = `$top_default_l1cache_size_cmd`;
    print "==== default_l1cache_size_usage is: $default_l1cache_size_usage";
    my @default_l1cache_size;
    for(my $i=0; $i<length($default_l1cache_size_usage)-1; $i++){
        $default_l1cache_size[$i] = substr($default_l1cache_size_usage, $i, 1);
    }
    #print "==== the unit of default_l1cache_size_usage is: $default_l1cache_size[-1]\n";
    system("pkill top");
    

    #TEST: compare
    my $result_l1cache_size;
    if($default_l1cache_size[-1] eq $optimized[-1]){
        #print "==== the unit of default_l1cache_szie and optimized is equal\n";
        $result_l1cache_size = $default_l1cache_size_usage cmp $optimized_usage;
        like($result_l1cache_size, qr/^1|^0/, "the optimized configuration usage is smaller than default, ZS_L1CAHCE_SIZE OK");
    }
    elsif($default_l1cache_size[-1] lt $optimized[-1]){
        #print "==== the unit of default_l1cache_size larger than optimized\n";
        $result_l1cache_size = 1;
        like($result_l1cache_size, qr/^1/, "the optimized configuration usage is smaller than default, ZS_L1CAHCE_SIZE OK");
    }
    else{
        #print "==== the unit of default_l1cache_size smaller than optimized\n";
        $result_l1cache_size = -1;
        like($result_l1cache_size, qr/^1/, "the optimized configuration usage is smaller than default, ZS_L1CAHCE_SIZE OK");
    }

    $ret = $node->stop();
    like($ret, qr/OK.*/,"Node Stop");
    
    $node->set_ZS_prop(ZS_BTREE_L1CACHE_SIZE => 1000000000);


    #TEST: ZS_CACHE_SIZE default
    $node->set_ZS_prop(ZS_CACHE_SIZE => 1000000000);

    $ret = $node->start(ZS_REFORMAT => 1);
    like($ret, qr/OK.*/, "Node Start:ZS_REFORMAT=1");

    my $top_default_zs_cache_size_cmd = "top -b -n 1|grep zs_test_engine|awk '{print \$6}'&";
    my $default_zs_cache_size_usage = `$top_default_zs_cache_size_cmd`;
    #my $default_zs_cache_size_usage = "2.3g\n";
    print "==== default_zs_cache_size_usage is: $default_zs_cache_size_usage";
    my @default_zs_cache_size;
    for(my $i=0; $i<length($default_zs_cache_size_usage)-1; $i++){
        $default_zs_cache_size[$i] = substr($default_zs_cache_size_usage, $i, 1);
    }
    #print "==== the unit of default_zs_cache_size is: $default_zs_cache_size[-1]\n";

    system("pkill top");

    #TEST: compare
    my $result_zs_cache_size;
    if($default_zs_cache_size[-1] eq $optimized[-1]){
        #print "==== the unit of default_zs_cache_size and optimized is equal\n";
        $result_zs_cache_size = $default_zs_cache_size_usage cmp $optimized_usage;
        like($result_zs_cache_size, qr/^1|^0/, "the optimized configuration usage is smaller than default, ZS_CACHE_SIZE OK");
    }
    elsif($default_zs_cache_size[-1] lt $optimized[-1]){
        #print "==== the unit of default_zs_cache_size is larger than optimized\n";
        $result_zs_cache_size = 1;
        like($result_zs_cache_size, qr/^1/, "the optimized configuration usage is smaller than default, ZS_CACHE_SIZE OK");
    }
    else{
        #print "==== the unit of default_zs_cache_size is smaller than optimized\n";
        $result_zs_cache_size = -1;
        like($result_zs_cache_size, qr/^1/, "the optimized configuration usage is smaller than default, ZS_CACHE_SIZE OK");
    }

    $ret = $node->stop();
    like($ret, qr/OK.*/,"Node Stop");

    $node->set_ZS_prop(ZS_CACHE_SIZE => 17000000);


    #TEST: ZS_BTREE_OVERAGE default
    $node->set_ZS_prop(ZS_BTREE_OVERAGE => 1.01);

    $ret = $node->start(ZS_REFORMAT => 1);
    like($ret, qr/OK.*/, "Node Start:ZS_REFORMAT=1");

    my $top_default_zs_btree_overage_cmd = "top -b -n 1|grep zs_test_engine|awk '{print \$6}'&";
    my $default_zs_btree_overage_usage = `$top_default_zs_btree_overage_cmd`;
    print "==== default_zs_btree_overage_usage is: $default_zs_btree_overage_usage";
    my @default_zs_btree_overage;
    for(my $i=0; $i<length($default_zs_btree_overage_usage)-1; $i++){
        $default_zs_btree_overage[$i] = substr($default_zs_btree_overage_usage, $i, 1);
    }
    
    system("pkill top");

    #TEST: compare
    my $result_zs_btree_overage;
    if($default_zs_btree_overage[-1] eq $optimized[-1]){
        $result_zs_btree_overage = $default_zs_btree_overage_usage cmp $optimized_usage;
        like($result_zs_btree_overage, qr/^1|^0/, "the optimized configuration usage is smaller than default, ZS_BTREE_OVERAGE OK");
    }
    elsif($default_zs_btree_overage[-1] lt $optimized[-1]){
        $result_zs_btree_overage = 1;
        like($result_zs_btree_overage, qr/^1/, "the optimized configuration usage is smaller than default, ZS_BTREE_OVERAGE OK");
    }
    else{
        $result_zs_btree_overage = -1;
        like($result_zs_btree_overage, qr/^1/, "the optimized configuration usage is smaller than default, ZS_BTREE_OVERAGE OK");
    }

    $ret = $node->stop();
    like($ret, qr/OK.*/,"Node Stop");
   
    $node->set_ZS_prop(ZS_BTREE_OVERAGE => 0.9);

    
    #TEST: ZS_BTREE_MAX_NODES_PER_OBJECT default
    $node->set_ZS_prop(ZS_BTREE_MAX_NODES_PER_OBJECT => 67000);

    $ret = $node->start(ZS_REFORMAT => 1);
    like($ret, qr/OK.*/, "Node Start:ZS_REFORMAT=1");

    my $top_default_max_nodes_per_objs_cmd = "top -b -n 1|grep zs_test_engine|awk '{print \$6}'&";
    my $default_zs_max_nodes_per_objs_usage = `$top_default_max_nodes_per_objs_cmd`;
    print "==== default_zs_max_nodes_per_objects_usage is: $default_zs_max_nodes_per_objs_usage";
    my @default_zs_max_nodes_per_objs;
    for(my $i=0; $i<length($default_zs_max_nodes_per_objs_usage)-1; $i++){
        $default_zs_max_nodes_per_objs[$i] = substr($default_zs_max_nodes_per_objs_usage, $i, 1);
    }

    system("pkill top");

    #TEST:compare
    my $result_max_nodes_per_objs;
    if($default_zs_max_nodes_per_objs[-1] eq $optimized[-1]){
        $result_max_nodes_per_objs = $default_zs_max_nodes_per_objs_usage cmp $optimized_usage;
        like($result_max_nodes_per_objs, qr/^1|^0/, "the optimized configuration usage is smaller than default, ZS_BTREE_MAX_NODES_PER_OBJECT OK");
    }
    elsif($default_zs_max_nodes_per_objs[-1] lt $optimized[-1]){
        $result_max_nodes_per_objs = 1;
        like($result_max_nodes_per_objs, qr/^1/, "the optimized configuration usage is smaller than default, ZS_BTREE_MAX_NODES_PER_OBJECT OK");
    }
    else{
        $result_max_nodes_per_objs = -1;
        like($result_max_nodes_per_objs, qr/^1/, "the optimized configuration usage is smaller than default, ZS_BTREE_MAX_NODES_PER_OBJECT OK");
    }

    $ret = $node->stop();
    like($ret, qr/OK.*/,"Node Stop");

    $node->set_ZS_prop(ZS_BTREE_MAX_NODES_PER_OBJECT => 6700);


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
                
