# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file:
# author: ssshen
# email: ssshen@hengtiansoft.com
# date: Mar 20, 2013
# description:

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::TestCase;
use Fdftest::BasicTest;
#use Test::More tests => 25;
use Test::More 'no_plan';
use threads;

my $node;
my @threads;
#my $connection=3;

sub worker{
    my ($conn,$cname,$choice)= @_;
    my $ret;
    $ret = OpenContainer($conn,$cname,"ZS_CTNR_CREATE",1048576,$choice,"ZS_DURABILITY_PERIODIC","yes");
    my $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
    WriteReadObjects($conn,$cguid,1000,50,1000,50,50000);
    FlushContainer($conn,$cguid);
    CloseContainer($conn,$cguid);
    DeleteContainer($conn,$cguid);
}
 
sub test_run {
    my $ret; 

    print'<<< test with async_writes=yes >>>'."\n";
    $ret = $node->start(ZS_REFORMAT => 1,gdb_switch => 1);    
    like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=1");
   
   for(my $i=0; $i<=7; $i++){
        push(@threads, threads->new(\&worker,$node->conn($i),"ctr-$i",$i));
   }
   
   $_->join for (@threads);
   print @threads;
    
   $ret = $node->stop();
   like($ret,qr/OK.*/,"Node Stop");

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


