#----------------------------------------------------------------------------
# ZetaScale
# Copyright (c) 2016, SanDisk Corp. and/or all its affiliates.
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU Lesser General Public License version 2.1 as published by the Free
# Software Foundation;
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License v2.1 for more details.
#
# A copy of the GNU Lesser General Public License v2.1 is provided with this package and
# can also be found at: http:#opensource.org/licenses/LGPL-2.1
# You should have received a copy of the GNU Lesser General Public License along with
# this program; if not, write to the Free Software Foundation, Inc., 59 Temple
# Place, Suite 330, Boston, MA 02111-1307 USA.
#----------------------------------------------------------------------------

#file:
#author:Jie Wang
#email:Jie.Wang@sandisk.com
#date:Apr 1,2015
#descrption:

#!/usr/bin/perl

use strict;
use warnings;
use threads;
use threads ('exit' => 'threads_only');

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::Stress;
use Test::More "no_plan";

my $node;
my $nctr = 100;
sub worker_open{
      my ($con) = @_;
      my $ret;
      foreach(1..10)
      {
          my $cname = $con*10+$_;
          $ret = ZSOpen($node->conn($con),"ctr-$cname",4,0,"ZS_CTNR_CREATE","no","ZS_DURABILITY_HW_CRASH_SAFE","BTREE");
          if($ret =~ /^OK.*/)
          {
              like($ret,qr/^OK.*/,$ret);
          }else{
              like($ret,qr/.*/,$ret);
              }
      }
}

sub test_run {
	my $ret;
	my $cguid;
        my @threads;

	$ret = $node->start(
			ZS_REFORMAT => 1,
                        
		);

	like($ret,qr/OK.*/,'Node start');

	#TEST : Open an existed container
	print "==== Open an existed container ====\n";
	$ret = ZSOpen($node->conn(0),"ctr-0",4,0,"ZS_CTNR_CREATE","no","ZS_DURABILITY_HW_CRASH_SAFE","BTREE");
	like($ret,qr/^OK.*/,$ret);
	$cguid = $1 if($ret =~ /OK cguid=(\d+)/);
	$ret = ZSOpen($node->conn(0),"ctr-0",4,0,"ZS_CTNR_CREATE","no","ZS_DURABILITY_HW_CRASH_SAFE","BTREE");
	like($ret,qr/ZS_CONTAINER_EXISTS.*/,"ZSOpen: Open an existed container,expect report $ret ");

        #TEST : Open an not existed container
	print "==== Open an not existed container ====\n";
	$ret = ZSOpen($node->conn(0),"ctr-1",4,0,"ZS_CTNR_RW_MODE","no","ZS_DURABILITY_HW_CRASH_SAFE","BTREE");
	like($ret,qr/ZS_CONTAINER_UNKNOWN.*/,"ZSOpen:".$ret);

        #TEST :  parmeter is invalid or missing
	print "==== parmeter is invalid or missing ====\n";
	$ret = ZSOpen($node->conn(0),"ctr-0",4,0,"ZS_CTNR_RW_MODE","y","ZS_DURABILITY_HW_CRASH_SAFE","BTREE");
        like($ret,qr/CLIENT_ERROR.*/,$ret);
        
        #TEST : unlimited must be non-evicting
	print "====  unlimited must be non-evicting ====\n";
	$ret = ZSOpen($node->conn(0),"ctr-0",7,0,"ZS_CTNR_RW_MODE","no","ZS_DURABILITY_HW_CRASH_SAFE","BTREE");
        like($ret,qr/^OK.*/,$ret);

	#TEST : Close an close container
	$ret = ZSClose($node->conn(0),$cguid);
	like($ret,qr/^OK.*/,$ret);
        print "==== Close an closed container ====\n";
        $ret = ZSClose($node->conn(0),$cguid);
        like($ret,qr/ZS_FAILURE_CONTAINER_NOT_OPEN.*/,"ZSClose: Close an close container,expect report ERROR ZS_FAILURE_CONTAINER_NOT_OPEN");

        #TEST : size is greater than total storage size
        print "==== cntr size is greater than total storage size  ====\n";
        $ret = ZSOpen($node->conn(0),"ctr-2",4,3000000,"ZS_CTNR_CREATE","no","ZS_DURABILITY_HW_CRASH_SAFE","BTREE");
        like($ret,qr/ZS_FAILURE_INVALID_CONTAINER_SIZE.*/,"ZSOpen: $ret");

	#TEST : Delete an unexisted container
	$ret = ZSDelete($node->conn(0),$cguid);
	like($ret,qr/^OK.*/,$ret);
	print "==== Delete an unexisted container ====\n";
	$ret = ZSDelete($node->conn(0),$cguid);
	like($ret,qr/ZS_FAILURE_CONTAINER_NOT_FOUND.*/,"ZSDelete: Delete an unexisted container,expect report ERROR ZS_FAILURE_CONTAINER_NOT_FOUND" );

	#TEST : Open container after delete the container
	print "==== Open container after delete the container ====\n";
	$ret = ZSOpen($node->conn(0),"ctr-0",4,0,"ZS_CTNR_RW_MODE","no","ZS_DURABILITY_HW_CRASH_SAFE","BTREE");
	like($ret,qr/ZS_CONTAINER_UNKNOWN.*/,"ZSOpen: Open container after delete the container,except report $ret");

        $ret = $node->stop();
        like($ret,qr/OK.*/,"Node Stop");

        #TEST : Create maximum number of container
        my $numcntr = 11;
        $node -> set_ZS_prop(ZS_MAX_NUM_CONTAINERS => $numcntr);
        $ret = $node->start(
                        ZS_REFORMAT => 1,
               );
        like($ret,qr/OK.*/,"Node Start");
   
        foreach(0..$numcntr-1)
        {
            $ret = ZSOpen($node->conn(0),"ctr-$_",4,0,"ZS_CTNR_CREATE","no","ZS_DURABILITY_HW_CRASH_SAFE","BTREE");
            like($ret,qr/^OK.*/,$ret);
        }
        print "==== Create maximum number of container ====\n";
        $ret = ZSOpen($node->conn(0),"ctr-$numcntr",4,0,"ZS_CTNR_CREATE","no","ZS_DURABILITY_HW_CRASH_SAFE","BTREE");
        like($ret,qr/ZS_TOO_MANY_CONTAINERS.*/,"ZSOpen:Create maximum number of container,except report $ret");
        $ret = $node->stop();
        like($ret,qr/OK.*/,"Node Stop");
        $node -> set_ZS_prop(ZS_MAX_NUM_CONTAINERS => 6000);
    
       
        #TEST : storage apace lack
	print "==== objs size larger than total storage size ====\n";
        $node -> set_ZS_prop(ZS_FLASH_SIZE => 10);
        $ret = $node->start(
                        ZS_REFORMAT => 1,
     
        );
        like ($ret,qr/OK.*/,"Node Start");
        my $num = 9;
        $ret = ZSOpen($node->conn(0),"ctr-1",4,0,"ZS_CTNR_CREATE","no","ZS_DURABILITY_HW_CRASH_SAFE","BTREE");
        like($ret,qr/^OK.*/,$ret);
        $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
   
        foreach(0..$num)
        {        
            $ret = ZSSet($node->conn(0), $cguid, 10000+$_, 500+$_, 64000,10000, "ZS_WRITE_MUST_NOT_EXIST");
            if($ret =~ /OK.*/){ 
                like ($ret, qr/^OK.*/, $ret);
                }else{
		     like ($ret, qr/ZS_OUT_OF_STORAGE_SPACE.*/,"ZSWrite:write objs greater than torage space,expect report $ret");}
            $ret = ZSSet($node->conn(0), $cguid, 10001+$_, 600+$_, 128000,10000, "ZS_WRITE_MUST_NOT_EXIST");
            if($ret =~ /OK.*/){
                like ($ret, qr/^OK.*/, $ret);
                }else{
		    like ($ret, qr/ZS_OUT_OF_STORAGE_SPACE.*/,"ZSWrite:write objs greater than torage space,expect report $ret");}
            $ret = ZSSet($node->conn(0), $cguid, 10002+$_, 700+$_, 512,60000, "ZS_WRITE_MUST_NOT_EXIST");
            if($ret =~ /OK.*/){
                like ($ret, qr/^OK.*/, $ret);
                }else{
		     like ($ret, qr/ZS_OUT_OF_STORAGE_SPACE.*/,"ZSWrite:write objs greater than torage space,expect report $ret");}
        }

        $ret = $node->stop();
        like($ret,qr/OK.*/,"Node Stop");
	$node -> set_ZS_prop(ZS_FLASH_SIZE => 128 );
       
        #TEST : system is shutting down
        $ret = $node->start(
                        ZS_REFORMAT => 1,
                        threads     =>$nctr,
           );

        @threads = ();
        for (0 .. $nctr-1)
        {
           push(@threads, threads->new(\&worker_open, $_));

        }
        $ret = $node->stop();
        like($ret,qr/OK.*/,"Node Stop");
        sleep(1);
        #$node->kill();
}

sub test_init {
	$node = Fdftest::Node->new(
		ip	=> "127.0.0.1",
		port	=> "24422",
		nconn	=> $nctr,
                prop  => "$Bin/../../conf/zs.prop",

	     );
         return;
}

sub test_clean {
	$node->stop();	
	$node->set_ZS_prop(ZS_REFORMAT   => 1);

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

#clean ENV
END {
	$node->clean();
}
