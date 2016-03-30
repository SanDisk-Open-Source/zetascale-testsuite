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
#date:Dec 17,2014
#descrption:

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::Stress;
use Test::More 'no_plan';

my $node;

sub test_run {
	my $ret;
	my $cguid;
	$ret = $node->start(
			ZS_REFORMAT => 1,
		);

	like($ret,qr/OK.*/,'Node start');

	#TEST : Open an existed container
	$ret = ZSOpen($node->conn(0),"ctr-0",4,0,"ZS_CTNR_CREATE","yes","ZS_DURABILITY_SW_CRASH_SAFE","BTREE");
	like($ret,qr/^OK.*/,$ret);
	$cguid = $1 if($ret =~ /OK cguid=(\d+)/);
	$ret = ZSOpen($node->conn(0),"ctr-0",4,0,"ZS_CTNR_CREATE","yes","ZS_DURABILITY_SW_CRASH_SAFE","BTREE");
	like($ret,qr/ZS_CONTAINER_EXISTS.*/,"ZSOpen: Open an existed container,expect report ERROR ZS_CONTAINER_EXISTS");

	#TEST : Get an object before set object
	$ret = ZSGet($node->conn(0),$cguid,0,50,100,1);
	like($ret,qr/items check failed.*/,"ZSRead: Get an object before set  object,expect report ERROR items read failed and ietems check failed");
	
	#TEST : Update an object before set object
	$ret = ZSSet($node->conn(0),$cguid,0,50,100,5,"ZS_WRITE_MUST_EXIST");
	like($ret,qr/ZS_OBJECT_UNKNOWN.*/,"ZSWrite: Update an object before set,except report ERROR ZS_OBJECT_UNKNOWN");
	
	#TEST : set an existed object
	$ret = ZSSet($node->conn(0),$cguid,0,50,100,5,"ZS_WRITE_MUST_NOT_EXIST");
	like($ret,qr/^OK.*/,$ret);
	$ret = ZSSet($node->conn(0),$cguid,0,50,100,5,"ZS_WRITE_MUST_NOT_EXIST");
	like($ret,qr/ZS_OBJECT_EXISTS.*/,"ZSWrite: Set an existed object,expect report ERROR ZS_OBJECT_EXISTS");
	
	#TEST : Delete an unexisted object
	$ret = ZSDel($node->conn(0),$cguid,0,50,5);
	like($ret,qr/^OK.*/,$ret);
	$ret = ZSDel($node->conn(0),$cguid,0,50,5);
	like($ret,qr/ZS_OBJECT_UNKNOWN.*/,"ZSDeleteobj: Delete an unexisted object,expect report ERROR ZS_OBJECT_UNKNOWN");

	#TEST : Get an object after delete the object
	#$ret = ZSSet($node->conn(0),$cguid,0,50,100,5,"ZS_WRITE_MUST_EXIST");
	#like($ret,qr/^OK.*/,$ret);
	$ret = ZSSet($node->conn(0),$cguid,0,50,100,5,"ZS_WRITE_MUST_NOT_EXIST");
	like($ret,qr/^OK.*/,$ret);
	$ret = ZSDel($node->conn(0),$cguid,0,50,5);
	like($ret,qr/^OK.*/,$ret);
	$ret = ZSGet($node->conn(0),$cguid,0,50,100,5);
	like($ret,qr/items check failed.*/,"ZSRead: Get an object after delete the object,expect report ERROR items read failed and ietems check failed");

	$ret = ZSSet($node->conn(0),$cguid,0,30,100,10,"ZS_WRITE_MUST_NOT_EXIST");
	like($ret,qr/^OK.*/,$ret);

	#TEST : GetConts is not equal expect
	$ret = ZSGetConts($node->conn(0),2);
	like($ret,qr/Error.*/,"ZSGetContainers: Get container counts is not equal expect,expect report ERROR ");


	#TEST : Close an close container
	$ret = ZSClose($node->conn(0),$cguid);
	like($ret,qr/^OK.*/,$ret);
	$ret = ZSClose($node->conn(0),$cguid);
	like($ret,qr/ZS_FAILURE_CONTAINER_NOT_OPEN.*/,"ZSClose: Close an close container,expect report ERROR ZS_FAILURE_CONTAINER_NOT_OPEN");

	#TEST : Get object after container is close
	$ret = ZSGet($node->conn(0),$cguid,0,30,100,10);
	like($ret,qr/items check failed.*/,"ZSRead: Get an object after container is close,expect report ERROR items read failed and ietems check failed");

	#TEST : Set object after container is close
	$ret = ZSSet($node->conn(0),$cguid,0,50,100,5,"ZS_WRITE_MUST_NOT_EXIST");
	like($ret,qr/ZS_FAILURE_CONTAINER_NOT_OPEN.*/,"ZSWrite: Set an object after container is close,except report ERROR  ZS_FAILURE_CONTAINER_NOT_OPEN");

	#TEST : ZSDeleteRandom
	#$ret = ZSDeleteRandom($node->conn(0),$cguid,0);
	#like($ret,qr/^OK.*/,$ret);

	#TEST : CreateSnapshot after container is close
	$ret = ZSCreateSnapshot($node->conn(0),$cguid);
	like($ret,qr/ZS_FAILURE_CONTAINER_NOT_OPEN.*/,"ZSCreateContainerSnapshot: Create snapshot after container is close,excepet report ERROR ZS_FAILURE_CONTAINER_NOT_OPEN");

	#TEST : Open container when ctrname is not create
	$ret = ZSOpen($node->conn(0),"6",4,0,"ZS_CTNR_RW_MODE","yes","ZS_DURABILITY_SW_CRASH_SAFE","BTREE");
	like($ret,qr/ZS_CONTAINER_UNKNOWN.*/,"ZSOpen: Open container when ctrname is not create,except report ERROR ZS_CONTAINER_UNKNOWN");
	$ret = ZSOpen($node->conn(0),"a",4,0,"ZS_CTNR_RW_MODE","yes","ZS_DURABILITY_SW_CRASH_SAFE","BTREE");
	like($ret,qr/ZS_CONTAINER_UNKNOWN.*/,"ZSOpen: Open container when ctrname is not create,except report ERROR ZS_CONTAINER_UNKNOWN");

	#TEST : Delete an unexisted container
	$ret = ZSDelete($node->conn(0),$cguid);
	like($ret,qr/^OK.*/,$ret);
	$ret = ZSDelete($node->conn(0),$cguid);
	like($ret,qr/ZS_FAILURE_CONTAINER_NOT_FOUND.*/,"ZSDelete: Delete an unexisted container,expect report ERROR ZS_FAILURE_CONTAINER_NOT_FOUND" );

	#TEST : Open container after delete the container
	$ret = ZSOpen($node->conn(0),"ctr-0",4,0,"ZS_CTNR_RW_MODE","yes","ZS_DURABILITY_SW_CRASH_SAFE","BTREE");
	like($ret,qr/ZS_CONTAINER_UNKNOWN.*/,"ZSOpen: Open container after delete the container,except report ZS_CONTAINER_UNKNOWN");
	
	#TEST : Getprops an container after delete the container
	$ret = ZSGetProps($node->conn(0),$cguid);
	like($ret,qr/ZS_FAILURE_CONTAINER_NOT_FOUND.*/,"ZSGetContainerProps: Getprops an container,expect report ERROR ZS_FAILURE_CONTAINER_NOT_FOUND");
	
}

sub test_init {
	$node = Fdftest::Node->new(
		ip	=> "127.0.0.1",
		port	=> "24422",
		nconn	=> 1,
	     );
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
