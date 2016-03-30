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
#use Test::More tests => 14;
use Test::More 'no_plan';

my $node; 

sub test_run {
    my $ret;
    my $cguid;

    $ret = $node->start(
               ZS_REFORMAT  => 1,
           );
    like($ret, qr/OK.*/, 'Node start');



        	$ret=OpenContainer($node->conn(0), "c1","ZS_CTNR_CREATE",1048576,3,"ZS_DURABILITY_PERIODIC","no");
#        	chomp($ret);
        	$cguid = $1 if($ret =~ /OK cguid=(\d+)/);
            
            $ret = ZSWriteObject(
                      $node->conn(0),
                       cguid         => "$cguid",     
                       key_offset    => 0, 
                       key_len       => 25, 
                       data_offset   => 1000, 
                       data_len      => 50, 
                       nops          => 5,
                       flags         => "ZS_WRITE_MUST_EXIST",
                   );
            like($ret, qr/SERVER_ERROR ZS_OBJECT_UNKNOWN.*/, "ZSWriteObject-->cguid=$cguid nops=5000 flags=ZS_WRITE_MUST_EXIST SERVER_ERROR ZS_OBJECT_UNKNOWN");

            CloseContainer($node->conn(0),$cguid);
            DeleteContainer($node->conn(0),$cguid);
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

    test_init();
    
    test_run_2();

    test_clean();
}


# clean ENV
END {
    $node->clean();
}


