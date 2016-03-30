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


    $ret=OpenContainer($node->conn(0), "c1","ZS_CTNR_CREATE",0,4,"ZS_DURABILITY_PERIODIC","no");
    $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
    WriteReadObjects($node->conn(0),$cguid,0,25,1000,8388608,120);
    #WriteReadObjects($node->conn(0),$cguid,0,25,1000,8388508,120);
    CloseContainer($node->conn(0),$cguid);
    DeleteContainer($node->conn(0),$cguid);
    return;
}


sub test_run_2 {
##########################################
#         Asynce_write=no               #
##########################################
    print "<<<<<< Test with async_write=no >>>.\n";
    my $ret;
    my $cguid;

    $ret = $node->start(
               ZS_REFORMAT  => 1,
           );
    like($ret, qr/OK.*/, 'Node start');

    for(my $i=0; $i<=7; $i++){

        subtest "Test with ctnr c$i" => sub {
            for(my $j=1; $j<=1; $j++){
            	$ret=OpenContainer($node->conn(0), "c$i\_$j","ZS_CTNR_CREATE",2097152,$i,"ZS_DURABILITY_PERIODIC","no");
            	$cguid = $1 if($ret =~ /OK cguid=(\d+)/);
                WriteReadObjects($node->conn(0),$cguid,0,25,1000,8388608,120);
                CloseContainer($node->conn(0),$cguid);
                DeleteContainer($node->conn(0),$cguid);
            }
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


