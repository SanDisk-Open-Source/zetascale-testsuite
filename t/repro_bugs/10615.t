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
    my $ret;
    my $cguid;
    my $i = 0;
    
    $ret = $node->start(
               ZS_REFORMAT  => 1,gdb_switch => 1,
           );
    like($ret, qr/OK.*/, 'Node start');
    
#    for(my $i=0; $i<=7; $i++){
    subtest "Test with ctnr c$i" => sub {

    	$ret=OpenContainer($node->conn(0), "c$i","ZS_CTNR_CREATE",1048576,4,"ZS_DURABILITY_HW_CRASH_SAFE","no");
    	$cguid = $1 if($ret =~ /OK cguid=(\d+)/);
        WriteObjects($node->conn(0),$cguid,0,25,1000,8000000,70);
        FlushContainer($node->conn(0),$cguid);
        DeleteObjects($node->conn(0),$cguid,0,25,70);
        WriteObjects($node->conn(0),$cguid,1000,50,1000,50,2097152);
        FlushContainer($node->conn(0),$cguid);
        CloseContainer($node->conn(0),$cguid);
        DeleteContainer($node->conn(0),$cguid);
    }
#    }
#    return;
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


