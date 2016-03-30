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
# author: yiwen lu
# email: yiwenlu@hengtiansoft.com
# date: Jan 3, 2013
# description:

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::TestCase;
use Test::More tests => 14;

my $node; 
sub test_run {
    my $cguid;
    my $ret; 
    my @cguids;
    my $nctr=1;
    my $nops=2000000;
#async_writes=yes --------------------------------------------------------------------------------------------------------    
 #  print'<<< test with async_writes=yes >>>'."\n";
    $ret = $node->start(ZS_REFORMAT => 1);    
    like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=1");
    
    #OpenContainer {$conn,$ctrname,$flags,$size,$choice,$durab,$async_writes);
    
    foreach(0..$nctr-1){
        $ret = OpenContainer($node->conn(0),"ctr-$_","ZS_CTNR_CREATE",524288,4,"ZS_DURABILITY_HW_CRASH_SAFE","no");
        $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
        push @cguids,$cguid;
    }
    foreach(@cguids){
        WriteReadObjects($node->conn(0),$_,0,50,1000,1,$nops);
        FlushContainer($node->conn(0),$_);
        ContainerEnumerate($node->conn(0),$_,$nops);
        CloseContainer($node->conn(0),$_);
    }
    $ret = $node->stop();
    like($ret,qr/OK.*/,"Node Stop");
    $ret = $node->start(ZS_REFORMAT => 0);    
    like($ret,qr/OK.*/,"Node Start: REFORMAT=0");
    foreach(0..$nctr-1){
        $ret = OpenContainer($node->conn(0),"ctr-$_","ZS_CTNR_RW_MODE",524288,4,"ZS_DURABILITY_HW_CRASH_SAFE","no");
    }
    foreach(@cguids){
        ReadObjects($node->conn(0),$_,0,50,1000,1,$nops);
        CloseContainer($node->conn(0),$_);
        DeleteContainer($node->conn(0),$_);
    }
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


