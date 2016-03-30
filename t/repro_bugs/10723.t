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
use Test::More tests => 30;

my $node; 
sub test_run {
    my $cguid;
    my $ret; 
    my $keyoffset=0;
    my $keylen=50;
    my $dataoffset=1000;
    my $datalen=50;
    my $nops=50000;
    my $loops=2;
    my $choice;

    foreach my $async ("no") {
        foreach my $write ("yes") {
            print "<<< test with async_writes=$async writethru=$write >>>\n";

            $ret = $node->start(ZS_REFORMAT => 1);    
            like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=1");

            $choice = 4 if ($write eq "yes");
            $choice = 6 if ($write eq "no");

            for(my $i=0; $i<$loops; $i++){
                print "=CYCLE:$i=\n";
                GetContainers($node->conn(0),0);
                $ret = OpenContainer($node->conn(0),"ctr-1","ZS_CTNR_CREATE",10485760,$choice,"ZS_DURABILITY_HW_CRASH_SAFE","$write");
                $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
                $ret = GetContainers($node->conn(0),1);
                WriteReadObjects($node->conn(0),$cguid,$keyoffset,$keylen,$dataoffset,$datalen,$nops);
                FlushContainer($node->conn(0),$cguid);
                ContainerEnumerate($node->conn(0),$cguid,$nops);
                DeleteObjects($node->conn(0),$cguid,$keyoffset,$keylen,$nops/2);
                ContainerEnumerate($node->conn(0),$cguid,$nops/2); 
                CloseContainer($node->conn(0),$cguid);
                DeleteContainer($node->conn(0),$cguid);
                GetContainers($node->conn(0),0);
                $ret = $node->stop();
                like($ret,qr/OK.*/,"Node Stop");
                $ret = $node->start(ZS_REFORMAT => 0);    
                like($ret,qr/OK.*/,"Node Start: REFORMAT=0");
            }
            $ret = $node->stop();
            like($ret,qr/OK.*/,"Node Stop");
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


