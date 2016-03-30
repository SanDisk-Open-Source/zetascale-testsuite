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
use Test::More tests => 19;

my $node; 


sub test_run {
   my $ret;
   my $cguid;
   my $i = 0;
   my @prop=([4,"ZS_DURABILITY_HW_CRASH_SAFE","no"]);
   #my @data=([50,64000,30000],[100,128000,30000],[150,512,180000]);
   my @data = ([64, 16000, 15000], [74, 32000, 15000], [84, 64000, 15000], [94, 128000, 15000], [104, 48, 300000]);
    $ret = $node->start(
               ZS_REFORMAT  => 1,
           );
    like($ret, qr/OK.*/, 'Node start');
    foreach my $p(@prop){
        $ret=OpenContainer($node->conn(0), "c$i","ZS_CTNR_CREATE",0,$$p[0],$$p[1],$$p[2]);
        $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
        
        foreach my $d(@data){
            WriteReadObjects($node->conn(0),$cguid,1000,$$d[0],1000,$$d[1],$$d[2]);
            DeleteObjects($node->conn(0),$cguid,1000,$$d[0],$$d[2]);
        } 
        CloseContainer($node->conn(0),$cguid);
        DeleteContainer($node->conn(0),$cguid);

        $i++;
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


