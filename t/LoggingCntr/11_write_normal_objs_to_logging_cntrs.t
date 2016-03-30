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
# author: Youyou Cai 
# email: youyoucai@hengtiansoft.com
# date: April 10, 2015
# description: 

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::BasicTest;
use Fdftest::UnifiedAPI;
use Test::More tests =>81;

my $node; 

sub test_run {
    my ($ret, $cguid,);
    my $size = 0;
    my $ncntr = 20;
    my $cname;
    my $ctr_type = "LOGGING";
    my $valoff = 1000;
    my $nobject = 5000;
    my @prop = ([3, "no", "ZS_DURABILITY_HW_CRASH_SAFE"],);
    #my @data = ([50, "aa", "bbb", 160, 0.05], [100, "cdefm", "z", 320, 0.05], [150, "hhg", "os", 640, 0.05], [200, "w", "n", 1280, 1]);
    #counter, pg, osd, vallen, nops 
    my @data = ([50, 64000, 6250], [100, 128000, 6250], [150, 512, 37500]);
    #my @data = ([64, 160, 3000], [64, 320, 3000], [64, 640, 3000], [64, 1280, 3000], [64, 48, 60000]);

    $ret = $node->start(
               ZS_REFORMAT  => 1,
           );
    like($ret, qr/OK.*/, 'Node start');


    foreach my $p(@prop){
        for(1 .. $ncntr)
        {
            $cname = "ctr-$_";
	    $ret=OpenContainer($node->conn(0), $cname, $$p[0], $size, "ZS_CTNR_CREATE", $$p[1], $$p[2], $ctr_type);
	    $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
	    like($ret, qr/OK.*/, $ret);

            foreach my $d(@data){
                #$ret = WriteLogObjects($node->conn(0), $cguid, $$d[0]+$nobject*$$d[4], $$d[1], $$d[2], $valoff+$nobject*$$d[4], $$d[3], $nobject*$$d[4], 0);
                $ret = WriteObjects($node->conn(0), $cguid, 0, $$d[0], 1000, $$d[1], $$d[2], "ZS_WRITE_MUST_NOT_EXIST");
                like($ret, qr/SERVER_ERROR ZS_BAD_KEY.*/, $ret);
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


