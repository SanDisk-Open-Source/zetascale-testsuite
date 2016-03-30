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
# date: 
# description:

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Test::More tests => 64002;

my $node; 

sub test_run {

    my $ret;
    my $cguid;
    my $nops=10000;
    my @cguids;
    my @cnames;
    my $i;

    $ret = $node->set_ZS_prop (ZS_MAX_NUM_CONTAINERS => 64000);
    like ($ret, qr//, 'set ZS_MAX_NUM_CONTAINERS to 64K');

    $ret = $node->start(
               ZS_REFORMAT  => 1,
           );
    like($ret, qr/OK.*/, 'Node start');

    for($i=0; $i<64000; $i++){    
        $ret = ZSOpenContainer(
                $node->conn(0), 
                cname            => "c$i",
                fifo_mode        => "no",
                persistent       => "yes",
                evicting         => "no",
                writethru        => "yes",
                async_writes     => "no",
                size             => 1024,
                durability_level => "ZS_DURABILITY_HW_CRASH_SAFE",
                num_shards       => 1,
                flags            => "ZS_CTNR_CREATE",
                );
        $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
        like($ret, qr/OK.*/, "ZSOpenContainer canme=c$i,cguid=$cguid,fifo_mode=no,persistent=yes,evicting=no,writethru=yes,flags=CREATE");

        push @cguids,$cguid;
        
    }
    
#    $ret = ZSGetContainers($node->conn(0));
#    print "$ret";
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
    $node->set_ZS_prop (ZS_REFORMAT => 1, ZS_MAX_NUM_CONTAINERS => 6000);

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


