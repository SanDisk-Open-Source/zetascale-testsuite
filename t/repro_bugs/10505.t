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

# file: 08_multi_slab_writethru_noevicting_container_recovery.pl 
# author: yiwen lu
# email: yiwenlu@hengtiansoft.com
# date: Nov 13, 2012
# description: recovery test  for one persistent container which fifo_mode=no,writethru=yes,evicting=no

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Test::More tests => 11;

my $node; 

sub test_run {

    my $ret;
    my $cguid;
    my $nops=1000;
    my @cguids;
    my @cnames;
    my $i;

    $node->set_ZS_prop(ZS_FLASH_SIZE => '12G');
    $ret = $node->start(
               ZS_REFORMAT  => 1,
           );
    like($ret, qr/OK.*/, 'Node start');

    for($i=1; $i<=10; $i++){    
        $ret = ZSOpenContainer(
                $node->conn(0), 
                cname            => "c$i",
                fifo_mode        => "no",
                persistent       => "yes",
                evicting         => "no",
                writethru        => "yes",
                async_writes     => "no",
                size             => 1048576,
                durability_level => "ZS_DURABILITY_HW_CRASH_SAFE",
                num_shards       => 1,
                flags            => "ZS_CTNR_CREATE",
                );
        $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
        like($ret, qr/OK.*/, "ZSOpenContainer canme=c$i,cguid=$cguid,fifo=no,persis=yes,evict=no,writethru=yes,size=1048576,flags=CREATE");
=disable write
        $ret = ZSWriteObject(
                $node->conn(0),
                cguid         => "$cguid",     
                key_offset    => 0, 
                key_len       => 125, 
                data_offset   => 1000, 
                data_len      => 500000, 
                nops          => "$nops",
                flags         => "ZS_WRITE_MUST_NOT_EXIST",
                );
        like($ret, qr/OK.*/, "ZSWriteObject-->cguid=$cguid,data_len=500000,nops=$nops");
=cut
#        push @cguids,$cguid;
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
    $node->set_ZS_prop(ZS_REFORMAT  => 1,ZS_FLASH_SIZE => 300);

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


