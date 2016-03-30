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

# file:01_one_slab_writethru_noevict_ctr.t
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
use Test::More tests => 19;

my $node; 

sub test_run {

    my $ret;
    my $cguid;

    $ret = $node->start(
               ZS_REFORMAT  => 1,
           );
    like($ret, qr/OK.*/, 'Node start');
    
    $ret = ZSOpenContainer(
               $node->conn(0), 
               cname            => "demo0",
               fifo_mode        => "no",
               persistent       => "yes",
               evicting         => "no",
               writethru        => "yes",
               async_writes     => "no", 
               size             => 1048576,
               durability_level => "ZS_DURABILITY_PERIODIC",
               num_shards       => 1,
               flags            => "ZS_CTNR_CREATE",
           );
    $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
    like($ret, qr/OK.*/, "ZSOpenContainer canme=demo0,cguid=$cguid,fifo_mode=no,persistent=yes,evicting=no,writethru=yes,flags=CREATE");

    $ret = ZSWriteObject(
               $node->conn(0),
               cguid         => "$cguid",     
               key_offset    => 0, 
               key_len       => 25, 
               data_offset   => 1000, 
               data_len      => 50, 
               nops          => 1,
               flags         => "ZS_WRITE_MUST_NOT_EXIST",
           );
    like($ret, qr/OK.*/, "ZSWriteObject-->cguid=$cguid nops=1");
    
    $ret = ZSReadObject(
               $node->conn(0),
               cguid         => "$cguid",     
               key_offset    => 0, 
               key_len       => 25, 
               data_offset   => 1000, 
               data_len      => 50, 
               nops          => 1,
               check         => "yes",
               keep_read     => "yes",
           );
    like($ret, qr/OK.*/, "ZSReadObject->cguid=$cguid data_len=50 nops=1");
    
    $ret = ZSFlushContainer(
               $node->conn(0),
               cguid     => "$cguid",
           );
    like($ret, qr/OK.*/, "ZSFlushContainer->cguid=$cguid");

    $ret = ZSCloseContainer(
               $node->conn(0),
               cguid     => "$cguid",
           );
    like($ret, qr/OK.*/, "ZSCloseContainer->cguid=$cguid");
    
    $ret = $node->stop();
    like($ret, qr/OK.*/, 'Node stop');

    $ret = $node->start(
               ZS_REFORMAT  => 0,
           );
    like($ret, qr/OK.*/, 'Node restart');
    
    $ret = ZSOpenContainer(
               $node->conn(0), 
               cname            => "demo0",
               fifo_mode        => "no",
               persistent       => "yes",
               evicting         => "no",
               writethru        => "yes",
               async_writes     => "yes", 
               size             => 1048576,
               durability_level => "ZS_DURABILITY_PERIODIC",
               num_shards       => 1,
               flags            => "ZS_CTNR_RW_MODE",
           );
    $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
    like($ret, qr/OK.*/, "ZSOpenContainer cguid=$cguid flags=RW_MODE");
    
    $ret = ZSReadObject(
               $node->conn(0),
               cguid         => "$cguid",     
               key_offset    => 0, 
               key_len       => 25, 
               data_offset   => 1000, 
               data_len      => 50, 
               nops          => 1,
               check         => "yes",
               keep_read     => "yes",
           );
    like($ret, qr/OK.*/, "ZSReadObject->cguid=$cguid data_len=50 nops=1");

    $ret = ZSWriteObject(
               $node->conn(0),
               cguid         => "$cguid",     
               key_offset    => 0, 
               key_len       => 25, 
               data_offset   => 1000, 
               data_len      => 55, 
               nops          => 1,
               flags         => "ZS_WRITE_MUST_EXIST",
           );
    like($ret, qr/OK.*/, "ZSWriteObject-->cguid=$cguid update data_len=55");

    $ret = ZSReadObject(
               $node->conn(0),
               cguid         => "$cguid",     
               key_offset    => 0, 
               key_len       => 25, 
               data_offset   => 1000, 
               data_len      => 55, 
               nops          => 1,
               check         => "yes",
               keep_read     => "yes",
           );
    like($ret, qr/OK.*/, "ZSReadObject->cguid=$cguid data_len=55 nops=1");
 
    $ret = ZSFlushContainer(
               $node->conn(0),
               cguid     => "$cguid",
           );
    like($ret, qr/OK.*/, "ZSFlushContainer->cguid=$cguid");

    $ret = ZSCloseContainer(
               $node->conn(0),
               cguid     => "$cguid",
           );
    like($ret, qr/OK.*/, "ZSCloseContainer->cguid=$cguid");
    
    $ret = $node->stop();
    like($ret, qr/OK.*/, 'Node stop');

    $ret = $node->start(
               ZS_REFORMAT  => 0,
           );
    like($ret, qr/OK.*/, 'Node restart');
    
    $ret = ZSOpenContainer(
               $node->conn(0), 
               cname            => "demo0",
               fifo_mode        => "no",
               persistent       => "yes",
               evicting         => "no",
               writethru        => "yes",
               async_writes     => "yes", 
               size             => 1048576,
               durability_level => "ZS_DURABILITY_PERIODIC",
               num_shards       => 1,
               flags            => "ZS_CTNR_RW_MODE",
           );
    $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
    like($ret, qr/OK.*/, "ZSOpenContainer cguid=$cguid flags=RW_MODE");

    $ret = ZSReadObject(
               $node->conn(0),
               cguid         => "$cguid",     
               key_offset    => 0, 
               key_len       => 25, 
               data_offset   => 1000, 
               data_len      => 50, 
               nops          => 1,
               check         => "yes",
               keep_read     => "yes",
           );
    like($ret, qr/SERVER_ERROR.*/, "ZSReadObject->cguid=$cguid data_len=50 nops=1");
   
    $ret = ZSReadObject(
               $node->conn(0),
               cguid         => "$cguid",     
               key_offset    => 0, 
               key_len       => 25, 
               data_offset   => 1000, 
               data_len      => 55, 
               nops          => 1,
               check         => "yes",
               keep_read     => "yes",
           );
    like($ret, qr/OK.*/, "ZSReadObject->cguid=$cguid data_len=55 nops=1");
    
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


