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

# file:02_one_ulimit_cntr_with_larger_than_flashsize_object_recovery
# author: Jing Xu(Lee)
# email: leexu@hengtiansoft.com
# date: Feb 22, 2013
# description: recovery test for one unlimited container which is set with larger than flash size objects

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Test::More tests => 15;

my $node;

sub test_run {

    my $ret;
    my $cguid;
    my @prop = (["yes","no","yes","ZS_DURABILITY_HW_CRASH_SAFE","no"],);
    #my @data = ([50, 64000, 7000], [100, 128000, 7000], [150, 512, 42000]);
    my @data = ([64, 16000, 21000], [74, 32000, 21000], [84, 64000, 21000], [94, 128000, 21000], [104, 48, 420000]);

    $ret = $node->set_ZS_prop (ZS_FLASH_SIZE => 12);
    like ($ret, qr//, 'set ZS_FLASH_SIZE to 12G');

    foreach my $p(@prop){
        $ret = $node->start (gdb_switch => 1, ZS_REFORMAT => 1);
        like ($ret, qr/OK.*/, 'Node start');

        $ret = ZSOpenContainer(
            $node->conn(0),
            cname            => "cntr0",
            fifo_mode        => "no",
            persistent       => $$p[0],
            evicting         => $$p[1],
            writethru        => $$p[2],
            async_writes     => $$p[4],
            size             => 0,
            durability_level => $$p[3],
            num_shards       => 1,
            flags            => "ZS_CTNR_CREATE",
        );
        $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
        like ($ret, qr/OK.*/,
            "ZSOpenContainer cname=cntr0,cguid=$cguid,fifo_mode=no,persistent=$$p[0],evicting=$$p[1],writethru=$$p[2],flags=CREATE"
        );

        $ret = ZSGetContainerProps ($node->conn (0), cguid => "$cguid",);
        $ret =~ /.*size=(\d+) kb/;
        is ($1, 0, "ZSGetContainerProps:size=$1 kb");

        foreach my $d(@data){
            $ret = ZSWriteObject(
                $node->conn(0),
                cguid         => "$cguid",
                key_offset    => 0,
                key_len       => $$d[0],
                data_offset   => 1000,
                data_len      => $$d[1],
                nops          => $$d[2],
                flags         => "ZS_WRITE_MUST_NOT_EXIST",
            );
        }
        my $mode  = ZSTransactionGetMode (
                        $node->conn(0),
                    );
        chomp($mode);
        like('OK', qr/OK.*/, 'ZSTransactionGetMode success: '.$mode);

        if ($mode =~ /.*mode=1.*/)
        {
            like ($ret, qr/SERVER_ERROR ZS_FLASH_ENOSPC.*/, 'ZSWriteObject error for flash size is too small');
        }
        elsif ($mode =~ /.*mode=2.*/)
        {
            like ($ret, qr/SERVER_ERROR ZS_FLASH_ENOSPC.*/, 'ZSWriteObject error for flash size is too small');
        }
        
        foreach my $d(@data){
            $ret = ZSReadObject(
                $node->conn(0),
                cguid         => "$cguid",
                key_offset    => 0,
                key_len       => $$d[0],
                data_offset   => 1000,
                data_len      => $$d[1],
                nops          => $$d[2],
                check         => "yes",
                keep_read     => "yes",
            );
        }
        like ($ret, qr/SERVER_ERROR.*/, 'ZSReadObject error for flash size is too small');

        $ret = ZSFlushContainer ($node->conn (0), cguid => "$cguid",);
        like ($ret, qr/OK.*/, "ZSFlushContainer->cguid=$cguid");

        $ret = ZSCloseContainer ($node->conn (0), cguid => "$cguid",);
        like ($ret, qr/OK.*/, "ZSCloseContainer->cguid=$cguid");

        $ret = $node->stop ();
        like ($ret, qr/OK.*/, 'Node stop');

        $ret = $node->start (ZS_REFORMAT => 0,);
        like ($ret, qr/OK.*/, 'Node restart');

        $ret = ZSOpenContainer(
            $node->conn(0),
            cname            => "cntr0",
            fifo_mode        => "no",
            persistent       => $$p[0],
            evicting         => $$p[1],
            writethru        => $$p[2],
            async_writes     => $$p[4],
            size             => 0,
            durability_level => $$p[3],
            num_shards       => 1,
            flags            => "ZS_CTNR_RW_MODE",
        );
        $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
        like ($ret, qr/OK.*/, "ZSOpenContainer cguid=$cguid flags=RW_MODE");

        foreach my $d(@data){
            $ret = ZSReadObject(
                $node->conn(0),
                cguid         => "$cguid",
                key_offset    => 0,
                key_len       => $$d[0],
                data_offset   => 1000,
                data_len      => $$d[1],
                nops          => $$d[2],
                check         => "yes",
                keep_read     => "yes",
            );
        }
        like ($ret, qr/SERVER_ERROR.*/, 'ZSReadObject error for flash size is too small');

        $ret = ZSCloseContainer ($node->conn (0), cguid => "$cguid",);
        like ($ret, qr/OK.*/, "ZSCloseContainer->cguid=$cguid");
        $ret = ZSDeleteContainer ($node->conn (0), cguid => "$cguid",);
        like ($ret, qr/OK.*/, "ZSDeleteContainer->cguid=$cguid ");

        $node->stop ();
        $node->set_ZS_prop (ZS_REFORMAT => 1);
    }
    return;
}

sub test_init {
    $node = Fdftest::Node->new (
        ip    => "127.0.0.1",
        port  => "24422",
        nconn => 1,
    );

}

sub test_clean {
    $node->stop ();
    $node->set_ZS_prop (ZS_REFORMAT => 1, ZS_FLASH_SIZE => 128);

    return;
}

#
# main
#
{
    test_init ();

    test_run ();

    test_clean ();
}

