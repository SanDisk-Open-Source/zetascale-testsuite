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

# file: t/MiniTransaction/09_5_1Gcntrs_with_workload_10_parallel_threads_10000000updates_recovery.t
# author: xiaofeng chen
# email: xiaofengchen@hengtiansoft.com
# date: Jan 28, 2013
# description:  

#!/usr/bin/perl

use strict;
use warnings;
use threads;
use Switch;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::Stress;
use Test::More tests => 9;

my $node;
my $nthread = 10;
my $ncntr = 5;

sub test_ZSWriteObject_update{
    my ($conn, $cguid, $key_offset, $key_len, $val_offset, $val_len, $nops) = @_;

    my $ret = ZSTransactionStart(
            $conn,
            );
    like($ret, qr/OK.*/, 'ZSTransactionStart');

    $ret = ZSWriteObject(
            $conn,
            cguid         => $cguid,
            key_offset    => $key_offset,
            key_len       => $key_len,
            data_offset   => $val_offset,
            data_len      => $val_len,
            nops          => $nops,
            flags         => 0,
            );

    like($ret, qr/OK.*/, "ZSWriteObject: update $nops objects on cguid=$cguid will failed after kill the node.");
}

sub test_run {
    my $ret;
    my $cguid;
    my @cguid;
    my $cname;
    my @update_thread;
    my $key_offset = 1000;
    my $key_len = 50;
    my $val_offset = 500;
    my $val_len = 50;
    my $size = 1024 * 1024;
    my $nobject = 100;
    #my $update = 50000;
    my $update = 100;
    $ret = $node->start(
               ZS_REFORMAT  => 1,
               threads       => 10,
           );
    like($ret, qr/OK.*/, 'Node start');

            $ret = ZSOpenContainer(
                            $node->conn(0),
                            cname            => 'Tran_Cntr0',
                            fifo_mode        => "no",
                            persistent       => "yes",
                            writethru        => "yes",
                            evicting         => "no",
                            size             => 0,
                            durability_level => "ZS_DURABILITY_HW_CRASH_SAFE",
                            async_writes     => "no",
                            num_shards       => 1,
                            flags            => "ZS_CTNR_CREATE"
                            );
            $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
            like($ret, qr/OK.*/, "ZSopenContainer: cname=Tran_Cntr0, cguid=$cguid");

            $ret = ZSWriteObject(
                            $node->conn(0),
                            cguid         => $cguid,
                            key_offset    => $key_offset,
                            key_len       => $key_len ,
                            data_offset   => $val_offset,
                            data_len      => $val_len,
                            nops          => $nobject,
                            flags         => "ZS_WRITE_MUST_NOT_EXIST",
                            );
            like($ret, qr/OK.*/, "ZSWriteObject: load $nobject objects to Tran_Cntr0, cguid=$cguid");

	    threads->new(\&test_ZSWriteObject_update,
			    $node->conn(0), 
			    $cguid, 
			    $key_offset, 
			    $key_len + 20 , 
			    $val_offset , 
			    $val_len + 10, 
			    $update);

    system("sleep 20");
    $node->kill();

    $ret = $node->start(
               ZS_REFORMAT  => 0,
           );
    like($ret, qr/OK.*/, 'Node restart');
 
            $ret = ZSOpenContainer(
                            $node->conn(0),
                            cname            => 'Tran_Cntr0',
                            fifo_mode        => "no",
                            persistent       => "yes",
                            writethru        => "yes",
                            evicting         => "no",
                            size             => 0,
                            durability_level => "ZS_DURABILITY_HW_CRASH_SAFE",
                            async_writes     => "no",
                            num_shards       => 1,
                            flags            => "ZS_CTNR_RW_MODE"
                            );
            $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
            like($ret, qr/OK.*/, "ZSopenContainer: cname=Tran_Cntr0, cguid=$cguid");

            $ret = ZSReadObject(
                            $node->conn(0),
                            cguid         => $cguid,
                            key_offset    => $key_offset,
                            key_len       => $key_len  ,
                            data_offset   => $val_offset,
                            data_len      => $val_len ,
                            nops          => $nobject,
                            check         => "yes",
                            );
            like($ret, qr/OK.*/, "ZSReadObject $nobject objects on Tran_Cntr0, cguid=$cguid");
            $ret = ZSReadObject(
                            $node->conn(0),
                            cguid         => $cguid,
                            key_offset    => $key_offset,
                            key_len       => $key_len + 20 ,
                            data_offset   => $val_offset,
                            data_len      => $val_len+10,
                            nops          => 100,
                            check         => "yes",
			    keep_read	  => "yes",
                            );
            like($ret, qr/SERVER_ERROR.*/, "ZSReadObject 0 objects on Tran_Cntr0, cguid=$cguid");
return;
}

sub test_init {
    $node = Fdftest::Node->new(
                ip     => "127.0.0.1",
                port   => "24422",
                nconn  => 10,
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


