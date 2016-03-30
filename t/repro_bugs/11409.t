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
use Fdftest::BasicTest;
use Test::More tests => 13;

my $node;
my $nthread = 1;
my $ncntr = 1;

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
#flags         => "ZS_WRITE_MUST_EXIST",
            flags         => 0,
            );
    like($ret, qr/OK.*/, "ZSWriteObject, update nops=$nops objects on Cntr_cguid=$cguid");

    $ret = ZSTransactionCommit(
            $conn,
            );
    chomp($ret);
    like($ret, qr/OK.*/, "ZSTransactionCommit: Update_ops > 100K, Commit will fail, expect return $ret");
}

sub test_run {
    my $ret;
    my $cguid;
    my @cguid;
    my $cname;
    my @update_thread;
    my $key_offset = 1000;
    my $key_len = 50;
    my $val_offset = 100;
    my $val_len = 50;
    my $size    = 0;
    my $nobject = 1000001;
    my $update  = 1000001;

    $ret = $node->start(
               ZS_REFORMAT  => 1,
               threads       => 5,
           );
    like($ret, qr/OK.*/, 'Node start');

    for(0 .. $ncntr - 1)
    {
            $ret = ZSOpenContainer(
                            $node->conn(0),
                            cname            => 'Tran_Cntr'.$_,
                            fifo_mode        => "no",
                            persistent       => "yes",
                            writethru        => "yes",
                            evicting         => "no",
                            size             => $size,
                            durability_level => "ZS_DURABILITY_PERIODIC",
                            async_writes     => "no",
                            num_shards       => 1,
                            flags            => "ZS_CTNR_CREATE"
                            );
            $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
            like($ret, qr/OK.*/, "ZSopenContainer:  Tran_Cntr$_, cguid=$cguid");
            push(@cguid, $cguid);
=cut
            $ret = ZSWriteObject(
                            $node->conn(0),
                            cguid         => $cguid,
                            key_offset    => $key_offset,
                            key_len       => $key_len + $_,
                            data_offset   => $val_offset,
                            data_len      => $val_len,
                            nops          => $nobject,
                            flags         => "ZS_WRITE_MUST_NOT_EXIST",
                    );
            like($ret, qr/OK.*/, "ZSWriteObject: load $nobject to Tran_Cntr$_, cguid=$cguid");
=cut            
    }

    for(0 .. $nthread - 1)
    {
        $update_thread[$_] = threads->create(\&test_ZSWriteObject_update, 
                $node->conn($_), 
                $cguid[$_], 
                $key_offset, 
                $key_len + $_, 
                $val_offset, 
                $val_len + 10, 
                $update);
    }

    for(0 .. $nthread - 1)
    {
        $update_thread[$_]->join();
    }

    for(0 .. $nthread - 1)
    {
        $ret = ZSReadObject(
                $node->conn(0),
                cguid         => $cguid[$_],
                key_offset    => $key_offset,
                key_len       => $key_len + $_,
                data_offset   => $val_offset,
                data_len      => $val_len + 10,
                nops          => $update -1,
                check         => "yes",
                );
        like($ret, qr/OK.*/, "ZSReadObject: check update 0 objects succeed on cguid=$cguid[$_]");
    }

    for(0 .. $ncntr - 1)
    {
        $ret = ZSCloseContainer(
                $node->conn(0),
                cguid      => $cguid[$_],
                );
        like($ret, qr/OK.*/, "ZSCloseContainer: cguid=$cguid[$_]");
    }

    $ret = $node->stop();
    like($ret, qr/OK.*/, 'Node stop');

    $ret = $node->start(
            ZS_REFORMAT  => 0,
            );
    like($ret, qr/OK.*/, 'Node restart');

    for(0 .. $ncntr - 1)
    {
        $ret = ZSOpenContainer(
                $node->conn(0),
                cname            => 'Tran_Cntr'.$_,
                fifo_mode        => "no",
                persistent       => "yes",
                writethru        => "yes",
                evicting         => "no",
                size             => $size,
                durability_level => "ZS_DURABILITY_PERIODIC",
                async_writes     => "no",
                num_shards       => 1,
                flags            => "ZS_CTNR_RW_MODE"
                );
        $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
        like($ret, qr/OK.*/, "ZSopenContainer:  Tran_Cntr$_, cguid=$cguid");
        push(@cguid, $cguid);

        $ret = ZSReadObject(
                $node->conn(0),
                cguid         => $cguid[$_],
                key_offset    => $key_offset,
                key_len       => $key_len + $_,
                data_offset   => $val_offset,
                data_len      => $val_len +10,
                nops          => $update - 1,
                check         => "yes",
                );
        like($ret, qr/OK.*/, "ZSReadObject: check update 0 objects succeed on cguid=$cguid");
    }

    for(0 .. $ncntr - 1)
    {
            $ret = ZSCloseContainer(
                            $node->conn(0),
                            cguid      => $cguid[$_],
                            );
            like($ret, qr/OK.*/, "ZSCloseContainer: cguid=$cguid[$_]");

            $ret = ZSDeleteContainer(
                            $node->conn(0),
                            cguid      => $cguid[$_],
                            );
            like($ret, qr/OK.*/, "ZSDeleteContainer: cguid=$cguid[$_]");
    }

    return;
}

sub test_init {
    $node = Fdftest::Node->new(
                ip     => "127.0.0.1",
                port   => "24422",
                nconn  => 5,
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


