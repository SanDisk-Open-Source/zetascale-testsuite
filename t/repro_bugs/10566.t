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
use Test::More tests => 28;

my $node;
my $run_time_without_gc;
my $run_time_with_gc;

sub test_ZSWriteObject{
    my ($conn, $cguid, $key_offset, $key_len, $val_offset, $val_len, $nops) = @_;

    my $ret = ZSWriteObject(
            $conn,
            cguid         => $cguid,
            key_offset    => $key_offset,
            key_len       => $key_len,
            data_offset   => $val_offset,
            data_len      => $val_len,
            nops          => $nops,
            flags         => "ZS_WRITE_MUST_NOT_EXIST"
            );
    like($ret, qr/OK.*/, "ZSWriteObject: write $nops objects on cguid=$cguid.");
}

sub test_ZSDeleteObject{
    my ($conn, $cguid, $key_offset, $key_len, $nops, $nsegment) = @_;

    for(0 .. $nsegment)
    {
        my $ret = ZSDeleteObject(
                $conn,
                cguid         => $cguid,
                key_offset    => $key_offset + 32 * 1024 * $_,
                key_len       => $key_len,
                nops          => $nops
                );

        my $start_key_offset = $key_offset + 32 * 1024 * $_;
        my $end_key_offset = $start_key_offset + $nops;

        like($ret, qr/OK.*/, "ZSDeleteObject: delete $nops objects on cguid=$cguid, the range of key_offset from $start_key_offset to $end_key_offset.");
    }
}

sub test_run_pure_set {
    my $ret;
    my $cguid;
    my $cname = "Cntr1";
    my $key_offset = 1000;
    my $key_len = 50;
    my $val_offset = 500;
    my $val_len = 900;
    my $size = 1024 * 10240;
    my $nobject = 500000;
    my $start_time;
    my $stop_time;

    $ret = $node->start(
            ZS_REFORMAT  => 1,
            );
    like($ret, qr/OK.*/, 'Node start');

    $ret = ZSOpenContainer(
            $node->conn(0),
            cname            => $cname,
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
    like($ret, qr/OK.*/, "ZSopenContainer: $cname, cguid=$cguid");

    print "Warmup for GC_impact.\n";
    $ret = ZSWriteObject(
            $node->conn(0),
            cguid         => $cguid,
            key_offset    => $key_offset,
            key_len       => $key_len,
            data_offset   => $val_offset,
            data_len      => $val_len,
            nops          => $nobject,
            flags         => "ZS_WRITE_MUST_NOT_EXIST",
            );
    like($ret, qr/OK.*/, "ZSWriteObject: load $nobject objects to $cname, cguid=$cguid");

    $start_time = time();

    my $write_thread = threads->create(\&test_ZSWriteObject,
            $node->conn(0),
            $cguid,
            $key_offset + $nobject,
            $key_len,
            $val_offset,
            $val_len * 2,
            $nobject);
    
    $write_thread->join();

    $stop_time = time();

    $run_time_without_gc = $stop_time - $start_time;
    print "Running time is ", $run_time_without_gc, " secs.\n";
    print "TPS is ", int($nobject/$run_time_without_gc), "\n";

    $ret = ZSCloseContainer(
            $node->conn(0),
            cguid      => $cguid,
            );
    like($ret, qr/OK.*/, "ZSCloseContainer: cguid=$cguid");

    $ret = ZSDeleteContainer(
            $node->conn(0),
            cguid      => $cguid,
            );
    like($ret, qr/OK.*/, "ZSDeleteContainer: cguid=$cguid");

    return;
}

sub test_run_without_gc {
    my $ret;
    my $cguid;
    my $cname = "Cntr_without_GC";
    my $key_offset = 1000;
    my $key_len = 50;
    my $val_offset = 500;
    my $val_len = 900;
    my $size = 1024 * 1024 * 10;
    my $nobject = 500000;
    my $start_time;
    my $stop_time;
    my $nsegment = int($nobject * 1024 / 32 / 1024 / 1024);
    print "The number of segment is ", $nsegment, "\n";

    $ret = $node->start(
            ZS_REFORMAT  => 1,
            );
    like($ret, qr/OK.*/, 'Node start with ZS_SLAB_GC = off');

    $ret = ZSOpenContainer(
            $node->conn(0),
            cname            => $cname,
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
    like($ret, qr/OK.*/, "ZSopenContainer: $cname, cguid=$cguid");

    print "Warmup for GC_impact.\n";
    $ret = ZSWriteObject(
            $node->conn(0),
            cguid         => $cguid,
            key_offset    => $key_offset,
            key_len       => $key_len,
            data_offset   => $val_offset,
            data_len      => $val_len,
            nops          => $nobject,
            flags         => "ZS_WRITE_MUST_NOT_EXIST",
            );
    like($ret, qr/OK.*/, "ZSWriteObject: load $nobject objects to $cname, cguid=$cguid");
    
    $start_time = time();

    my $write_thread = threads->create(\&test_ZSWriteObject,
            $node->conn(0),
            $cguid,
            $key_offset + $nobject,
            $key_len,
            $val_offset,
            $val_len * 2,
            $nobject);
    
    my $delete_ops = int(32 * 1024 * 0.5);

    my $delete_thread = threads->create(\&test_ZSDeleteObject,
            $node->conn(1),
            $cguid,
            $key_offset,
            $key_len,
            $delete_ops,
            $nsegment);

    $write_thread->join();

    $stop_time = time();

    $delete_thread->join();

    $run_time_with_gc = $stop_time - $start_time;
    print "Running time is ", $run_time_with_gc, " secs.\n";
    print "TPS is ", int($nobject/$run_time_with_gc), "\n";

    $ret = ZSCloseContainer(
            $node->conn(0),
            cguid      => $cguid,
            );
    like($ret, qr/OK.*/, "ZSCloseContainer: cguid=$cguid");

    $ret = ZSDeleteContainer(
            $node->conn(0),
            cguid      => $cguid,
            );
    like($ret, qr/OK.*/, "ZSDeleteContainer: cguid=$cguid");

    return;
}

sub test_init {
    $node = Fdftest::Node->new(
            ip     => "127.0.0.1",
            port   => "24422",
            nconn  => 2,
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

    test_run_pure_set();

    $node->stop();
    
    $node->set_ZS_prop(ZS_SLAB_GC  => 'Off');
    test_run_without_gc();

    test_clean();
}


# clean ENV
END {
    $node->clean();
}

