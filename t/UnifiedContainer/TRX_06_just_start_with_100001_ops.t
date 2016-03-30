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
# author: Jing Xu(Lee)
# email: leexu@hengtiansoft.com
# date: Jul 8, 2014
# description:
 
#!/usr/bin/perl

use strict;
use warnings;
use threads;
use Switch;
use POSIX qw(strftime);

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::Stress;
use Test::More tests => 123;

my $node;
my $nthread = 10;
my $ncntr = 10;

sub worker{
    my ($conn, $cguid, $key_offset, $key_len, $val_offset, $val_len, $nops, $type) = @_;
    my ($ret);

    $ret = ZSTransactionSetMode(
        $conn,
        mode    => (($type eq "BTREE")?1:2),
    );
    like($ret, qr/OK.*/, "ZSTransactionSetMode for $type success");

    $ret = ZSTransactionGetMode(
        $conn,
    );
    like($ret, qr/OK.*/, "ZSTransactionGetMode for $type success:".$ret);   

    $ret = ZSTransactionStart(
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
        flags         => "ZS_WRITE_MUST_EXIST",
    );
    like($ret, qr/OK.*/, "ZSWriteObject, update $nops objects on cguid=$cguid");
}

sub test_run {
    my $ret;
    my ($cguid, @cguid);
    my (@update_thread, %nhash, %chash);
    my $key_offset = 1000;
    my $key_len = 50;
    my $val_offset = 100;
    my $val_len = 50;
    my $size = 0;
    my $nobject = 100001;
    my $update = 100001 * 10;
    my @types = ("BTREE", "HASH");

    $ret = $node->start(
        ZS_REFORMAT  => 1,
        threads      => 10,
    );
    like($ret, qr/OK.*/, 'Node start');

    for(0 .. $ncntr - 1)
    {
        my $cname = 'Tran_Cntr' . "$_";
        my $type = $types[ rand(@types) ];
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
            flags            => "ZS_CTNR_CREATE",
            type             => $type,
        );
        $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
        like($ret, qr/OK.*/, "ZSopenContainer: cname=$cname, cguid=$cguid, type=$type");
        push(@cguid, $cguid);
        $nhash{$cname} = $type;
        $chash{$cguid} = $type;

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
        like($ret, qr/OK.*/, "ZSWriteObject: load $nobject objects to Tran_Cntr$_, cguid=$cguid");
    }

    for(0 .. $nthread - 1)
    {
        $update_thread[$_] = threads->create(\&worker, 
            $node->conn($_), 
            $cguid[$_], 
            $key_offset, 
            $key_len, 
            $val_offset, 
            $val_len + 10, 
            $update / $nthread,
            $chash{$cguid[$_]},
        );
    }

    for(0 .. $nthread - 1)
    {
        $update_thread[$_]->join();
    } 

    my $update_per_cntr = $update / $ncntr;
    for(0 .. $ncntr - 1)
    {
        $ret = ZSReadObject(
            $node->conn(0),
            cguid         => $cguid[$_],
            key_offset    => $key_offset,
            key_len       => $key_len,
            data_offset   => $val_offset,
            data_len      => $val_len + 10,
            nops          => $update_per_cntr,
            check         => "yes",
        );
        like($ret, qr/OK.*/, "ZSReadObject: check update $update_per_cntr objects succeed on cguid=$cguid[$_]");
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

    for(0 .. $ncntr -1)
    {
        my $cname = 'Tran_Cntr' . "$_";
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
            flags            => "ZS_CTNR_RW_MODE",
            type             => $nhash{$cname},
        );
        $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
        like($ret, qr/OK.*/, "ZSopenContainer: cname=$cname, cguid=$cguid, type=$nhash{$cname}");
        push(@cguid, $cguid);
    }

    for(0 .. $ncntr - 1)
    {
        $ret = ZSReadObject(
            $node->conn(0),
            cguid         => $cguid[$_],
            key_offset    => $key_offset,
            key_len       => $key_len,
            data_offset   => $val_offset,
            data_len      => $val_len,
            nops          => $update_per_cntr,
            check         => "yes",
        );
        like($ret, qr/OK.*/, "ZSReadObject: check rollback $update_per_cntr objects succeed after restart ZS on cguid=$cguid[$_]");
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


