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
# date: June 19, 2013
# description:

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::BasicTest;
use Test::More tests => 3;
use threads;

my $node;

sub test_run {
    my $ret;
    my $cguid;
    my $cname      = "Cntr";
    my $key_offset = 0;
    my $key_len    = 50;
    my $val_offset = $key_offset;
    my $val_len    = 1048576;
    my $size       = 0;
    my $num_objs   = 100;

    $ret = $node->start (ZS_REFORMAT => 1,);
    like ($ret, qr/OK.*/, 'Node start');

    $ret = ZSOpenContainer (
        $node->conn (0),
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
    $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
    like ($ret, qr/OK.*/, "ZSopenContainer: $cname, cguid=$cguid flags=CREATE");

    $ret = ZSMPut (
        $node->conn (0),
        cguid       => $cguid,
        key_offset  => $key_offset,
        key_len     => $key_len,
        data_offset => $val_offset,
        data_len    => $val_len,
        num_objs    => $num_objs,
        flags       => 0,
    );
    like ($ret, qr/OK.*/, "ZSMPut: load $num_objs objects keylen= $key_len datalen=$val_len to $cname, cguid=$cguid");
=cut
    $ret = ZSReadObject (
        $node->conn (0),
        cguid       => $cguid,
        key     => $key_offset,
        key_len     => $key_len,
        data_offset => $val_offset,
        data_len    => $val_len,
        nops        => $num_objs,
        check       => "yes",
        keep_read   => "yes",
    );
    like ($ret, qr/OK.*/, "ZSReadObject: cguid=$cguid nops=$num_objs");
=cut
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
    $node->set_ZS_prop (ZS_REFORMAT => 1);

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

# clean ENV
END {
    $node->clean ();
}

