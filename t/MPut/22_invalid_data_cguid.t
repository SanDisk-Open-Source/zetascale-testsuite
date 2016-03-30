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
# author: shujing zhu
# email: shujingzhu@hengtiansoft.com
# date: Jan 5, 2015
# description:

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::BasicTest;
use Test::More tests => 34;
use threads;

my $node;

sub test_run {
    my $ret;
    my $cguid;
    my $cname      = "Cntr";
    my $key_offset = 1000;
    my $val_offset = 1000;
    my $size       = 0;
    my @prop = (["yes","yes","no","ZS_DURABILITY_HW_CRASH_SAFE","no"],);
    #my @data = ([50, 64000, 6250], [100, 128000, 6250], [150, 512, 37500]);
    my @data = ([64, 16000, 3000], [74, 32000, 3000], [84, 64000, 3000], [94, 128000, 3000], [104, 48, 60000]);
    my @flags = (0,"ZS_WRITE_MUST_NOT_EXIST","ZS_WRITE_MUST_EXIST");

    $ret = $node->start (ZS_REFORMAT => 1,);
    like ($ret, qr/OK.*/, 'Node start');

    foreach my $p(@prop){
        foreach my $f(@flags){
            $ret = ZSOpenContainer (
                    $node->conn (0),
                    cname            => $cname,
                    fifo_mode        => "no",
                    persistent       => $$p[0],
                    writethru        => $$p[1],
                    evicting         => $$p[2],
                    size             => $size,
                    durability_level => $$p[3],
                    async_writes     => $$p[4],
                    num_shards       => 1,
                    flags            => "ZS_CTNR_CREATE"
                    );
            $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
            like ($ret, qr/OK.*/, "ZSopenContainer: $cname, cguid=$cguid flags=CREATE");

            $ret = ZSMPut (
                    $node->conn (0),
#cguid       => $cguid,
                    key_offset  => $key_offset,
                    key_len     => $data[0][0],
                    data_offset => $val_offset,
                    data_len    => $data[0][1],
                    num_objs    => $data[0][2],
                    flags       => $f,
                    );
            like ($ret, qr/CLIENT_ERROR.*/, "ZSMPut: without cguid,flags=$f->$ret ");

            $ret = ZSMPut (
                    $node->conn (0),
                    cguid       => -1,
                    key_offset  => $key_offset,
                    key_len     => $data[1][0],
                    data_offset => $val_offset,
                    data_len    => $data[1][1],
                    num_objs    => $data[1][2],
                    flags       => $f,
                    );
            like ($ret, qr/SERVER_ERROR.*/, "ZSMPut: with invalid cguid,flags=$f->$ret ");

            $ret = ZSMPut (
                    $node->conn (0),
                    cguid       => $cguid,
                    key_offset  => $key_offset,
                    key_len     => $data[2][0],
                    data_offset => -1,
                    data_len    => $data[2][1],
                    num_objs    => $data[2][2],
                    flags       => $f,
                    );
            like ($ret, qr/CLIENT_ERROR.*/, "ZSMPut:with data_offset =-1 ,flags=$f $ret");
            
            $ret = ZSMPut (
                    $node->conn (0),
                    cguid       => $cguid,
                    key_offset  => $key_offset,
                    key_len     => $data[2][0],
                    data_offset => 200*1024*1024+1,
                    data_len    => $data[2][1],
                    num_objs    => $data[2][2],
                    flags       => $f,
                    );
            like ($ret, qr/CLIENT_ERROR.*/, "ZSMPut:with data_offset =200*1024*1024+1 ,flags=$f $ret");
            
            $ret = ZSMPut (
                    $node->conn (0),
                    cguid       => $cguid,
                    key_offset  => $key_offset,
                    key_len     => $data[0][0],
                    data_offset => $val_offset,
#				data_len    => $data[0][1],
                    num_objs    => $data[0][2],
                    flags       => $f,
                    );
            like ($ret, qr/SERVER_ERROR.*/, "ZSMPut:without data_len ,flags=$f->$ret");
            
            $ret = ZSMPut (
                    $node->conn (0),
                    cguid       => $cguid,
                    key_offset  => $key_offset,
                    key_len     => $data[1][0],
                    data_offset => $val_offset,
                    data_len    => -1,
                    num_objs    => $data[1][2],
                    flags       => $f,
                    );
            like ($ret, qr/CLIENT_ERROR.*/, "ZSMPut:with data_len=-1 ,flags=$f $ret");
            
            $ret = ZSMPut (
                    $node->conn (0),
                    cguid       => $cguid,
                    key_offset  => $key_offset,
                    key_len     => $data[1][0],
                    data_offset => $val_offset,
                    data_len    => 30*1024*1024+1,
                    num_objs    => $data[1][2],
                    flags       => $f,
                    );
            like ($ret, qr/CLIENT_ERROR.*/, "ZSMPut:with data_len=30*1024*1024+1 ,flags=$f $ret");
            
            $ret = ZSMPut (
                    $node->conn (0),
                    cguid       => $cguid,
                    key_offset  => $key_offset,
                    key_len     => $data[2][0],
                    data_offset => $val_offset,
                    data_len    => 0,
                    num_objs    => $data[2][2],
                    flags       => $f,
                    );
            like ($ret, qr/SERVER_ERROR.*/, "ZSMPut:with data_len=0 ,flags=$f-> $ret");
        
            $ret = ZSCloseContainer ($node->conn (0), cguid => $cguid,);
            like ($ret, qr/OK.*/, "ZSCloseContainer: cguid=$cguid");
            $ret = ZSDeleteContainer ($node->conn (0), cguid => $cguid,);
            like ($ret, qr/OK.*/, "ZSDeleteContainer: cguid=$cguid");
        }
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

