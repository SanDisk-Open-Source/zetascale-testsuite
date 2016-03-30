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
# author: shujing zhu(lisa)
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
use Test::More tests => 54;
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
    #my @prop = (["yes","yes","no","ZS_DURABILITY_HW_CRASH_SAFE","no"],["yes","yes","no","ZS_DURABILITY_HW_CRASH_SAFE","yes"],);
    #my @data = ([50, 64000, 6250], [100, 128000, 6250], [150, 512, 37500]);
    my @data = ([64, 16000, 3000], [74, 32000, 3000], [84, 64000, 3000], [94, 128000, 3000], [104, 48, 60000]);

    $ret = $node->start (ZS_REFORMAT => 1,);
    like ($ret, qr/OK.*/, 'Node start');

    foreach my $p(@prop){
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

    my $enum_num = 0;
	foreach my $d(@data){
    	$ret = ZSMPut (
                $node->conn (0),
                cguid       => $cguid,
                key_offset  => $key_offset,
                key_len     => $$d[0],
                data_offset => $val_offset,
                data_len    => $$d[1],
                num_objs    => $$d[2],
                flags       => 0,
                );
		like ($ret, qr/OK.*/, "ZSMPut: load $$d[2] objects keylen= $$d[0] datalen=$$d[1] to $cname, cguid=$cguid");

		$ret = ZSReadObject (
				$node->conn (0),
				cguid       => $cguid,
				key	    => $key_offset,
				key_len     => $$d[0],
				data_offset => $val_offset,
				data_len    => $$d[1],
				nops        => $$d[2],
				check       => "yes",
				keep_read   => "yes",
				);
		like ($ret, qr/OK.*/, "ZSReadObject: cguid=$cguid nops=$$d[2]");

		$ret = ZSMPut (
				$node->conn (0),
				cguid       => $cguid,
				key_offset  => $key_offset,
				key_len     => $$d[0],
				data_offset => $val_offset,
				data_len    => $$d[1],
				num_objs    => $$d[2],
                flags       => 0,
			    );
		like ($ret, qr/OK.*/, "ZSMPut: flags=0 load the same objs, $ret");

        $enum_num = $enum_num + $$d[2];
        $ret = ZSEnumerateContainerObjects(
                $node->conn(0),
                cguid  => "$cguid",
                );
        chomp($ret);
        like($ret, qr/OK.*/, "ZSEnumerateContainerObjects: cguid=$cguid--> ".($ret));

        $ret = ZSNextEnumeratedObject($node->conn(0) );
        like($ret, qr/OK enumerate $enum_num objects/, "ZSNextEnumeratedObject: cguid=$cguid-->".($ret));
        $ret = ZSFinishEnumeration($node->conn(0) );
        like($ret, qr/OK.*/, "ZSFinishEnumeration: cguid=$cguid-->".($ret));

		$ret = ZSMPut (
				$node->conn (0),
				cguid       => $cguid,
				key_offset  => $key_offset,
				key_len     => $$d[0],
				data_offset => $val_offset+5,
				data_len    => $$d[1],
				num_objs    => $$d[2],
                flags       => 0,
			    );
		like ($ret, qr/OK.*/, "ZSMPut: flags=0 update objs, $ret");
        
        $ret = ZSEnumerateContainerObjects(
                $node->conn(0),
                cguid  => "$cguid",
                );
        chomp($ret);
        like($ret, qr/OK.*/, "ZSEnumerateContainerObjects: cguid=$cguid--> ".($ret));

        $ret = ZSNextEnumeratedObject($node->conn(0) );
        like($ret, qr/OK enumerate $enum_num objects/, "ZSNextEnumeratedObject: cguid=$cguid-->".($ret));
        $ret = ZSFinishEnumeration($node->conn(0) );
        like($ret, qr/OK.*/, "ZSFinishEnumeration: cguid=$cguid-->".($ret));
	}
    	$ret = ZSCloseContainer ($node->conn (0), cguid => $cguid,);
    	like ($ret, qr/OK.*/, "ZSCloseContainer: cguid=$cguid");
    	$ret = ZSDeleteContainer ($node->conn (0), cguid => $cguid,);
    	like ($ret, qr/OK.*/, "ZSDeleteContainer: cguid=$cguid");
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

