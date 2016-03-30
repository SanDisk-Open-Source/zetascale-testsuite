# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
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
use Test::More tests => 108;
use threads;

my $node;

sub test_run {
    my ($ret, $cguid, @cguids);
    my $cname      = "Cntr";
    my $key_offset = 0;
    my $val_offset = $key_offset;
    my $size       = 0;
    my $ncntr      = 5;
    my @prop = (["yes","yes","no","ZS_DURABILITY_HW_CRASH_SAFE","no"],);
    #my @data = ([50, 64000, 1250], [100, 128000, 1250], [150, 512, 7500]);
    my @data = ([64, 16000, 500], [74, 32000, 500], [84, 64000, 500], [94, 128000, 500], [104, 48, 10000]);

    $ret = $node->start (ZS_REFORMAT => 1,);
    like ($ret, qr/OK.*/, 'Node start');

    foreach my $p(@prop){
        @cguids = ();
        for (1 .. $ncntr) {
            $ret = ZSOpenContainer (
                $node->conn (0),
		cname            => $cname . $_,
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
	    like ($ret, qr/OK.*/, "ZSopenContainer: $cname$_, cguid=$cguid flags=CREATE");
	    push(@cguids, $cguid);
	}

	for (1 .. $ncntr) {
            foreach my $d(@data){
                $ret = ZSMPut (
                    $node->conn (0),
		    cguid       => $cguids[ $_ - 1 ],
		    key_offset  => $key_offset,
		    key_len     => $$d[0],
		    data_offset => $val_offset,
		    data_len    => $$d[1],
		    num_objs    => $$d[2],
		    flags       => "ZS_WRITE_MUST_NOT_EXIST",
                );
		like ($ret, qr/OK.*/,
                    "ZSMPut: load $$d[2] objects keylen=$$d[0] datalen=$$d[1] to $cname$_, cguid=$cguids[$_-1]");

		$ret = ZSReadObject (
                    $node->conn (0),
		    cguid       => $cguids[ $_ - 1 ],
                    #key_offset  => $key_offset,
		    key		=> $key_offset,
		    key_len     => $$d[0],
		    data_offset => $val_offset,
		    data_len    => $$d[1],
		    nops        => $$d[2],
		    check       => "yes",
		    keep_read   => "yes",
                );
		like ($ret, qr/OK.*/, "ZSReadObject: cguid=$cguids[$_-1] nops=$$d[2]");
  	    }
        }

	for (1 .. $ncntr) {
            $ret = ZSFlushContainer ($node->conn (0), cguid => "$cguids[$_-1]",);
	    like ($ret, qr/OK.*/, "ZSFlushContainer: cguid=$cguids[$_-1]");
	    $ret = ZSCloseContainer ($node->conn (0), cguid => $cguids[ $_ - 1 ],);
	    like ($ret, qr/OK.*/, "ZSCloseContainer: cguid=$cguids[$_-1]");
	}

	$ret = $node->stop ();
	like ($ret, qr/OK.*/, 'Node stop');
	$ret = $node->start (ZS_REFORMAT => 0,);
	like ($ret, qr/OK.*/, 'Node restart');

	for (1 .. $ncntr) {
            $ret = ZSOpenContainer (
                $node->conn (0),
		cname            => $cname . $_,
		fifo_mode        => "no",
		persistent       => $$p[0],
		writethru        => $$p[1],
		evicting         => $$p[2],
		size             => $size,
		durability_level => $$p[3],
		async_writes     => $$p[4],
		num_shards       => 1,
		flags            => "ZS_CTNR_RW_MODE"
            );
	    $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
	    like ($ret, qr/OK.*/, "ZSopenContainer: $cname$_, cguid=$cguid flags=RW_MODE");

            foreach my $d(@data){
	        $ret = ZSReadObject (
                    $node->conn (0),
	  	    cguid       => $cguid,
                    #key_offset  => $key_offset,
		    key	        => $key_offset,
		    key_len     => $$d[0],
		    data_offset => $val_offset,
		    data_len    => $$d[1],
		    nops        => $$d[2],
		    check       => "yes",
		    keep_read   => "yes",
                );
		like ($ret, qr/OK.*/, "ZSReadObject: cguid=$cguid nops=$$d[2]");
            }
        }

	for (1 .. $ncntr) {
            $ret = ZSCloseContainer ($node->conn (0), cguid => "$cguids[$_-1]",);
	    like ($ret, qr/OK.*/, "ZSCloseContainer: cguid=$cguids[$_-1]");
	    $ret = ZSDeleteContainer ($node->conn (0), cguid => $cguids[ $_ - 1 ],);
	    like ($ret, qr/OK.*/, "ZSDeleteContainer: cguid=$cguids[$_-1]");
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

