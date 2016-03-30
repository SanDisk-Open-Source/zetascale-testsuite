# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file:
# author: Jing Xu(Lee)
# email: leexu@hengtiansoft.com
# date: June 23, 2013
# description:

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::BasicTest;
use Test::More tests => 516;
use threads;
use threads::shared;

my $node;
my @rets : shared;
#my @data = ([50, 64000, 1250], [100, 128000, 1250], [150, 512, 7500]);
my @data = ([64, 16000, 500], [74, 32000, 500], [84, 64000, 500], [94, 128000, 500], [104, 48, 10000]);

sub worker {
    my ($connid, $cguid, $key_offset, $val_offset, $ret) = @_;
    my $buff;

    foreach my $d(@data){
        $ret = ZSMPut (
            $node->conn ($connid),
	    cguid       => $cguid,
	    key_offset  => $key_offset + $$d[2] * ($connid - 1),
	    key_len     => $$d[0],
	    data_offset => $val_offset,
	    data_len    => $$d[1],
	    num_objs    => $$d[2],
	    flags       => "ZS_WRITE_MUST_NOT_EXIST",
        );

        #like ($rets[($connid - 1) * 2], qr/OK.*/, "ZSMPut: load $num_objs objects keylen= $key_len datalen=$val_len, cguid=$cguid");

        $buff = ($connid - 1) * 2;
	$rets[$buff] = $ret;

	$ret = ZSReadObject (
            $node->conn ($connid),
	    cguid       => $cguid,
            #key_offset  => $key_offset,
	    key	        => $key_offset + $$d[2] * ($connid - 1),
	    key_len     => $$d[0],
	    data_offset => $val_offset,
	    data_len    => $$d[1],
	    nops        => $$d[2],
	    check       => "yes",
	    keep_read   => "yes",
        );

        #like ($rets[($connid - 1) * 2 + 1], qr/OK.*/, "ZSReadObject: cguid=$cguid nops=$num_objs");

	$buff = ($connid - 1) * 2 + 1;
	$rets[$buff] = $ret;
    }
}

sub test_run {
    my ($ret, $cguid, @cguids, @threads, $connid);
    my $cname      = "Cntr";
    my $key_offset = 0;
    my $val_offset = $key_offset;
    my $size       = 0;
    my $nthread    = 10;
    my $ncntr      = 5;
    my @prop = (["yes","yes","no","ZS_DURABILITY_HW_CRASH_SAFE","no"],);

    $ret = $node->start (ZS_REFORMAT => 1, threads => 64,);
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

	for my $cntrid(1 .. $ncntr) {
            @threads = ();
	    for (1 .. $nthread) {
                $connid = $_;
		push(
                    @threads,
                    threads->new (
                        \&worker, $connid, $cguids[$cntrid - 1], $key_offset, $val_offset, $ret
                    )
		);
	    }
	    $_->join for (@threads);

	    for (1 .. $nthread) {
                foreach my $d(@data){
                    like ($rets[ ($_ - 1) * 2 ],
                        qr/OK.*/, "ZSMPut: load $$d[2] objects keylen=$$d[0] datalen=$$d[1], cguid=$cguids[$cntrid - 1]");
		    like ($rets[ ($_ - 1) * 2 + 1 ], qr/OK.*/, "ZSReadObject: cguid=$cguids[$cntrid - 1] nops=$$d[2]");
		}
	    }
	}

	for (1 .. $ncntr) {
            $ret = ZSCloseContainer ($node->conn (0), cguid => $cguids[ $_ - 1 ],);
	    like ($ret, qr/OK.*/, "ZSCloseContainer: cguid=$cguids[$_ - 1]");
	    $ret = ZSDeleteContainer ($node->conn (0), cguid => $cguids[ $_ - 1 ],);
	    like ($ret, qr/OK.*/, "ZSDeleteContainer: cguid=$cguids[$_ - 1]");
	}
    }

    return;
}

sub test_init {
    $node = Fdftest::Node->new (
        ip    => "127.0.0.1",
        port  => "24422",
        nconn => 10,
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
