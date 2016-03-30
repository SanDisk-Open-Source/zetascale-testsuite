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
use Test::More tests => 104;
use threads;
use threads::shared;

my $node;
my @rets : shared;

#my @data = ([50, 64000, 1250], [100, 128000, 1250], [150, 512, 7500]);
my @data = ([64, 16000, 500], [74, 32000, 500], [84, 64000, 500], [94, 128000, 500], [104, 48, 10000]);

sub worker {
    my ($connid, $cguid, $key_offset,$val_offset, $ret) = @_;

    my $i = 0;
    my $key_offset_nop;
    foreach my $d(@data){
    $key_offset_nop = $$d[2]*$key_offset;
    $ret = ZSMPut (
        $node->conn ($connid),
        cguid       => $cguid,
		key_offset  => $key_offset_nop,
		key_len     => $$d[0],
		data_offset => $val_offset,
		data_len    => $$d[1],
		num_objs    => $$d[2],
		flags       => "ZS_WRITE_MUST_NOT_EXIST",
    );
    #like ($ret, qr/OK.*/, "ZSMPut: load $$d[2] objects keylen= $$d[0] datalen=$$d[1], cguid=$cguid");

    my $buff = ($connid - 1) * 6 + $i;
    $rets[$buff] = $ret;

    $ret = ZSReadObject (
        $node->conn ($connid),
        cguid       => $cguid,
        #key_offset  => $key_offset,
		key		=> $key_offset_nop,
		key_len     => $$d[0],
		data_offset => $val_offset,
		data_len    => $$d[1],
		nops        => $$d[2],
		check       => "yes",
		keep_read   => "yes",
    );
    #like ($ret, qr/OK.*/, "ZSReadObject: cguid=$cguid nops=$$d[2]");

    $buff = ($connid - 1) * 6 + $i + 3;
    $rets[$buff] = $ret;

    $i++;
    } 
}

sub test_run {
    my ($ret, $cguid, @threads, $connid);
    my $cname      = "Cntr";
    my $key_offset = 0;
    my $val_offset = $key_offset;
    my $size       = 0;
    my $nthread    = 10;

    my @prop = (["yes","yes","no","ZS_DURABILITY_HW_CRASH_SAFE","no"],);

    $ret = $node->start (ZS_REFORMAT => 1, threads => 64,);
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

    @threads = ();
    for (1 .. $nthread) {
        $connid = $_;
        #$key_offset = $num_objs * ($_ - 1);
        $key_offset = $_ - 1;
        push(@threads,
            threads->new (\&worker, $connid, $cguid, $key_offset, $val_offset, $ret));
        #threads->new (\&worker, $connid, $cguid, $key_offset, $key_len, $val_offset, $val_len, $num_objs, $ret));
    }
    $_->join for (@threads);

    my $i = 0;
    my $j = 0;
    my $key_offset_nop;
    for $j(1 .. $nthread) {
        $i=0;
        foreach my $d(@data){
        $key_offset_nop = ($j -1)*$$d[2];
        like ($rets[ ($j - 1) * 6 + $i ],
            qr/OK.*/, "ZSMPut: load $$d[2] objects keylen=$$d[0] key_offset=$key_offset_nop datalen=$$d[1], cguid=$cguid");
        like ($rets[ ($j - 1) * 6 + $i + 3 ], qr/OK.*/, "ZSReadObject: cguid=$cguid nops=$$d[2]");
    
    $i++;
        }
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

