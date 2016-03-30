# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file:
# author: yiwen lu
# email: yiwenlu@hengtiansoft.com
# date: Jan 3, 2013
# description:

#!/usr/bin/perl

use strict;
use warnings;
use Switch;
use threads;


use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::Stress;
use Test::More tests => 3844;

my $node; 
my $nconn = 128;
my $keyoffset = 1000;
my $dataoffset = 1000;
#my @data = ([50, 64000, 625], [100, 128000, 625], [150, 512, 3750]);
my @data = ([64, 16000, 300], [74, 32000, 300], [84, 64000, 300], [94, 128000, 300], [104, 48, 6000]);

sub worker_write{
    my ($con,$cguid) = @_;
    my $res;
    foreach my $d(@data){
    	$res = ZSMPut (
		$node->conn (0),
		cguid       => $cguid,
		key_offset  => $keyoffset,
		key_len     => $$d[0],
		data_offset => $dataoffset,
		data_len    => $$d[1],
		num_objs    => $$d[2],
		flags       => "ZS_WRITE_MUST_NOT_EXIST",
		);
	like ($res, qr/OK.*/, "ZSMPut: cguid=$cguid, keylen=$$d[0], datalen=$$d[1], nops=$$d[2]");
    }
}

sub worker_read{
    my ($con,$cguid) = @_;
    my $res;
    foreach my $d(@data){
    	$res = ZSReadObject (
		$node->conn (0),
		cguid       => $cguid,
		key         => $keyoffset,
		key_len     => $$d[0],
		data_offset => $dataoffset,
		data_len    => $$d[1],
		nops        => $$d[2],
		check       => "yes",
		keep_read   => "yes",
		);
	like ($res, qr/OK.*/, "ZSReadObject: cguid=$cguid nops=$$d[2]");
    }
}

sub worker_del{
    my ($con,$cguid) = @_;
    my $res;
    foreach my $d(@data){
    	$res = ZSDeleteObject (
        	$node->conn (0),
        	cguid       => $cguid,
        	key         => $keyoffset,
        	key_len     => $$d[0],
        	nops        => $$d[2],
    	);
    	like ($res, qr/OK.*/, "ZSDeleteObject: cguid=$cguid nops=$$d[2]");
    }
}

sub test_run {
    my ($ret, $cguid, @cguids, @threads);
    my $nctr = 128;
    my $size = 0;
    my @prop = ([3, "no", "ZS_DURABILITY_HW_CRASH_SAFE" ],); 


    $ret = $node->start(ZS_REFORMAT => 1);    
    like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=1");
   
    foreach my $p(@prop){
	@cguids = ();
    	foreach(0..$nctr-1)
    	{
        	$ret = ZSOpen($node->conn(0),"ctr-$_",$$p[0],$size,"ZS_CTNR_CREATE",$$p[1],$$p[2]);
        	like ($ret, qr/^OK.*/, $ret);
        	$cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
        	$cguids[$_]=$cguid;
    	}

    	@threads = ();
    	foreach(0..$nctr-1) 
    	{
        	push(@threads, threads->new (\&worker_write, $_, $cguids[$_]));
    	}
	$_->join for (@threads); 

	@threads = ();
	foreach(0..$nctr-1)
	{
		push(@threads, threads->new (\&worker_read, $_, $cguids[$_]));
	}
	$_->join for (@threads);

	@threads = ();
	foreach(0..$nctr-1)
	{
		push(@threads, threads->new (\&worker_del, $_, $cguids[$_]));
	}
	$_->join for (@threads);

	foreach(0..$nctr-1)
	{ 
		my $mode  = ZSTransactionGetMode (
				$node->conn(0),
				);
		chomp($mode);

		if ($mode =~ /.*mode=1.*/){
			$ret = ZSRangeAll($node->conn(0), $cguids[$_], 0);
			like ($ret, qr/^OK.*/, $ret);
		}elsif ($mode =~ /.*mode=2.*/){
			$ret = ZSEnumerate($node->conn(0), $cguids[$_]);
			like ($ret, qr/^OK.*/, $ret);
		}
	}

	$ret = ZSClose($node->conn(0), $cguid);
	like ($ret, qr/^OK.*/, $ret);

	$ret = $node->stop();
	like($ret,qr/OK.*/,"Node Stop");
	$ret = $node->start(ZS_REFORMAT => 0);
	like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=0");

	foreach(0..$nctr-1)
	{
		$ret = ZSOpen($node->conn(0),"ctr-$_",$$p[0],$$p[0],"ZS_CTNR_RW_MODE",$$p[1],$$p[2]);
		like ($ret, qr/^OK.*/, $ret);
	}

	@threads = ();
	foreach(0..$nctr-1)
	{
		push(@threads, threads->new (\&worker_write, $_, $cguids[$_]));
	}
	$_->join for (@threads);

	@threads = ();
	foreach(0..$nctr-1)
	{
		push(@threads, threads->new (\&worker_read, $_, $cguids[$_]));
	}
	$_->join for (@threads);

	foreach $cguid (@cguids)
	{
		$ret = ZSClose($node->conn(0), $cguid);
		like ($ret, qr/^OK.*/, $ret);
		$ret = ZSDelete($node->conn(0), $cguid);
		like ($ret, qr/^OK.*/, $ret);
	}
    } 
    return;
}

sub test_init {
    $node = Fdftest::Node->new(
                ip     => "127.0.0.1", 
                port   => "24422",
                nconn  => $nconn,
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


