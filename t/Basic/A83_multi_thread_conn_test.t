# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file: 
# author: Shanshan Shen
# email: ssshen@hengtiansoft.com
# date: Apr 19, 2013
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
use Fdftest::Stress;
use Test::More tests => 328;

my @threads;
my $node;
#my @data = ([50, 64000, 1250], [100, 128000, 1250], [150, 512, 7500]);
my @data = ([64, 16000, 1000], [74, 32000, 1000], [84, 64000, 1000], [94, 128000, 10], [104, 48, 20000]);

#my $ncntr = 3;


sub worker {
    my ($thread, $persist, $writethru, $evict, $durability, $async_writes) = @_;
    my $ret;
    my $cguid;
    my @cguids;
    my $size = 0;
    my $ncntr = 3;
    
    for(0 .. $ncntr -1)
    {
        $ret = ZSOpenContainer(
                $node->conn($thread),
                cname            => 'Cntr_'.$thread.$_,
                fifo_mode        => "no",
                persistent       => $persist,
                writethru        => $writethru,
                evicting         => $evict,
                size             => $size,
                durability_level => $durability,
                async_writes     => $async_writes,
                num_shards       => 1,
                flags            => "ZS_CTNR_CREATE"
            );
        $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
        like($ret, qr/OK.*/, "ZSopenContainer: cname=Cntr_$thread\_$_, cguid=$cguid");
        push(@cguids, $cguid);

        foreach my $d(@data){
            $ret = ZSWriteObject(
                    $node->conn($thread),
                    cguid         => $cguid,
                    key_offset    => 0,
                    key_len       => $$d[0],
                    data_offset   => 1000,
                    data_len      => $$d[1],
                    nops          => $$d[2],
                    flags         => "ZS_WRITE_MUST_NOT_EXIST",
                    );
            like($ret, qr/OK.*/, "ZSWriteObject: load $$d[2] objects to Cntr$_, cguid=$cguid");
        }
    }

    foreach $cguid(@cguids)
    {
        foreach my $d(@data){
            $ret = ZSReadObject(
                $node->conn($thread),
                cguid         => $cguid,
                key_offset    => 0,
                key_len       => $$d[0],
                data_offset   => 1000,
                data_len      => $$d[1],
                nops          => $$d[2],
                check         => "yes",
                );
            like($ret, qr/OK.*/, "ZSReadObject: check $$d[2] objects succeed on cguid=$cguid");
        }
    }

    foreach(@cguids)
    {
        $ret = ZSCloseContainer(
                $node->conn($thread),
                cguid      => $_,
            );
        like($ret, qr/OK.*/, "ZSCloseContainer: cguid=$_");

    }

    #check data after reopen the container
    for(0 .. $ncntr -1)
    {
        $ret = ZSOpenContainer(
                $node->conn($thread),
                cname            => 'Cntr_'.$thread.$_,
                fifo_mode        => "no",
                persistent       => $persist,
                writethru        => $writethru,
                evicting         => $evict,
                size             => $size,
                durability_level => $durability,
                async_writes     => $async_writes,
                num_shards       => 1,
                flags            => "ZS_CTNR_RW_MODE"
                );
        $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
        like($ret, qr/OK.*/, "ZSopenContainer: cname=Cntr_$thread\_$_, cguid=$cguid");
       # push(@cguids, $cguid);

        foreach my $d(@data){
            $ret = ZSReadObject(
                $node->conn($thread),
                cguid         => $cguid,
                key_offset    => 0,
                key_len       => $$d[0],
                data_offset   => 1000,
                data_len      => $$d[1],
                nops          => $$d[2],
                check         => "yes",
                );
        like($ret, qr/OK.*/, "ZSReadObject: check $$d[2] objects succeed on cguid=$cguid");
        }
    }

    foreach(@cguids)
    {
        $ret = ZSCloseContainer(
                $node->conn($thread),
                cguid      => $_,
                );
        like($ret, qr/OK.*/, "ZSCloseContainer: cguid=$_");
    }

    return;
}

sub worker_2 {
    my ($thread, $persist, $writethru, $evict, $durability, $async_writes) = @_;
    my $ret;
    my $cguid;   
    my @cguids;
    my $size = 0;
    my $ncntr = 3;
    for(0 .. $ncntr -1)
    {
        $ret = ZSOpenContainer(
            $node->conn($thread),
	    cname            => 'Cntr_'.$thread.$_,
	    fifo_mode        => "no",
	    persistent       => $persist,
	    writethru        => $writethru,
	    evicting         => $evict,
	    size             => $size,
	    durability_level => $durability,
	    async_writes     => $async_writes,
	    num_shards       => 1,
	    flags            => "ZS_CTNR_RW_MODE"
	);
	$cguid = $1 if($ret =~ /OK cguid=(\d+)/);
	like($ret, qr/OK.*/, "ZSopenContainer: cname=Cntr_$thread\_$_, cguid=$cguid");
	push(@cguids, $cguid);
    }

    foreach $cguid(@cguids)
    {
        foreach my $d(@data){
	    $ret = ZSReadObject(
                $node->conn($thread),
		cguid         => $cguid,
		key_offset    => 0,
		key_len       => $$d[0],
		data_offset   => 1000,
		data_len      => $$d[1],
		nops          => $$d[2],
		check         => "yes",
            );
	    like($ret, qr/OK.*/, "ZSReadObject: check $$d[2] objects succeed after restart ZS on cguid=$cguid");
        }
    }

    foreach(@cguids)
    {
	$ret = ZSCloseContainer(
            $node->conn($thread),
	    cguid      => $_,
        );
	like($ret, qr/OK.*/, "ZSCloseContainer: cguid=$_");

	$ret = ZSDeleteContainer(
            $node->conn($thread),
	    cguid      => $_,
	);
	like($ret, qr/OK.*/, "ZSDeleteContainer: cguid=$_");
    }

    return;
}

sub test_run {
    my $ret; 
    my @prop = (["yes","yes","no","ZS_DURABILITY_HW_CRASH_SAFE","no"],);
    
    foreach my $p(@prop){    
        $ret = $node->start(ZS_REFORMAT => 1,gdb_switch => 1);    
        like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=1");
   
#    foreach(0..$connection){
        @threads = ();
        for(my $i=0; $i<=3; $i++){
            push(@threads, threads->new(\&worker,$i,$$p[0],$$p[1],$$p[2],$$p[3],$$p[4]));
        }
        $_->join for (@threads);
        print @threads;
    
        $ret = $node->stop();
        like($ret, qr/OK.*/, 'Node stop');
        $ret = $node->start(ZS_REFORMAT => 0,gdb_switch => 1);    
        like($ret,qr/OK.*/,"Node Restart: ZS_REFORMAT=0");

        @threads = ();
        for(my $i=0; $i<=3; $i++){
            push(@threads, threads->new(\&worker_2,$i,$$p[0],$$p[1],$$p[2],$$p[3],$$p[4]));
	}   
        $_->join for (@threads);
        print @threads;
    
	$ret = $node->stop();
	like($ret,qr/OK.*/,"Node Stop");
    }
 
    return;
}

sub test_init {
    $node = Fdftest::Node->new(
                ip     => "127.0.0.1",
                port   => "24422",
                nconn  => 64,
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


