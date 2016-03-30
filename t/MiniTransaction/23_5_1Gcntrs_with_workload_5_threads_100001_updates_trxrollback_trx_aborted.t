# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file: 
# author: xiaofeng chen
# email: xiaofengchen@hengtiansoft.com
# date: Jan 28, 2013
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
use Fdftest::BasicTest;
use Test::More tests => 58;

my $node;
my $nthread = 5;
my $ncntr = 5;
my $mode;

sub test_ZSWriteObject_update{
    my ($conn, $cguid, $key_offset, $key_len, $val_offset, $val_len, $nops, $mode) = @_;

    my $ret = ZSTransactionStart(
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
    like($ret, qr/OK.*/, "ZSWriteObject, update nops=$nops objects on Cntr_cguid=$cguid");

    $ret = ZSTransactionRollback(
            $conn,
            );
    chomp($ret);
    if ($mode =~ /.*mode=1.*/)
    {
        like ($ret, qr/SERVER_ERROR ZS_UNSUPPORTED_REQUEST.*/, 'Btree type container not support ZSTransactionRollback '.$ret);
    }
    elsif ($mode =~ /.*mode=2.*/)
    {
	like($ret, qr/SERVER_ERROR ZS_TRANS_ABORTED/, "ZSTransactionRollback: Update_ops > 100K, Rollback will fail, expect return $ret");
    }
}

sub test_run {
    my $ret;
    my $cguid;
    my @cguid;
    my $cname;
    my @update_thread;
    my $key_offset = 1000;
    my $key_len = 50;
    my $val_offset = 100;
    my $val_len = 512;
    my $size    = 0;
    my $nobject = 100001;
    my $update  = 100001;
    my @prop = (["yes","yes","no","ZS_DURABILITY_HW_CRASH_SAFE","no"],);

    $ret = $node->start(
               ZS_REFORMAT  => 1,
               threads       => 5,
           );
    like($ret, qr/OK.*/, 'Node start');

    foreach my $p(@prop){
        @cguid = ();
	for(0 .. $ncntr - 1)
	{
            $ret = ZSOpenContainer(
                            $node->conn(0),
                            cname            => 'Tran_Cntr'.$_,
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
            $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
            like($ret, qr/OK.*/, "ZSopenContainer:  Tran_Cntr$_, cguid=$cguid");
            push(@cguid, $cguid);

            $ret = ZSWriteObject(
                            $node->conn(0),
                            cguid         => $cguid,
                            key_offset    => $key_offset,
                            key_len       => $key_len + $_,
                            data_offset   => $val_offset,
                            data_len      => $val_len,
                            nops          => $nobject,
                            flags         => "ZS_WRITE_MUST_NOT_EXIST",
                    );
            like($ret, qr/OK.*/, "ZSWriteObject: load $nobject to Tran_Cntr$_, cguid=$cguid");
	}

	my $mode  = ZSTransactionGetMode (
                  $node->conn(0),
                );
	chomp($mode);


	for(0 .. $nthread - 1)
	{
            $update_thread[$_] = threads->create(\&test_ZSWriteObject_update, 
                $node->conn($_), 
                $cguid[$_], 
                $key_offset, 
                $key_len + $_, 
                $val_offset - 1, 
                $val_len, 
                $update,
                $mode);
	}

	for(0 .. $nthread - 1)
	{
            $update_thread[$_]->join();
	}

	for(0 .. $nthread - 1)
	{
            $ret = ZSReadObject(
                $node->conn(0),
                cguid         => $cguid[$_],
                key_offset    => $key_offset,
                key_len       => $key_len + $_,
                data_offset   => $val_offset,
                data_len      => $val_len,
                nops          => $update - 1,
                check         => "yes",
                );
	    if ($mode =~ /.*mode=1.*/)
	    {
                like($ret, qr/SERVER_ERROR.*/, "ZSReadObject: check btree type container data update on cguid=$cguid[$_]");
	    }
	    elsif ($mode =~ /.*mode=2.*/)
	    {
		like($ret, qr/OK.*/, "ZSReadObject: check hash type container rollback 100000 objects succeed on cguid=$cguid[$_]");
	    }

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

	for(0 .. $ncntr - 1)
	{
            $ret = ZSOpenContainer(
                $node->conn(0),
                cname            => 'Tran_Cntr'.$_,
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
	    $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
	    like($ret, qr/OK.*/, "ZSopenContainer:  Tran_Cntr$_, cguid=$cguid");
	    push(@cguid, $cguid);

	    $ret = ZSReadObject(
                $node->conn(0),
                cguid         => $cguid[$_],
                key_offset    => $key_offset,
                key_len       => $key_len + $_,
                data_offset   => $val_offset,
                data_len      => $val_len,
                nops          => $update,
                check         => "yes",
                );
	    like($ret, qr/OK.*/, "ZSReadObject: check $update objects succeed on cguid=$cguid");
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
    }

    return;
}

sub test_init {
    $node = Fdftest::Node->new(
                ip     => "127.0.0.1",
                port   => "24422",
                nconn  => 5,
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


