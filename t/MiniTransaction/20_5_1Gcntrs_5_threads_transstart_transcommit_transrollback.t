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
use Fdftest::Stress;
use Test::More tests => 193;

my $node;
my $nthread = 5;
my $ncntr = 5;
my $mode;
#my @data = ([50, 64000, 0.125], [100, 128000, 0.125], [150, 512, 0.75]);
my @data = ([64, 16000, 0.05], [74, 32000, 0.05], [84, 64000, 0.05], [94, 128000, 0.05], [104, 48,1]);

sub test_ZSWriteObject_update{
    my ($conn, $cguid, $key_offset, $val_offset, $nops, $mode) = @_;

    my $ret = ZSTransactionStart(
            $conn,
            );
    like($ret, qr/OK.*/, 'ZSTransactionStart');

    foreach my $d(@data){
        $ret = ZSWriteObject(
            $conn,
            cguid         => $cguid,
            key_offset    => $key_offset,
            key_len       => $$d[0],
            data_offset   => $val_offset,
            data_len      => $$d[1],
            nops          => $nops*$$d[2],
            flags         => "ZS_WRITE_MUST_EXIST",
            );
	like($ret, qr/OK.*/, "ZSWriteObject, update ".$nops*$$d[2]." objects on cguid=$cguid");
    }

    $ret = ZSTransactionCommit(
            $conn,
            );
    like($ret, qr/OK.*/, 'ZSTransactionCommit succeed.');

    $ret = ZSTransactionRollback(
            $conn,
            );
    chomp($ret);
    if ($mode =~ /.*mode=1.*/)
    {
        like ($ret, qr/SERVER_ERROR ZS_UNSUPPORTED_REQUEST.*/, 'Btree type container ZSTransactionRollback '.$ret);
    }
    elsif ($mode =~ /.*mode=2.*/)
    {
        like($ret, qr/SERVER_ERROR ZS_FAILURE_NO_TRANS.*/, 'Hash type container:ZSTransactionRollback '.$ret);
    }
}

sub test_run {
    my $ret;
    my $cguid;
    my @cguid;
    my @update_thread;
    my $key_offset = 1000;
    my $val_offset = 100;
    my $size = 0;
    my $nobject = 10000;
    my $update = 100;
    my @prop = (["yes","yes","no","ZS_DURABILITY_HW_CRASH_SAFE","no"],);

    $ret = $node->start(
        ZS_REFORMAT  => 1,
        threads      => $nthread,
    );
    like($ret, qr/OK.*/, 'Node start');

    foreach my $p(@prop){
        @cguid = ();
	for(0 .. $ncntr -1)
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
	    like($ret, qr/OK.*/, "ZSopenContainer: cname=Tran_Cntr$_, cguid=$cguid");
	    push(@cguid, $cguid);

            foreach my $d(@data){
	        $ret = ZSWriteObject(
			    $node->conn(0),
			    cguid         => $cguid,
			    key_offset    => $key_offset,
			    key_len       => $$d[0],
			    data_offset   => $val_offset,
			    data_len      => $$d[1],
			    nops          => $nobject*$$d[2],
			    flags         => "ZS_WRITE_MUST_NOT_EXIST",
			    );
	        like($ret, qr/OK.*/, "ZSWriteObject: load ".$nobject*$$d[2]." objects to Tran_Cntr$_, cguid=$cguid");
	    }
	}

	$mode  = ZSTransactionGetMode (
                $node->conn(0),
                );
	chomp($mode);

	for(0 .. $nthread - 1)
	{
	    $update_thread[$_] = threads->create(\&test_ZSWriteObject_update, 
			    $node->conn($_), 
			    $cguid[$_], 
			    $key_offset + $_, 
			    $val_offset + $_ - 1, 
			    $update / $nthread,
                            $mode);
	}

	for(0 .. $nthread - 1)
	{
	    $update_thread[$_]->join();
	}

	my $update_per_cntr = $update / $ncntr;

	for(0 .. $ncntr - 1)
	{
            foreach my $d(@data){
                $ret = ZSReadObject(
                            $node->conn(0),
                            cguid         => $cguid[$_],
                            key_offset    => $key_offset + $_,
                            key_len       => $$d[0],
                            data_offset   => $val_offset + $_ - 1,
                            data_len      => $$d[1],
                            nops          => $update_per_cntr*$$d[2],
                            check         => "yes",
                            );
		like($ret, qr/OK.*/, "ZSReadObject: check update ".$update_per_cntr*$$d[2]." objects succeed on cguid=$cguid[$_]");
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

	for(0 .. $ncntr -1)
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
            like($ret, qr/OK.*/, "ZSopenContainer: cname=Tran_Cntr$_, cguid=$cguid");
            push(@cguid, $cguid);
	}

	for(0 .. $ncntr - 1)
	{
            foreach my $d(@data){
                $ret = ZSReadObject(
                            $node->conn(0),
                            cguid         => $cguid[$_],
                            key_offset    => $key_offset + $_,
                            key_len       => $$d[0],
                            data_offset   => $val_offset + $_ - 1,
                            data_len      => $$d[1],
                            nops          => $update_per_cntr*$$d[2],
                            check         => "yes",
                            );
                like($ret, qr/OK.*/, "ZSReadObject: check ".$update_per_cntr*$$d[2]." objects succeed after restart ZS on cguid=$cguid[$_]");
	    }

            foreach my $d(@data){
                $ret = ZSReadObject(
                            $node->conn(0),
                            cguid         => $cguid[$_],
                            key_offset    => $key_offset,
                            key_len       => $$d[0],
                            data_offset   => $val_offset,
                            data_len      => $$d[1],
                            nops          => $_,
                            check         => "yes",
                            );
                like($ret, qr/OK.*/, "ZSReadObject: check ".$_." objects succeed after restart ZS on cguid=$cguid[$_]");
            }

            foreach my $d(@data){
                my $upobjs = $update_per_cntr*$$d[2];
                $ret = ZSReadObject(
                            $node->conn(0),
                            cguid         => $cguid[$_],
                            key_offset    => $key_offset+$upobjs+$_,
                            key_len       => $$d[0],
                            data_offset   => $val_offset+$upobjs+$_ ,
                            data_len      => $$d[1],
                            nops          => $nobject*$$d[2]-$upobjs-$_,
                            check         => "yes",
                            );
                like($ret, qr/OK.*/, "ZSReadObject: check ".($nobject*$$d[2]-$upobjs-$_)." objects succeed after restart ZS on cguid=$cguid[$_]");
            }
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


