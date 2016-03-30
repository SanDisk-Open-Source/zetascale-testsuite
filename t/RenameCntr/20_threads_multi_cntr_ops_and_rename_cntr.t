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
# author: Runhan Mao(Monica)
# email: runhanmao@hengtiansoft.com
# date: Apr 8, 2015
# description:


#!/usr/bin/perl

use strict;
use warnings;
use threads;

use FindBin qw($Bin);
use threads::shared;
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::Stress;
use Fdftest::BasicTest;
use Test::More tests => 283;

my $node; 
my $ncntr = 10;
our @cguidss :shared;

sub worker_rename {
    my ($con,$cguid, $cname) = @_;
    my $ret;
    $ret=ZSRename($con, $cguid, "rename_$cname");
    like($ret, qr/^OK.*/, $ret);
    $ret=ZSRename($con, $cguid, "rename_$cname");
    like($ret, qr/^Error.*/, $ret);
}

sub worker_cntr_operation {
    my ($con, $cguid, $cname) = @_;
    my $size = 0;
    my @prop  = ([3,"ZS_DURABILITY_HW_CRASH_SAFE","no"]);
    my $ret;
    foreach my $p(@prop){
    	FlushContainer($con, $cguid);
    	CloseContainer($con, $cguid);
    	$ret=OpenContainer($con, "rename_$cname","ZS_CTNR_RW_MODE",$size,$$p[0],$$p[1],$$p[2]);

    	CloseContainer($con, $cguid);
    	DeleteContainer($con, $cguid);

    	$ret=OpenContainer($con, $cname,"ZS_CTNR_CREATE",$size,$$p[0],$$p[1],$$p[2]);
    	$cguid = $1 if($ret =~ /OK cguid=(\d+)/);
        push(@cguidss, $cguid);
    }
}

sub test_run {
    my ($ret, $cguid);
    my $loop = 100;
    my $size = 0;
    my $keyoff = 1000;
    my $key = 1;
    my $val_offset = $key;
    my $flags = 'ZS_RANGE_START_GE|ZS_RANGE_END_LE|ZS_RANGE_KEYS_ONLY';
    my @prop  = ([3,"ZS_DURABILITY_HW_CRASH_SAFE","no"]);
    my @data = ([64, 16000, 3000], [64, 32000, 3000], [64, 64000, 3000], [64, 128000, 3000], [64, 48, 60000]);
    my @cguids;

    $ret = $node->start(ZS_REFORMAT  => 1,);
    like($ret, qr/OK.*/, 'Node started');

    foreach my $p(@prop){
        @cguids = ();
        for(0 .. $ncntr-1){
            $ret=OpenContainer($node->conn(0), "ctr-$_","ZS_CTNR_CREATE",$size,$$p[0],$$p[1],$$p[2]);
            $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
            push(@cguids, $cguid);
            foreach my $d(@data){
                WriteReadObjects($node->conn(0), $cguid, $keyoff, $$d[0], $keyoff, $$d[1], $$d[2]);
                    $keyoff = $keyoff + $$d[0]*$$d[2];
            }
            $keyoff = 1000;
        }  

        my @threads = ();
        for(0 .. $ncntr-1){
            push(@threads, threads->new (\&worker_rename, $node->conn(2*$_+1), $cguids[$_],  "ctr-$_"));
            push(@threads, threads->new (\&worker_cntr_operation, $node->conn(2*$_), $cguids[$_], "ctr-$_"));
        }
        $_->join for (@threads);
     

        for(0 .. $ncntr-1){
            $ret = ZSClose($node->conn(0), $cguidss[$_]);
            like ($ret, qr/^OK.*/, "Close Container");
        }

        $ret = $node->stop();
        like($ret,qr/OK.*/,"Node Stop");
        $ret = $node->start(ZS_REFORMAT => 0);
        like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=0");

        for(0 .. $ncntr-1){
            $ret=OpenContainer($node->conn(0), "ctr-$_", "ZS_CTNR_RW_MODE",$size,$$p[0],$$p[1],$$p[2]);
        }


        for(0 .. $ncntr-1){
            foreach my $d(@data){
                $ret = ZSReadObject(
                        $node->conn(0),
                        cguid         => $cguidss[$_],
                        key           => $key,
                        key_len       => $$d[0],
                        data_offset   => $val_offset,
                        data_len      => $$d[1], 
                        nops          => $$d[2],
                        check         => "yes",
                        keep_read     => "yes",
                        );
                like($ret, qr/SERVER_ERROR.*/, "ZSReadObject: read $$d[2] objects keylen=$$d[0] datalen=$$d[1] from rename_b_ctr-$_, cguid=$cguidss[$_], $ret");
                $key = $key+$$d[2];
            }
            $key = 1;
        }

        for(@cguidss){
            $ret = ZSClose($node->conn(0), $_);
            like ($ret, qr/^OK.*/, $ret);
            $ret = ZSDelete($node->conn(0), $_);
            like ($ret, qr/^OK.*/, $ret);
        }   
    }
}

sub test_init {
    $node = Fdftest::Node->new(
        ip     => "127.0.0.1",
        port   => "24422",
        nconn  => $ncntr*2,
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
                
