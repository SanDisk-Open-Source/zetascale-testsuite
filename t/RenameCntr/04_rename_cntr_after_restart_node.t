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
# date: Apr 9, 2014
# description:


#!/usr/bin/perl

use strict;
use warnings;
use threads;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::BasicTest;
use Fdftest::Stress;
use Test::More tests => 93;

my $node; 

sub test_run {
    my ($ret, $cguid);
    my @cguids;
    my $loop = 100;
    my $ncntr = 10;
    my $size = 0;
    my $keyoff = 1000;
    my @prop  = ([3,"ZS_DURABILITY_HW_CRASH_SAFE","no"]);
    my @data = ([64, 16000, 3000], [64, 32000, 3000], [64, 64000, 3000], [64, 128000, 3000], [64, 48, 60000]);

    $ret = $node->start(ZS_REFORMAT  => 1,);
    like($ret, qr/OK.*/, 'Node started');

    foreach my $p(@prop){
        @cguids = ();
        for(0 .. $ncntr-1){
            $ret=OpenContainer($node->conn(0), "ctr-$_","ZS_CTNR_CREATE",$size,$$p[0],$$p[1],$$p[2]);
            $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
            push(@cguids, $cguid);

        }
 
        for(0 .. $ncntr-1){
            $ret = ZSRename($node->conn(0), $cguids[$_], "rename_a_ctr-$_");
            like ($ret, qr/^OK.*/, $ret);
        }

        for(@cguids){
            FlushContainer($node->conn(0), $_);
            CloseContainer($node->conn(0), $_);
        }

        $ret = $node->stop();
        like($ret,qr/OK.*/,"Node Stop");
        $ret = $node->start(ZS_REFORMAT => 0);
        like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=0");

        for(0 .. $ncntr-1){
            $ret=OpenContainer($node->conn(0), "rename_a_ctr-$_","ZS_CTNR_RW_MODE",$size,$$p[0],$$p[1],$$p[2]);
        }

        for(0 .. $ncntr-1){
            $ret = ZSRename($node->conn(0), $cguids[$_], "rename_b_ctr-$_");
            like ($ret, qr/^OK.*/, $ret);
            $ret = ZSRename($node->conn(0), $cguids[$_], "rename_b_ctr-$_");
            like ($ret, qr/^Error.*/, $ret);
        }

        for(@cguids){
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
        nconn  => 10,
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
                
