# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file:
# author: Runhan Mao(Monica)
# email: runhanmao@hengtiansoft.com
# date: April 8, 2015
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
use Test::More tests => 142;

my $node; 
my $ncntr =10;

sub worker_rename {
    my ($con,$cguid, $cname) = @_;
    my $ret;
    $ret=ZSRename($con, $cguid, "rename_$cname"); 
    like($ret,qr/OK.*/,$ret);
    $ret=ZSRename($con, $cguid, "rename_$cname");
    like($ret,qr/Error.*/,$ret);
}

sub worker_delcntr {
    my ($con,$cguid) = @_;
    my $ret;
    $ret = ZSDelete($con, $cguid);
    like ($ret, qr/^OK.*/, $ret);
}

sub test_run {
    my ($ret, $cguid);
    my @cguids;
    my $loop = 100;
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

            foreach my $d(@data){
                WriteReadObjects($node->conn(0), $cguid, $keyoff, $$d[0], $keyoff, $$d[1], $$d[2]);
                $keyoff = $keyoff + $$d[0]*$$d[2];
            }
            $keyoff = 1000;
        }

        my @threads = ();
        for(0..$ncntr-1)
        {
            my $cname="ctr-$_";
            push(@threads, threads->new (\&worker_rename, $node->conn(2*$_), $cguids[$_],  $cname));
            #push(@threads, threads->new (\&worker_rename, $node->(2*$_), $cguids[$_],  "ctr-$_"));
            push(@threads, threads->new (\&worker_delcntr, $node->conn(2*$_+1), $cguids[$_]));
        }
        $_->join for (@threads);
 
        $ret = $node->stop();
        like($ret,qr/OK.*/,"Node Stop");
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
                