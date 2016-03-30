# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file: 
# author: Yiwen Lu 
# email: yiwenlu@hengtiansoft.com
# date: Sep 19, 2014
# description: 

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::Stress;
use Test::More 'no_plan';
use threads;
use threads::shared;

my $node;
my @count:shared = (0,0,0,0,0,0,0,0);
sub worker_write{
        my ($con,$cguid) = @_;
        my $ret;
        foreach(1..1941){
            $ret = ZSSet($node->conn($con), $cguid, $_, 20, 128000,1, "ZS_WRITE_MUST_NOT_EXIST");
            like ($ret, qr/^OK.*/, $ret);
            $ret = ZSSet($node->conn($con), $cguid, $_, 21, 64000,1, "ZS_WRITE_MUST_NOT_EXIST");
            like ($ret, qr/^OK.*/, $ret);
            $ret = ZSSet($node->conn($con), $cguid, $_, (22+$_), 512,6, "ZS_WRITE_MUST_NOT_EXIST");
            like ($ret, qr/^OK.*/, $ret);
            $count[$con]++ ;
        }
}

sub worker_read{
        my $ret;
        my ($con,$cguid,$count) = @_;
        foreach(1..$count){
            $ret = ZSGet($node->conn($con), $cguid, $_, 20, 128000, 1);
            like ($ret, qr/^OK.*/, $ret);
            $ret = ZSGet($node->conn($con), $cguid, $_, 21, 64000, 1);
            like ($ret, qr/^OK.*/, $ret);
            $ret = ZSGet($node->conn($con), $cguid, $_, (22+$_), 512, 6);
            like ($ret, qr/^OK.*/, $ret);
        }
}
sub worker_read_rec{
        my $ret;
        my ($con,$cguid,$count,$info) = @_;
        foreach(1..$count){
            $ret = ZSGet($node->conn($con), $cguid, $_, 20, 128000, 1);
            like ($ret, qr/^OK.*/, $info.' Read after recovery:'.$ret);
            $ret = ZSGet($node->conn($con), $cguid, $_, 21, 64000, 1);
            like ($ret, qr/^OK.*/, $info.' Read after recovery:'.$ret);
            $ret = ZSGet($node->conn($con), $cguid, $_, (22+$_), 512, 6);
            like ($ret, qr/^OK.*/, $info.' Read after recovery:'.$ret);
        }
}

my @cguids;
my @threads;
my $nctr = 1;
my @ctr_type = ("BTREE","BTREE");
my $ret;
my $cguid;


sub test_run_node {
    $ret = $node->start(
                        ZS_REFORMAT  => 1,
                        nconn   => 8,
                        );
    like($ret, qr/OK.*/, 'remote engine started');

    foreach(0..$nctr-1)
    {
         $ret = ZSOpen($node->conn(0),"ctr-$_",3,0,"ZS_CTNR_CREATE","yes","ZS_DURABILITY_HW_CRASH_SAFE",$ctr_type[$_%2]);
         like ($ret, qr/^OK.*/, $ret);
         $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
         $cguids[$_]=$cguid;
    }

    @threads = ();
    #foreach(@count){print $_;}
    foreach(0..$nctr-1)
    {
        push(@threads, threads->new (\&worker_write,$_, $cguids[$_]));
    }
    $_->join for (@threads);
    #foreach(@count){print $_;}

    @threads = ();
    foreach(0..$nctr-1)
    {
        print "cguid=$cguids[$_],count=$count[$_]\n";
        push(@threads, threads->new (\&worker_read,$_, $cguids[$_], $count[$_]));
    }
    $_->join for (@threads);
    return;
}

sub test_run_recovery {

    print "<<< Test recovery with async_write=yes on remote engine >>>.\n";
    $ret = $node->start(
                        ZS_REFORMAT  => 0,
                        nconn   => 8,
                        );
    like($ret, qr/OK.*/, 'remote engine started for recovery');

    foreach(0..$nctr-1)
    {
         $ret = ZSOpen($node->conn(0),"ctr-$_",3,0,"ZS_CTNR_RW_MODE","yes","ZS_DURABILITY_HW_CRASH_SAFE",$ctr_type[$_%2]);
         like ($ret, qr/^OK.*/, $ret);
         $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
         $cguids[$_] = $cguid;
    }

    @threads = ();
    foreach(0..$nctr-1)
    {
        print "$_==count=$count[$_]\n";
        push(@threads, threads->new (\&worker_read_rec,$_, $cguids[$_], $count[$_],'MULTI'));
    }
    $_->join for (@threads);


    print "READ DATA IN SINGLE THREAD\n";

    foreach(0..$nctr-1)
    {
        print "$_==count=$count[$_]\n";
        worker_read_rec($_, $cguids[$_], $count[$_], 'SINGLE');
    }

    return;
}

sub test_init {
    my $ssh = shift;

    $node = Fdftest::Node->new(
                ip          => "127.0.0.1", 
                port        => "24422",
                nconn       => 8,
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

    test_run_node();
    sleep(2);
    $node->stop();
    test_run_recovery();
    test_clean();
}


# clean ENV
END {
    $node->clean();
}
