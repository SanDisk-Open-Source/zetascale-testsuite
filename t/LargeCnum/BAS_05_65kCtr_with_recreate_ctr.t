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

#use strict;
use warnings;
use Switch;
use threads;


use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::Stress;
use Test::More 'no_plan';

my $node; 
my $nctr = 64000;
my $nconn = 100;
sub worker_write{
    my ($con,$cguid) = @_;
    my $res;
    $res = ZSSet($node->conn($con), $cguid, 50, 100, 100, 5, "ZS_WRITE_MUST_NOT_EXIST");
    like ($res, qr/^OK.*/, $res);
}

sub worker_read{
    my ($con,$cguid) = @_;
    my $res;
    $res = ZSGet($node->conn($con), $cguid, 50, 100, 100, 5);
    like ($res, qr/^OK.*/, $res);
}

sub worker_close_and_delete{
    my ($con,$cguid) = @_;
    my $res;
    $res = ZSClose($node->conn($con), $cguid);
    like ($res, qr/^OK.*/, $res);
    $res = ZSDelete($node->conn($con), $cguid);
    like ($res, qr/^OK.*/, $res);
}

sub test_run {
    my $cguid;
    my $ret;
    my $choice; 
    my @cguids;
    my @threads;    
    my @ctypes;
    my @ctr_type = ("BTREE","HASH");    

    $ret = $node->set_ZS_prop (ZS_MAX_NUM_CONTAINERS => 64000);
    like ($ret, qr//, 'set ZS_MAX_NUM_CONTAINERS to 64K');

    $ret = $node->start(ZS_REFORMAT => 1);    
    like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=1");
   
    foreach(3,5)
    {
        $choice = $_;
        print "choice=$choice\n";  
        foreach(0..$nctr-1)
        {   
            my $ctype_index = int(rand(2));
            $ret = ZSOpen($node->conn(0),"ctr-$_",$choice,0,"ZS_CTNR_CREATE","yes","ZS_DURABILITY_SW_CRASH_SAFE",$ctr_type[$ctype_index]);
            like ($ret, qr/^OK.*/, $ret);
            $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
            $cguids[$_] = $cguid;
            $ctypes[$_] = $ctr_type[$ctype_index];
        }
         
        #$ret = ZSGetConts($node->conn(0), $nctr);
        #like ($ret, qr/^OK.*/, $ret);

        my $count;
        foreach(0..$nctr/$nconn-1){
            @threads = ();
            $count = $_;
            foreach(0..$nconn-1) 
	    {
                push(@threads, threads->new (\&worker_write, $_, $cguids[$count*$nconn+$_]));
	    }
	    $_->join for (@threads);
        }

        foreach(0..$nctr/$nconn-1) 
        {
            @threads = ();
            $count = $_;
            foreach(0..$nconn-1)
            {
                push(@threads, threads->new (\&worker_read, $_, $cguids[$count*$nconn+$_]));
            }
            $_->join for (@threads);
        }

        foreach(0..$nctr/2/$nconn-1) 
        {
            @threads = ();
            $count = $_;
            foreach(0..$nconn-1)
            {
                push(@threads, threads->new (\&worker_close_and_delete, $_, $cguids[$count*$nconn+$_]));
            }
            $_->join for (@threads);
        }

        foreach(0..$nctr/2-1){
	    my $ctype_index = int(rand(2));
	    $ret = ZSOpen($node->conn(0),"ctr-$_",$choice,0,"ZS_CTNR_CREATE","yes","ZS_DURABILITY_SW_CRASH_SAFE",$ctr_type[$ctype_index]);
	    like ($ret, qr/^OK.*/, $ret);
	    $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
	    $cguids[$_] = $cguid;
	    $ctypes[$_] = $ctr_type[$ctype_index];

            worker_write(0,$cguid);
        }

        foreach($nctr/2..$nctr-1){
	    my $ctype_index = int(rand(2));
	    $ret = ZSOpen($node->conn(0),"ctr-$_",$choice,0,"ZS_CTNR_RW_MODE","yes","ZS_DURABILITY_SW_CRASH_SAFE",$ctypes[$_]);
	    like ($ret, qr/^OK.*/, $ret);
	    $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
	    $cguids[$_] = $cguid;
        }

        foreach(0..$nctr/$nconn-1) 
        {
            @threads = ();
            $count = $_;
            foreach(0..$nconn-1)
            {
                push(@threads, threads->new (\&worker_read, $_, $cguids[$count*$nconn+$_]));
            }
            $_->join for (@threads);
        }

        foreach(0..$nctr/$nconn-1) 
        {
            @threads = ();
            $count = $_;
            foreach(0..$nconn-1)
            {
                push(@threads, threads->new (\&worker_close_and_delete, $_, $cguids[$count*$nconn+$_]));
            }
            $_->join for (@threads);
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
    $node->set_ZS_prop (ZS_REFORMAT => 1, ZS_MAX_NUM_CONTAINERS => 6000);

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


