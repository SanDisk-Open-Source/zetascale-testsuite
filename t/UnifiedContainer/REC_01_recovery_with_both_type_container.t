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
use Test::More tests => 59;

my $node; 
my $nconn = 128;
sub worker_write{
    my ($con,$cguid) = @_;
    my $res;
    $res = ZSSet($node->conn($con), $cguid, 50, 50, 100, 5000, "ZS_WRITE_MUST_NOT_EXIST");
    like ($res, qr/^OK.*/, $res);
    $res = ZSSet($node->conn($con), $cguid, 100, 100, 1000, 5000, "ZS_WRITE_MUST_NOT_EXIST");
    like ($res, qr/^OK.*/, $res);
    $res = ZSSet($node->conn($con), $cguid, 150, 150, 10000, 5000, "ZS_WRITE_MUST_NOT_EXIST");
    like ($res, qr/^OK.*/, $res);
}

sub worker_read{
    my ($con,$cguid) = @_;
    my $res;
    $res = ZSGet($node->conn($con), $cguid, 50, 50, 100, 5000);
    like ($res, qr/^OK.*/, $res);
    $res = ZSGet($node->conn($con), $cguid, 100, 100, 1000, 5000);
    like ($res, qr/^OK.*/, $res);
    $res = ZSGet($node->conn($con), $cguid, 150, 150, 10000, 5000);
    like ($res, qr/^OK.*/, $res);
}

sub test_run {
    my $cguid;
    my $ret;
    my @cguids;
    my @threads;    
    my %cguid_cname;
    my $nctr = 2*2;
    my @choice = (3,5); 
    my @ctr_type = ("BTREE","HASH");    
    $ret = $node->start(ZS_REFORMAT => 1);    
    like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=1");
   
    foreach(0..$nctr-1)
    {
        $ret = ZSOpen($node->conn(0),"ctr-$_",$choice[$_%2],0,"ZS_CTNR_CREATE","yes","ZS_DURABILITY_SW_CRASH_SAFE",$ctr_type[$_%2]);
        like ($ret, qr/^OK.*/, $ret);
        $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
        $cguids[$_]=$cguid;
        $cguid_cname{$cguid}="ctr-$_";
    }

    @threads = ();
    foreach(0..$nctr-1) 
    {
        push(@threads, threads->new (\&worker_write,$_, $cguids[$_]));
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
    }

    $ret = $node->stop();
    like($ret,qr/OK.*/,"Node Stop"); 
    $ret = $node->start(ZS_REFORMAT => 0);
    like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=0");

    foreach(0..$nctr-1)
    {
        $ret = ZSOpen($node->conn(0),"ctr-$_",$choice[$_%2],0,"ZS_CTNR_RW_MODE","yes","ZS_DURABILITY_SW_CRASH_SAFE",$ctr_type[$_%2]);
        like ($ret, qr/^OK.*/, $ret);
    }

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


