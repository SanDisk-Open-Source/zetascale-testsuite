# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file: basic.pl
# author: yiwen sun
# email: yiwensun@hengtiansoft.com
# date: Oct 15, 2012
# description: basic sample for testcase

#!/usr/bin/perl

use strict;
use warnings;
use Switch;
use threads;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Stress;
use Fdftest::Node;
use Test::More tests => 9;

my $node;
my (@cguids, %chash);
my $nconn = 2;
sub worker_write {
    my ($connid, $cguid, $keyoffset, $keylen, $datalen, $nops) = @_;
    my $ret = ZSSet ($node->conn ($connid), $cguid, $keyoffset, $keylen, $datalen, $nops, "ZS_WRITE_MUST_NOT_EXIST");
    like ($ret, qr/^OK.*/, $ret);
}

sub worker_read {
    my ($connid, $cguid, $keyoffset, $keylen, $datalen, $nops) = @_;
    my $ret = ZSGet ($node->conn ($connid), $cguid, $keyoffset, $keylen, $datalen, $nops);
    like ($ret, qr/^OK.*/, $ret);
}

sub test_run {
    my ($ret, $msg);
    my @threads;
	my $cname= "demo0";
	my ($keyoffset, $keylen, $datalen, $maxops, $nops);
    my $size = 102400;
    my $mode= 3;

    $ret = $node->start (
#        gdb_switch   => 1,
        ZS_REFORMAT => 1,
    );
    like ($ret, qr/^OK.*/, 'Node start');

    $ret = ZSOpen ($node->conn (0), $cname, $mode, $size, "ZS_CTNR_CREATE", "no");
    like ($ret, qr/^OK.*/, $ret);
    my $cguid = $1 if ($ret =~ /^OK cguid=(\d+)/) ; 

    @threads = ();
    ($keyoffset, $keylen, $datalen, $nops) = (1000,50,1000,50000);
    for (1 .. $nconn) {
        push(@threads, threads->new (\&worker_write, $_, $cguid, $keyoffset, $keylen, $datalen, $nops));
        $keylen = $keylen + 5;
        $datalen = $datalen + 200 ;
    }
    $_->join for (@threads);

    @threads = ();
    ($keyoffset, $keylen, $datalen, $nops) = (1000,50,1000,50000);
    for (1 .. $nconn) {
        push(@threads, threads->new (\&worker_read, $_, $cguid, $keyoffset, $keylen, $datalen, $nops));
        $keylen = $keylen + 5;
        $datalen = $datalen + 200 ;
    }
    $_->join for (@threads);

   $ret = ZSClose($node->conn (0),$cguid);
   like ($ret, qr/^OK.*/,$ret);
   $node->stop ();
   $ret = $node->start (
            ZS_REFORMAT => 0,
           );
   
   $ret = ZSOpen ($node->conn(0), $cname, $mode, $size, "ZS_CTNR_RW_MODE", "no");
   @threads = ();
   ($keyoffset, $keylen, $datalen, $nops) = (1000,50,1000,50000);
   for (1 .. $nconn) {
       push(@threads, threads->new (\&worker_read, $_, $cguid, $keyoffset, $keylen, $datalen, $nops));
       $keylen = $keylen + 5;
       $datalen = $datalen + 200 ;
   }
   $_->join for (@threads);

   return;
}

sub test_init {
    $node = Fdftest::Node->new (
        ip    => "127.0.0.1",
        port  => "24422",
        nconn => $nconn,
		prop  => "$Bin/../../conf/stress.prop",
    );
    return;
}

sub test_clean {
    $node->stop ();
    $node->set_ZS_prop (ZS_REFORMAT => 1);

    return;
}

#
# main
#
{
    test_init ();

    test_run ();

    test_clean ();
}

