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
use Test::More 'no_plan';

#tests =( 6*($nconn) + 2) * ($loop) + 4
#tests = 66 + 482 * $loop
my $node;
my (@cguids, %chash);
my $ncntr = 5;
my $nconn = $ncntr;
my $loop  = 5;
my $write_loop  = 10;
#$loop  = 100;

sub recreate {
    my ($connid, $cname) = @_;
    my ($ret, $msg, $cguid);
    my $size = 0;
    $cguid = $chash{$cname};
=cut
    $ret = ZSOpen ($node->conn ($connid), $cname, 3, $size, "ZS_CTNR_RW_MODE", "no");
    if (!($ret =~ /OK.*/)) {
        return "Error cguid=$cguid ZSOpen: $ret";
    }
    $msg = "ZSOpen: OK ";
=cut
#=comment for #10532
	$ret = ZSClose($node->conn($connid),$cguid);
	if ( ! ($ret =~ /OK.*/)){
		return "Error cguid=$cguid ZSClose: $ret";
	}
	$ret =~ s/(\s+)/ /g;
	$msg.= "ZSClose: OK ";
	$ret = ZSDelete($node->conn($connid),$cguid);
	if ( ! ($ret =~ /OK.*/)){
		return "Error cguid=$cguid ZSDelete: $ret";
	}
	@cguids = grep { $_ ne "$cguid" } @cguids;  
	delete $chash{$cname};
	$ret =~ s/(\s+)/ /g;
	$msg.= "ZSDelete: OK ";
	$ret = ZSOpen($node->conn($connid),$cname,3,$size,"ZS_CTNR_CREATE","no");
	$ret =~ s/(\s+)/ /g;
	if ( $ret =~ /^OK cguid=(\d+).*/ ){
		$chash{$cname} = $1;
		push(@cguids,$1);
		return "OK cguid=$1 $msg"."ZSCreate:OK";
	}
	return $ret." $msg";
#=cut

    return $ret;
}

sub ZSTrxUpdate {
    my ($connid, $cguid, $keyoffset, $keylen, $datalen, $nops) = @_;
    my ($ret, $msg, $flag);
    $flag  = "ZS_WRITE_MUST_EXIST";

    $ret = ZSTransactionStart(
             $node->conn($connid),
           );
    like($ret, qr/OK.*/, 'ZSTransactionStart');

    $ret = ZSSet($node->conn ($connid), $cguid, $keyoffset, $keylen, $datalen, $nops, $flag);
    like($ret, qr/OK.*/, $ret);

    $ret = ZSTransactionCommit(
                $node->conn($connid)
            );
    like($ret, qr/OK.*/, 'ZSTransactionCommit');
}    

sub verify_data {
    my ($connid, $cguid, $keyoffset, $keylen, $datalen, $nops) = @_;
    my ($ret, $msg, $flag);

    $ret = ZSGet($node->conn ($connid), $cguid, $keyoffset, $keylen, $datalen, $nops);
    like ($ret, qr/^OK.*/, $ret);
}    

sub worker {
    my ($connid, $cguid, $keyoffset, $keylen, $datalen, $nops) = @_;
    my ($ret, $msg, $flag);
    $flag  = "ZS_WRITE_MUST_NOT_EXIST";
    my $n_in = $nops * $write_loop;

    $ret = ZSSet($node->conn ($connid), $cguid, $keyoffset, $keylen, $datalen, $nops, $flag);
    like ($ret, qr/^OK.*/, $ret);
    $ret = ZSGet($node->conn ($connid), $cguid, $keyoffset, $keylen, $datalen, $nops);
    like ($ret, qr/^OK.*/, $ret);
    $ret = ZSRangeAll ($node->conn ($connid), $cguid, $n_in);
    like ($ret, qr/^OK.*/, $ret);
    $ret = ZSGetConts ($node->conn ($connid));
    $msg = substr($ret, 0, index($ret, "ZSGet"));
    like ($ret, qr/^OK.*/, $msg . "ZSGetContainers");
}

sub reopen {
    my ($connid, $cname, $size) = @_;
    my $cguid = $chash{$cname};
    my $ret;
    $ret = ZSClose ($node->conn ($connid), $cguid);
    like ($ret, qr/^OK.*/, $ret);
    $ret = ZSOpen ($node->conn ($connid), $cname, 3, $size, "ZS_CTNR_RW_MODE", "no");
    like ($ret, qr/^OK.*/, $ret);
}

sub test_run {
    my ($ret, $msg);
    my @threads;
    my $size = 0;
    $ret = $node->start (
        ZS_REFORMAT => 1,
        threads => $nconn + 1,
    );
    like ($ret, qr/^OK.*/, 'Node start');

    # Create containers with $nconn connections
    for (1 .. $ncntr) {
        my $cname = 'ctrn-' . "$_";
        $ret = ZSOpen ($node->conn (0), $cname, 3, $size, "ZS_CTNR_CREATE", "no");
        like ($ret, qr/^OK.*/, $ret);

        if ($ret =~ /^OK cguid=(\d+)/) {
            push(@cguids, $1);
            $chash{$cname} = $1;
        }
        else {
            return;
        }
    }

    for (1 .. $loop) {
        my $offset_id = $_;
        for (1 .. $write_loop) {
        my $current_id = $_;
        @threads = ();
        for (1 .. $nconn) {
            my $cname     = 'ctrn-' . $_;
            my $keyoffset = 0 + $offset_id;
            my $keylen    = 100 + $_ + $current_id;
            my $datalen   = $keylen + 64000;
            my $nops      = 50;
            my $connid    = $_ -1;
            push(@threads, threads->new (\&worker, $connid, $chash{$cname}, $keyoffset, $keylen, $datalen, $nops));
        }
        $_->join for (@threads);
        }

        @threads = ();
        for (1 .. $ncntr) {
            my $cname = 'ctrn-' . "$_";
            push(@threads, threads->new (\&reopen, $_, $cname, $size));
        }
        $_->join for (@threads);

        print "=====Cycle $_=======\n";
        $ret = $node->stop ();
        like ($ret, qr/OK.*/, 'Node stop');

        $ret = $node->start (ZS_REFORMAT => 0, threads => $nconn + 1);
        like ($ret, qr/OK.*/, 'Node restart');

        for (1 .. $ncntr) {
            my $cname = 'ctrn-' . "$_";
            $ret = ZSOpen ($node->conn (0), $cname, 3, $size, "ZS_CTNR_RW_MODE", "no");
            like ($ret, qr/^OK.*/, $ret);
        }

        for ( 1 .. $write_loop) {
        my $current_id = $_;
        @threads = ();
        for (1 .. $nconn) {
            my $cname     = 'ctrn-' . $_;
            my $keyoffset = 0 + $offset_id;
            my $keylen    = 100 + $_ + $current_id;
            my $datalen   = $keylen + 64000;
            my $nops      = 50;
            my $connid    = $_ -1;
            push(@threads, threads->new (\&verify_data, $connid, $chash{$cname}, $keyoffset, $keylen, $datalen, $nops));
        }
        $_->join for (@threads);
        }

        for ( 1 .. $write_loop) {
        my $current_id = $_;
        @threads = ();
        for (1 .. $nconn) {
            my $cname     = 'ctrn-' . $_;
            my $keyoffset = 0 + $offset_id;
            my $keylen    = 100 + $_ + $current_id;
            my $datalen   = $keylen + 64000 + 64000;
            my $nops      = 50;
            my $connid    = $_ -1;
            push(@threads, threads->new (\&ZSTrxUpdate, $connid, $chash{$cname}, $keyoffset, $keylen, $datalen, $nops));
        }
        $_->join for (@threads);
        }

        for ( 1 .. $write_loop) {
        my $current_id = $_;
        @threads = ();
        for (1 .. $nconn) {
            my $cname     = 'ctrn-' . $_;
            my $keyoffset = 0 + $offset_id;
            my $keylen    = 100 + $_ + $current_id;
            my $datalen   = $keylen + 64000 + 64000;
            my $nops      = 50;
            my $connid    = $_ -1;
            push(@threads, threads->new (\&verify_data, $connid, $chash{$cname}, $keyoffset, $keylen, $datalen, $nops));
        }
        $_->join for (@threads);
        }

        for (1 .. $ncntr) {
            my $cname = 'ctrn-' . "$_";
            $ret = recreate (0, $cname);
            like ($ret, qr/^OK.*/, $ret);
            if ($ret =~ qr/^Error.*/) {
                return;
            }
        }
    }

    $ret = ZSGetConts ($node->conn (0), $ncntr);
    $msg = substr($ret, 0, index($ret, "ZSGet"));
    like ($ret, qr/^OK.*/, $msg . "ZSGetContainers");
    for (@cguids) {
        $ret = ZSClose ($node->conn (0), $_);
        like ($ret, qr/^OK.*/, $ret);
    }
    return;
}

sub test_init {
    $node = Fdftest::Node->new (
        ip    => "127.0.0.1",
        port  => "24422",
        threads => $nconn + 1,
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

=comemnt for long run
    my $mode = "short";
    my $arg = shift(@ARGV);
    while ($arg)
    {
        if ($arg eq "-m" or $arg eq "-M")
        {
            $mode = shift(@ARGV);
        }
        $arg = shift(@ARGV);
    }

    if ( defined ($mode) & 'x'.$mode eq 'x'.'long') {
        #$loop   = 1000;
        $loop   = 50;
        #$loop   = 10;
        $nconn  = 64;
        $ncntr  = 32;
    }
=cut

    test_init ();

    test_run ();

    test_clean ();
}

