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
use Fdftest::UnifiedAPI;
use Fdftest::Node;
use Test::More 'no_plan';

#tests =( 6*($nconn) + 2) * ($loop) + 4
#tests = 66 + 482 * $loop
my $node;
my (@cguids, %chash);
my $ncntr = 1;
my $nconn = 10;
my $loop  = 5;
my $write_loop  = 1;
#$loop  = 100;

sub recreate {
    my ($connid, $cname) = @_;
    my ($ret, $msg, $cguid);
    my $size = 0;
    $cguid = $chash{$cname};

	$ret = CloseContainer($node->conn($connid),$cguid);
	if ( ! ($ret =~ /OK.*/)){
		return "Error cguid=$cguid CloseContainer: $ret";
	}
	$ret =~ s/(\s+)/ /g;
	$msg.= "CloseContainer: OK ";
	$ret = DeleteContainer($node->conn($connid),$cguid);
	if ( ! ($ret =~ /OK.*/)){
		return "Error cguid=$cguid DeleteContainer: $ret";
	}
	@cguids = grep { $_ ne "$cguid" } @cguids;  
	delete $chash{$cname};
	$ret =~ s/(\s+)/ /g;
	$msg.= "DeleteContainer: OK ";
	return $ret." $msg";
}

sub ZSTrxUpdate_lc {
    my ($connid, $cguid, $counter, $pg, $osd, $dataoffset, $datalen, $nops) = @_;
    my ($ret, $msg, $flag);
    $flag  = "ZS_WRITE_MUST_EXIST";

    $ret = ZSTransactionStart(
             $node->conn($connid),
           );
    like($ret, qr/OK.*/, 'ZSTransactionStart');

    $ret = WriteLogObjects($node->conn ($connid), $cguid, $counter, $pg, $osd, $dataoffset, $datalen, $nops, $flag);
    like($ret, qr/OK.*/, $ret);

    $ret = ZSTransactionCommit(
                $node->conn($connid)
            );
    like($ret, qr/OK.*/, 'ZSTransactionCommit');
}    

sub ZSTrxUpdate {
    my ($connid, $cguid, $keyoffset, $keylen, $datalen, $nops) = @_;
    my ($ret, $msg, $flag);
    $flag  = "ZS_WRITE_MUST_EXIST";

    $ret = ZSTransactionStart(
             $node->conn($connid),
           );
    like($ret, qr/OK.*/, 'ZSTransactionStart');

    $ret = WriteObjects($node->conn ($connid), $cguid, $keyoffset, $keylen, $keyoffset, $datalen, $nops, $flag);
    like($ret, qr/OK.*/, $ret);

    $ret = ZSTransactionCommit(
                $node->conn($connid)
            );
    like($ret, qr/OK.*/, 'ZSTransactionCommit');
}    

sub verify_data_lc {
    my ($connid, $cguid, $counter, $pg, $osd, $dataoffset, $datalen, $nops) = @_;
    my ($ret, $msg, $flag);

    $ret = ReadLogObjects($node->conn ($connid), $cguid, $counter, $pg, $osd, $dataoffset, $datalen, $nops);
    like ($ret, qr/^OK.*/, $ret);
}    

sub verify_data {
    my ($connid, $cguid, $keyoffset, $keylen, $datalen, $nops) = @_;
    my ($ret, $msg, $flag);

    $ret = ReadObjects($node->conn ($connid), $cguid, $keyoffset, $keylen, $keyoffset, $datalen, $nops);
    like ($ret, qr/^OK.*/, $ret);
}    

sub delete_data{
    my ($connid, $cguid, $keyoffset, $keylen, $nops) = @_;
    my ($ret, $msg, $flag);

    sleep(5);
    $ret = DeleteSeqObjects($node->conn ($connid), $cguid, $keyoffset, $keylen, $nops);
    like ($ret, qr/^OK.*/, $ret);
}

sub worker_lc {
    my ($connid, $cguid, $counter, $pg, $osd, $dataoffset, $datalen, $nops) = @_;
    my ($ret, $msg, $flag);
    $flag  = "ZS_WRITE_MUST_NOT_EXIST";
    my $n_in = $nops * $write_loop;

    $ret = WriteLogObjects($node->conn ($connid), $cguid, $counter, $pg, $osd, $dataoffset, $datalen, $nops, $flag);
    like ($ret, qr/^OK.*/, $ret);
    $ret = EnumeratePG ($node->conn ($connid), $cguid, $counter, $pg, $osd);
    like ($ret, qr/^OK.*/, $ret);
    $ret = GetContainers ($node->conn ($connid));
    $msg = substr($ret, 0, index($ret, "ZSGet"));
    like ($ret, qr/^OK.*/, $msg . "ZSGetContainers");
}

sub worker {
    my ($connid, $cguid, $keyoffset, $keylen, $datalen, $nops) = @_;
    my ($ret, $msg, $flag);
    $flag  = "ZS_WRITE_MUST_NOT_EXIST";
    my $n_in = $nops * $write_loop;

    $ret = WriteSeqObjects($node->conn ($connid), $cguid, $keyoffset, $keylen, $keyoffset, $datalen, $nops, $flag);
    like ($ret, qr/^OK.*/, $ret);
=pod    
    $ret = ReadSeqObjects($node->conn ($connid), $cguid, $keyoffset, $keylen, $keyoffset, $datalen, $nops);
    like ($ret, qr/^OK.*/, $ret);
    $ret = RangeAll ($node->conn ($connid), $cguid, $n_in);
    like ($ret, qr/^OK.*/, $ret);
    $ret = GetContainers ($node->conn ($connid));
    $msg = substr($ret, 0, index($ret, "ZSGet"));
    like ($ret, qr/^OK.*/, $msg . "ZSGetContainers");
=cut    
}

sub reopen {
    my ($connid, $cname, $size) = @_;
    my $cguid = $chash{$cname};
    my $ret;
    $ret = CloseContainer ($node->conn ($connid), $cguid);
    like ($ret, qr/^OK.*/, $ret);
    $ret = OpenContainer ($node->conn ($connid), $cname, 3, $size, "ZS_CTNR_RW_MODE", "no");
    like ($ret, qr/^OK.*/, $ret);
}

sub test_run {
    my ($ret, $msg);
    my @threads;
    my $size = 0;
    my $nops = 1000;
    $ret = $node->start (
        ZS_REFORMAT => 1,
        threads => $nconn + 1,
    );
    like ($ret, qr/^OK.*/, 'Node start');


    	for (1 .. $ncntr) {
        my $cname = 'ctrn-' . "$_";
        $ret = OpenContainer ($node->conn (0), $cname, 3, $size, "ZS_CTNR_CREATE", "no", "ZS_DURABILITY_HW_CRASH_SAFE", "BTREE");
        like ($ret, qr/^OK.*/, $ret);

        if ($ret =~ /^OK cguid=(\d+)/) {
            push(@cguids, $1);
            $chash{$cname} = $1;
        }
        else {
            return;
        }
    	}

        @threads = ();
        for (1 .. $nconn) {
            my $cname     = 'ctrn-1';
            my $connid    = $_ -1;
            my $keyoffset = 100;
            my $keylen = 10 + int($_/2);
            my $dataoffset = 100;
            my $datalen = 1024;
            my $nops = 1000;
	    if ($_ % 2 == 0){
            push(@threads, threads->new (\&worker, $connid, $chash{$cname}, $keyoffset, $keylen, $datalen, $nops));
	    }else {
            push(@threads, threads->new (\&delete_data, $connid, $chash{$cname}, $keyoffset + $nops -1 , $keylen + 1, 1));
	    }
        }
        $_->join for (@threads);


        @threads = ();
        for (1 .. $nconn) {
            my $cname     = 'ctrn-1';
            my $connid    = $_ -1;
            my $keyoffset = 100;
            my $keylen = 20 + int($_/2);
            my $dataoffset = 100;
            my $datalen = 1024;
            my $nops = 100000;
	    if ($_ % 2 == 0){
            push(@threads, threads->new (\&worker, $connid, $chash{$cname}, $keyoffset, $keylen, $datalen, $nops));
	    }else {
            push(@threads, threads->new (\&delete_data, $connid, $chash{$cname}, $keyoffset , $keylen + 1, $nops));
	    }
        }
        $_->join for (@threads);

        my $cname= 'ctrn-1';
        my $cguid = $chash{$cname};
        for (1 .. $nconn) {
            my $keyoffset = 100;
            my $keylen = 10 + int($_/2);
            my $dataoffset = 100;
            my $datalen = 1024;
            my $nops = 999;
	    if ($_ % 2 != 0){
        $ret = DeleteSeqObjects($node->conn (1), $cguid, $keyoffset, $keylen + 1, $nops);
        like ($ret, qr/^OK.*/, $ret);
        }
        }
        $ret = RangeAll ($node->conn (0), $cguid, 10000);
        like ($ret, qr/QUERY_DONE.*/, $ret);
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

