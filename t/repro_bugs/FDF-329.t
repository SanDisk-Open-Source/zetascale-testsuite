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
my $nconn = $ncntr;
my $loop  = 1;
my $write_loop  = 100;
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

    $ret = EnumeratePG ($node->conn ($connid), $cguid, $counter, $pg, $osd);
    like ($ret, qr/^OK.*/, $ret);
    system("top -b -n 1|grep zs_test_engine");
    #$ret = ReadLogObjects($node->conn ($connid), $cguid, $counter, $pg, $osd, $dataoffset, $datalen, $nops);
    #like ($ret, qr/^OK.*/, $ret);
}    

sub verify_data {
    my ($connid, $cguid, $keyoffset, $keylen, $datalen, $nops) = @_;
    my ($ret, $msg, $flag);

    $ret = ReadObjects($node->conn ($connid), $cguid, $keyoffset, $keylen, $keyoffset, $datalen, $nops);
    like ($ret, qr/^OK.*/, $ret);
}    

sub worker_lc {
    my ($connid, $cguid, $counter, $pg, $osd, $dataoffset, $datalen, $nops) = @_;
    my ($ret, $msg, $flag);
    $flag  = "ZS_WRITE_MUST_NOT_EXIST";
    my $n_in = $nops * $write_loop;

    $ret = WriteLogObjects($node->conn ($connid), $cguid, $counter, $pg, $osd, $dataoffset, $datalen, $nops, $flag);
    like ($ret, qr/^OK.*/, $ret);
    #$ret = EnumeratePG ($node->conn ($connid), $cguid, $counter, $pg, $osd);
    #like ($ret, qr/^OK.*/, $ret);
    #$ret = GetContainers ($node->conn ($connid));
    #$msg = substr($ret, 0, index($ret, "ZSGet"));
    #like ($ret, qr/^OK.*/, $msg . "ZSGetContainers");
}

sub worker {
    my ($connid, $cguid, $keyoffset, $keylen, $datalen, $nops) = @_;
    my ($ret, $msg, $flag);
    $flag  = "ZS_WRITE_MUST_NOT_EXIST";
    my $n_in = $nops * $write_loop;

    $ret = WriteObjects($node->conn ($connid), $cguid, $keyoffset, $keylen, $keyoffset, $datalen, $nops, $flag);
    like ($ret, qr/^OK.*/, $ret);
    $ret = ReadObjects($node->conn ($connid), $cguid, $keyoffset, $keylen, $keyoffset, $datalen, $nops);
    like ($ret, qr/^OK.*/, $ret);
    $ret = RangeAll ($node->conn ($connid), $cguid, $n_in);
    like ($ret, qr/^OK.*/, $ret);
    $ret = GetContainers ($node->conn ($connid));
    $msg = substr($ret, 0, index($ret, "ZSGet"));
    like ($ret, qr/^OK.*/, $msg . "ZSGetContainers");
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


    for (1 .. $loop) {
        my $offset_id = $_;
    	for (1 .. $ncntr) {
        my $cname = 'ctrn-' . "$_";
        $ret = OpenContainer ($node->conn (0), $cname, 3, $size, "ZS_CTNR_CREATE", "no", "ZS_DURABILITY_HW_CRASH_SAFE", "LOGGING");
        like ($ret, qr/^OK.*/, $ret);

        if ($ret =~ /^OK cguid=(\d+)/) {
            push(@cguids, $1);
            $chash{$cname} = $1;
        }
        else {
            return;
        }
    	}

        for (1 .. $write_loop) {
        my $current_id = $_;
        @threads = ();
        for (1 .. $nconn) {
            my $cname     = 'ctrn-' . $_;
            my $connid    = $_ -1;
	    my $counter = 0;
	    my $pg = "pgaaaaaaaaaaaaaa". $offset_id;
	    my $osd = "osdbbbbbbbbbb" . "thread-$_" . "writeloop-$current_id"; 
	    my $dataoffset = 0 + $current_id * $_;
	    my $datalen = 200 + 80 * $nconn;
            push(@threads, threads->new (\&worker_lc, $connid, $chash{$cname}, $counter, $pg, $osd, $dataoffset, $datalen, $nops));
        }
        $_->join for (@threads);
        }
        system("top -b -n 1|grep zs_test_engine");

        print "=====Cycle $_=======\n";
        #$ret = $node->stop ();
        #like ($ret, qr/OK.*/, 'Node stop');

        #$ret = $node->start (ZS_REFORMAT => 0, threads => $nconn + 1);
        #like ($ret, qr/OK.*/, 'Node restart');

        for (1 .. $ncntr) {
            my $cname = 'ctrn-' . "$_";
            $ret = OpenContainer ($node->conn (0), $cname, 3, $size, "ZS_CTNR_RW_MODE", "no", "ZS_DURABILITY_HW_CRASH_SAFE", "LOGGING");
            like ($ret, qr/^OK.*/, $ret);
        }

        for ( 1 .. $write_loop) {
        my $current_id = $_;
        @threads = ();
        for (1 .. $nconn) {
            my $cname     = 'ctrn-' . $_;
            my $connid    = $_ -1;
            my $counter = 0;
            my $pg = "pgaaaaaaaaaaaaaa". $offset_id;
            my $osd = "osdbbbbbbbbbb" . "thread-$_" . "writeloop-$current_id";
            my $dataoffset = 0 + $current_id * $_;
            my $datalen = 200 + 80 * $nconn;
            push(@threads, threads->new (\&verify_data_lc, $connid, $chash{$cname}, $counter, $pg, $osd, $dataoffset, $datalen, $nops));
        }
        $_->join for (@threads);
        }
        system("top -b -n 1|grep zs_test_engine");
        return;
=pod
        for ( 1 .. $write_loop) {
        my $current_id = $_;
        @threads = ();
        for (1 .. $nconn) {
            my $cname     = 'ctrn-' . $_;
            my $connid    = $_ -1;
	    if ($_ % 5  == 0){
            my $counter = 0;
            my $pg = "pgaaaaaaaaaaaaaa". $offset_id;
            my $osd = "osdbbbbbbbbbb" . "thread-$_" . "writeloop-$current_id";
            my $dataoffset = 0 + $current_id * $_;
            my $datalen = 200 + 80 * $nconn + 80;
            push(@threads, threads->new (\&ZSTrxUpdate_lc, $connid, $chash{$cname}, $counter, $pg, $osd, $dataoffset, $datalen, $nops));
	    }else {
            my $keyoffset = 0 + $offset_id;
            my $keylen    = 100 + $_ + $current_id;
            my $datalen   = $keylen + 800 * $current_id + 800;
            push(@threads, threads->new (\&ZSTrxUpdate, $connid, $chash{$cname}, $keyoffset, $keylen, $datalen, $nops));
	    }
        }
        $_->join for (@threads);
        }
        for ( 1 .. $write_loop) {
        my $current_id = $_;
        @threads = ();
        for (1 .. $nconn) {
            my $cname     = 'ctrn-' . $_;
            my $connid    = $_ -1;
	    if ($_ % 5  == 0){
            my $counter = 0;
            my $pg = "pgaaaaaaaaaaaaaa". $offset_id;
            my $osd = "osdbbbbbbbbbb" . "thread-$_" . "writeloop-$current_id";
            my $dataoffset = 0 + $current_id * $_;
            my $datalen = 200 + 80 * $nconn + 80;
            push(@threads, threads->new (\&verify_data_lc, $connid, $chash{$cname}, $counter, $pg, $osd, $dataoffset, $datalen, $nops));
	    }else {
            my $keyoffset = 0 + $offset_id;
            my $keylen    = 100 + $_ + $current_id;
            my $datalen   = $keylen + 800 * $current_id + 800;
            push(@threads, threads->new (\&verify_data, $connid, $chash{$cname}, $keyoffset, $keylen, $datalen, $nops));
	    }
        }
        $_->join for (@threads);
        }
=cut

        for (1 .. $ncntr) {
            my $cname = 'ctrn-' . "$_";
            $ret = recreate (0, $cname);
            like ($ret, qr/^OK.*/, $ret);
            if ($ret =~ qr/^Error.*/) {
                return;
            }
        }
    }

    $ret = GetContainers ($node->conn (0), $ncntr);
    $msg = substr($ret, 0, index($ret, "ZSGet"));
    like ($ret, qr/^OK.*/, $msg . "ZSGetContainers");
    for (@cguids) {
        $ret = CloseContainer ($node->conn (0), $_);
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

