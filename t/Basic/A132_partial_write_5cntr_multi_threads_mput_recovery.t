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

my $node;
my (@cguids, %chash);
my $ncntr = 5;
my $nconn = 10;
my $loop  = 1;
my $write_loop  = 2;

sub recreate {
    my ($connid, $cname) = @_;
    my ($ret, $msg, $cguid);
    my $size = 0;
    $cguid = $chash{$cname};
=cut
    $ret = OpenContainer ($node->conn ($connid), $cname, 3, $size, "ZS_CTNR_RW_MODE", "no");
    if (!($ret =~ /OK.*/)) {
        return "Error cguid=$cguid OpenContainer: $ret";
    }
    $msg = "OpenContainer: OK ";
=cut
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
	$ret = OpenContainer($node->conn($connid),$cname,3,$size,"ZS_CTNR_CREATE","no");
	$ret =~ s/(\s+)/ /g;
	if ( $ret =~ /^OK cguid=(\d+).*/ ){
		$chash{$cname} = $1;
		push(@cguids,$1);
		return "OK cguid=$1 $msg"."ZSCreate:OK";
	}
	return $ret." $msg";

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

    $ret = MPut ($node->conn ($connid), $cguid, $keyoffset, $keylen, $keyoffset, $datalen, $nops, $flag);
    like($ret, qr/OK.*/, $ret);

    $ret = ZSTransactionCommit(
                $node->conn($connid)
            );
    like($ret, qr/OK.*/, 'ZSTransactionCommit');
}    

sub verify_data {
    my ($connid, $cguid, $keyoffset, $keylen, $datalen, $nops) = @_;
    my ($ret, $msg, $flag);

    $ret = ReadNumKeyObjects($node->conn ($connid), $cguid, $keyoffset, $keylen, $keyoffset, $datalen, $nops);
    like ($ret, qr/^OK.*/, $ret);
}    

sub worker {
    my ($connid, $cguid, $keyoffset, $keylen, $datalen, $nops) = @_;
    my ($ret, $msg, $flag);
    $flag  = "ZS_WRITE_MUST_NOT_EXIST";
    my $n_in = $nops * $write_loop;

    $ret = MPut($node->conn ($connid), $cguid, $keyoffset, $keylen, $keyoffset, $datalen, $nops, $flag);
    like ($ret, qr/^OK.*/, $ret);
    $ret = ReadNumKeyObjects($node->conn ($connid), $cguid, $keyoffset, $keylen, $keyoffset, $datalen, $nops);
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
    my $datadiff = 12000;
    my $nops = 500;

    for ( 1 .. 2) {
    if ($_ == 1 ){
	    print "====Enable Compression====\n";
	    $node->set_ZS_prop (ZS_COMPRESSION => 1);
    } else {
	    print "====Disable Compression====\n";
	    $node->set_ZS_prop (ZS_COMPRESSION => 0);
    }
    @cguids = ();
    $ret = $node->start (
        ZS_REFORMAT => 1,
        threads => $nconn + 1,
    );
    like ($ret, qr/^OK.*/, 'Node start with ZS_REFORMAT=1');

    # Create containers with $nconn connections
    for (1 .. $ncntr) {
        my $cname = 'ctrn-' . "$_";
        $ret = OpenContainer ($node->conn (0), $cname, 3, $size, "ZS_CTNR_CREATE", "no");
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
        for (1 .. $nconn/$ncntr) {
	    my $conn_l = $_;
	    for ( 1 .. $ncntr ){
            my $cname     = 'ctrn-' . $_;
            my $keyoffset = 0 + $offset_id ;
            my $keylen    = 100 + $_ + $current_id * $nconn + $conn_l;
            my $datalen   = $keylen +  $datadiff * $current_id;
            my $nops      = $nops;
            my $connid    = ($conn_l -1)*$ncntr + $_;
            push(@threads, threads->new (\&worker, $connid, $chash{$cname}, $keyoffset, $keylen, $datalen, $nops));
            }
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
        $ret = $node->kill ();

        $ret = $node->start (ZS_REFORMAT => 0, threads => $nconn + 1);
        like ($ret, qr/OK.*/, 'Node restart with REFORMAT = 0');

        for (1 .. $ncntr) {
            my $cname = 'ctrn-' . "$_";
            $ret = OpenContainer ($node->conn (0), $cname, 3, $size, "ZS_CTNR_RW_MODE", "no");
            like ($ret, qr/^OK.*/, $ret);
        }

        for ( 1 .. $write_loop) {
        my $current_id = $_;
        @threads = ();
        for (1 .. $nconn/$ncntr) {
	    my $conn_l = $_;
	    for (1 .. $ncntr) {
            my $cname     = 'ctrn-'. $_;
            my $keyoffset = 0 + $offset_id;
            my $keylen    = 100 + $_ + $current_id * $nconn + $conn_l;
            my $datalen   = $keylen + $datadiff * $current_id;
            my $nops      = $nops;
            my $connid    = ($conn_l -1)*$ncntr + $_;
            push(@threads, threads->new (\&verify_data, $connid, $chash{$cname}, $keyoffset, $keylen, $datalen, $nops));
	    }
        }
        $_->join for (@threads);
        }

        for ( 1 .. $write_loop) {
        my $current_id = $_;
        @threads = ();
        for (1 .. $nconn/$ncntr) {
	    my $conn_l = $_;
	    for (1 .. $ncntr) {
            my $cname     = 'ctrn-'. $_;
            my $keyoffset = 0 + $offset_id;
            my $keylen    = 100 + $_ + $current_id * $nconn + $conn_l;
            my $datalen   = $keylen + $datadiff * ( $current_id + 1 );
            my $nops      = $nops;
            my $connid    = ($conn_l -1)*$ncntr + $_;
            push(@threads, threads->new (\&ZSTrxUpdate, $connid, $chash{$cname}, $keyoffset, $keylen, $datalen, $nops));
            }
        }
        $_->join for (@threads);
        }

        for ( 1 .. $write_loop) {
        my $current_id = $_;
        @threads = ();
        for (1 .. $nconn/$ncntr) {
	    my $conn_l = $_;
	    for (1 .. $ncntr) {
            my $cname     = 'ctrn-'. $_;
            my $keyoffset = 0 + $offset_id;
            my $keylen    = 100 + $_ + $current_id * $nconn + $conn_l;
            my $datalen   = $keylen + $datadiff * ($current_id + 1 );
            my $nops      = $nops;
            my $connid    = ($conn_l -1)*$ncntr + $_;
            push(@threads, threads->new (\&verify_data, $connid, $chash{$cname}, $keyoffset, $keylen, $datalen, $nops));
            }
        }
        $_->join for (@threads);
        }

        for (1 .. $ncntr) {
            my $cname = 'ctrn-1'; 
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
    for (@cguids){
        $ret = CloseContainer ($node->conn (0), $_);
        like ($ret, qr/^OK.*/, $ret);
    }
    $ret = $node->stop ();
    like ($ret, qr/OK.*/, 'Node stop');
    }
    return;
}

sub test_init {
    $node = Fdftest::Node->new (
        ip    => "127.0.0.1",
        port  => "24422",
        threads => $nconn + 1,
		prop  => "$Bin/../../conf/zs.prop",
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
    $node->set_ZS_prop (ZS_COMPRESSION => 1);
    test_clean ();
}

