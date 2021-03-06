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
use Fdftest::Stress;
use Fdftest::Node;
#use Test::More tests => 3601;
use Test::More tests => 421;

#tests =( 6*($nconn) + 2*$ncntr) * ($loop) + $ncntr*2 + 1
my $node;
my $nconn = 500;
my $ncntr = 1000;
my $loop  = 1;
$nconn = 600;
$ncntr = 200;
$loop  = 200;

sub worker {
    my ($connid, $cname) = @_;
    my ($ret, $msg);
    my $size = 1024; 
	my $cguid;

    $ret = ZSGetConts ($node->conn ($connid));
    $msg = substr($ret, 0, index($ret, "ZSGet"));
    like ($ret, qr/^OK.*/, $msg . "ZSGetContainers");

    $ret = ZSOpen ($node->conn ($connid), $cname, 3, $size, "ZS_CTNR_CREATE", "yes");
    like ($ret, qr/^OK.*/, $ret);
    if ($ret =~ /^OK cguid=(\d+)/) {
		$cguid = $1;
	}

    #$ret = ZSSet ($node->conn ($connid), $cguid, 0, 12, 100, 50);
    #like ($ret, qr/^OK.*/, $ret);
}

sub test_run {
    my ($ret, $msg);
    my ($cguid, @cguids, %chash, @threads);
    my $size = 10;
    $ret = $node->start (
        gdb_switch   => 1,
        ZS_REFORMAT => 1,
    );
    like ($ret, qr/^OK.*/, 'Node start');

    for (1 .. $loop) {
		my $cname='test'.$_;
		worker(0, $cname);
    	#push(@threads, threads->new (\&worker, $_, $cname));
    }
    #$_->join for (@threads);
	
	for (1 .. 10) {
		my $cname='aaa'.$_;
    	$ret = ZSOpen ($node->conn (0), $cname, 3, 1024, "ZS_CTNR_CREATE", "yes");
	    like ($ret, qr/^OK.*/, $ret);
	    if ($ret =~ /^OK cguid=(\d+)/) {
			$cguid = $1;
		}
		
		$ret = ZSDelete ($node->conn(0), $cguid);
	    like ($ret, qr/^OK.*/, $ret);
	}

    return;
}

sub test_init {
    $node = Fdftest::Node->new (
        ip    => "127.0.0.1",
        port  => "24422",
        nconn => $nconn,
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

# clean ENV
END {
    $node->clean ();
}

