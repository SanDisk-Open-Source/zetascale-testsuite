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
use BSD::Resource;
use Test::More tests => 1539;

my $node;
my $nconn = 512;
my $loop  = 10;

sub worker {
    my ($connid, $cguid, $keyoffset, $keylen, $datalen, $nops) = @_;
    my ($ret, $msg);
    my $size = 0;

    $ret = ZSSet ($node->conn ($connid), $cguid, $keyoffset, $keylen, $datalen, $nops);
    like ($ret, qr/^OK.*/, $ret);
    $ret = ZSFlushRandom ($node->conn ($connid), $cguid, $keyoffset);
    like ($ret, qr/^OK.*/, $ret);
    #$ret = ZSEnumerate ($node->conn ($connid), $cguid);
    #like ($ret, qr/^OK.*/, $ret);
	$ret = ZSCreateSnapshot($node->conn($connid), $cguid);
	if ( $ret =~ qr/^OK.*/ || $ret =~ /SERVER_ERROR ZS_TOO_MANY_SNAPSHOTS.*/){ 
	    like ($ret, qr/^.*/, $ret);
	}else {
		like ($ret, qr/^OK.*/, $ret);
	}
}


sub test_run {
    my $ret;
    my $cguid;
    my @threads;
    my $size = 0;
	my @snap_seq;
    setrlimit(RLIMIT_NOFILE, 4096000, 4096000);

    $ret = $node->start (
        gdb_switch   => 1,
        ZS_REFORMAT => 1,
		threads		 => $nconn,
    );
    like ($ret, qr/^OK.*/, 'Node start');

    # Create containers with $nconn connections
    my $ctrname = 'ctrn-01';
    $ret = ZSOpen ($node->conn (0), $ctrname, 3, $size, "ZS_CTNR_CREATE", "yes");
    like ($ret, qr/^OK.*/, $ret);

    if ($ret =~ /^OK cguid=(\d+)/) {
        $cguid = $1;
    }else {
        return;
    }
	
	# Getsnapshots when container is empty.
	$ret = ZSGetSnapshots($node->conn(0), $cguid);
	like ($ret, qr/^OK.*/, $ret);

	my $i = 100;
	my @keysum;
	my @valuesum;
	my @counts;
	for ( 1 .. $nconn) { 
		my ($keyoffset, $keylen, $datalen, $nops) = (0, 20 + $_,  5000+$_, 1000);
        push(@threads, threads->new (\&worker, $_, $cguid, $keyoffset, $keylen, $datalen, $nops));
	}
	$_->join for (@threads);
	
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

