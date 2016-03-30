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
use Test::More tests => 11;

my $node;
my $nconn = 256;
my $loop  = 10;
$nconn = 4;

sub worker {
    my ($connid, $cname) = @_;
    my ($ret, $msg);
    my $size = 0;
    my $cguid;
    my @flags= ("ZS_CTNR_CREATE","ZS_CTNR_RW_MODE");
    foreach my $flag(@flags){
    $ret = ZSOpen ($node->conn ($connid), $cname, 3, $size,$flag, "no","ZS_DURABILITY_HW_CRASH_SAFE");
    #$ret =ZSOpen ($node->conn (0), $cname, 3, $size, "ZS_CTNR_CREATE|ZS_CTNR_RW_MODE","yes");
    
     like ($ret, qr/^OK.*/, $ret);

    if ($ret =~ /^OK cguid=(\d+)/) {
        $cguid = $1;
    }
    else {
        return "Error: ZSOpenContainer $ret.";
    }
  }
  
}

sub test_run {
    my $ret;
    my $cguid;
    my @threads;
    my $size = 10240;

    $ret = $node->start (
        gdb_switch   => 1,
        ZS_REFORMAT => 1,
    );
    like ($ret, qr/^OK.*/, 'Node start');

    @threads = ();
    for (0 .. $nconn) {
        my $connid = $_;
    	my $cname	= "cname1".$_;
        push(@threads, threads->new (\&worker, $_, $cname));
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

