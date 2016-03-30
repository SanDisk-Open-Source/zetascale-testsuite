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
use Test::More tests => 480;

#tests =( 6*($nconn+1) + 2) * ($loop+1) + 4
my $node;
my $nconn = 256;
my $loop  = 10;
$nconn = 10;
$loop  = 5;
my @data = ([50, 64000, 1], [100, 128000, 1], [150, 512, 6]);

sub worker {
    my ($connid, $cguid, $keyoffset, $nops) = @_;
    my ($ret, $msg);

    foreach my $d(@data){
        $ret = ZSSetGet ($node->conn ($connid), $cguid, $keyoffset, $$d[0], $$d[1], $nops*$$d[2]);
        like ($ret, qr/^OK.*/, $ret);
    }
    $ret = ZSFlushRandom ($node->conn ($connid), $cguid, $keyoffset);
    like ($ret, qr/^OK.*/, $ret);
    $ret = ZSEnumerate ($node->conn ($connid), $cguid);
    like ($ret, qr/^OK.*/, $ret);
    $ret = ZSGetProps ($node->conn ($connid), $cguid);
    $msg = substr($ret, 0, index($ret, "ZSGet"));
    like ($ret, qr/^OK.*/, $msg . "ZSGetContainerProps");
    $ret = ZSGetConts ($node->conn ($connid), 1);
    $msg = substr($ret, 0, index($ret, "ZSGet"));
    like ($ret, qr/^OK.*/, $msg . "ZSGetContainers");
}

sub test_run {
    my $ret;
    my $cguid;
    my @threads;
    my $size = 0;
    my @prop = ([3, "ZS_DURABILITY_HW_CRASH_SAFE", "no"],);

    $ret = $node->start (
        gdb_switch   => 1,
        ZS_REFORMAT => 1,
    );
    like ($ret, qr/^OK.*/, 'Node start');

    foreach my $p(@prop){
        # Create containers with $nconn connections
        my $ctrname = 'ctrn-01';
        $ret = ZSOpen ($node->conn (0), $ctrname, $$p[0], $size, "ZS_CTNR_CREATE", $$p[2], $$p[1]);
        like ($ret, qr/^OK.*/, $ret);

        if ($ret =~ /^OK cguid=(\d+)/) {
            $cguid = $1;
        }
        else {
            return;
        }

        for (0 .. $loop) {
            my $keyoffset = 0 + $_ * $nconn;
            my ($keylen, $datalen, $maxops, $nops);
            @threads = ();
            for (0 .. $nconn) {
                my $connid = $_;
                $keyoffset = $keyoffset + $_;
                #$maxops    = int((1000) / (($datalen) / 1000000));
                #$nops      = int(rand($maxops/$nconn));
                $nops      = 10; 
                push(@threads, threads->new (\&worker, $_, $cguid, $keyoffset, $nops, 0));
            }
            $_->join for (@threads);

            #=comment
            $ret = ZSClose ($node->conn (0), $cguid);
            like ($ret, qr/^OK.*/, $ret);
            $ret = ZSOpen ($node->conn (0), $ctrname, $$p[0], $size, "ZS_CTNR_RW_MODE", $$p[2], $$p[1]);
            like ($ret, qr/^OK.*/, $ret);

            #=cut
        }

        $ret = ZSGetConts ($node->conn (0), 1);
        chomp($ret);
        like ($ret, qr/^OK.*/, $ret);
        $ret = ZSClose ($node->conn (0), $cguid);
        like ($ret, qr/^OK.*/, $ret);

        $ret = ZSDelete ($node->conn (0), $cguid);
        like ($ret, qr/^OK.*/, $ret);
        $node->stop ();
        $node->set_ZS_prop (ZS_REFORMAT => 1);

	    $ret = $node->start (
    	    gdb_switch   => 1,
        	ZS_REFORMAT => 1,
	    );
    	like ($ret, qr/^OK.*/, 'Node start');
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

