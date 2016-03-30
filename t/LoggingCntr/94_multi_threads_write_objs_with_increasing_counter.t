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
# author: Youyou Cai 
# email: youyoucai@hengtiansoft.com
# date: April 10, 2015
# description: 

#!/usr/bin/perl

use strict;
use warnings;
use threads;
use Switch;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::UnifiedAPI;
use Test::More 'no_plan'; 

my $node; 
my $ncntr = 20;
my $nconn = 5 * $ncntr;

sub worker {
    my ($connid, $cguid, $counter, $pg, $osd, $dataoffset, $datalen, $nops) = @_;
    my ($ret, $msg, $flag);
    $flag  = "ZS_WRITE_MUST_NOT_EXIST";

    $ret = WriteLogObjects($node->conn ($connid), $cguid, $counter, $pg, $osd, $dataoffset, $datalen, $nops, $flag);
    like ($ret, qr/^OK.*/, $ret);
}

sub verify_data {
    my ($connid, $cguid, $counter, $pg, $osd, $dataoffset, $datalen, $nops) = @_;
    my ($ret, $msg, $flag);

    $ret = ReadLogObjects($node->conn ($connid), $cguid, $counter, $pg, $osd, $dataoffset, $datalen, $nops);
    like ($ret, qr/^OK.*/, $ret);
}

sub test_run {
    my ($ret, $cguid,@threads);
    my $size = 0;
    my $cname;
    my %chash;
    my $ctr_type = "LOGGING";
    my @prop = ([3, "no", "ZS_DURABILITY_HW_CRASH_SAFE"],);
    

    $ret = $node->start(
               ZS_REFORMAT  => 1,
           );
    like($ret, qr/OK.*/, 'Node start');


    foreach my $p(@prop){
        for(1 .. $ncntr)
        {
            $cname = "ctr-" . $_;
	    $ret=OpenContainer($node->conn(0), $cname, $$p[0], $size, "ZS_CTNR_CREATE", $$p[1], $$p[2], $ctr_type);
	    $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
	    $chash{$cname} = $cguid;
	    like($ret, qr/OK.*/, $ret);
	    $ret=CloseContainer($node->conn(0),$cguid);
	    like($ret, qr/OK.*/, $ret);
        }

	@threads = ();
	for (1 .. $nconn/$ncntr) {
	    my $conn_loop = $_;
	    for (1 .. $ncntr){
		my $connid = ($conn_loop - 1) * $ncntr + $_;
		$cname = "ctr-" . $_;
		$cguid = $chash{$cname};
		# Inserte same descending keys to different containers.
		my $nops = 500;
		my $counter = ($conn_loop - 1)  * $nops;
		my $pg = "pg-$conn_loop";
		my $osd = "OSD";
		my $dataoffset = 0 + $conn_loop * $nops;
		my $datalen = 10 + $conn_loop * 10;
		push(@threads, threads->new (\&worker, $connid, $chash{$cname}, $counter, $pg, $osd, $dataoffset, $datalen, $nops));
	    }
	}
	$_->join for (@threads);

	@threads = ();
	for (1 .. $nconn/$ncntr) {
	    my $conn_loop = $_;
	    for (1 .. $ncntr){
		my $connid = ($conn_loop - 1) * $ncntr + $_;
		$cname = "ctr-" . $_;
		$cguid = $chash{$cname};
		# Inserte different descending keys to different containers.
		my $nops = 500;
		my $counter = ($conn_loop - 1)  * $nops;
		my $pg = "pg-$conn_loop-" . $_;
		my $osd = "OSD";
		my $dataoffset = 0 + $conn_loop * $nops;
		my $datalen = 10 + $conn_loop * 300;
		push(@threads, threads->new (\&worker, $connid, $chash{$cname}, $counter, $pg, $osd, $dataoffset, $datalen, $nops));
	    }
	}
	$_->join for (@threads);


        #restart ZS
        $ret = $node->kill();
        #like($ret, qr/OK.*/, "Node stop");
        $ret = $node->start(ZS_REFORMAT => 0,);
        like ($ret, qr/OK.*/, 'Node restart');

        for(1 .. $ncntr){
            $cname = "ctr-$_";
            $ret=OpenContainer($node->conn(0), $cname, $$p[0], $size, "ZS_CTNR_RW_MODE", $$p[1], $$p[2], $ctr_type);
            like($ret, qr/OK.*/, $ret);

        }

	@threads = ();
	for (1 .. $nconn/$ncntr) {
	    my $conn_loop = $_;
	    for (1 .. $ncntr){
		my $connid = ($conn_loop - 1) * $ncntr + $_;
		$cname = "ctr-" . $_;
		$cguid = $chash{$cname};
		# Inserte same descending keys to different containers.
		my $nops = 500;
		my $counter = ($conn_loop - 1)  * $nops;
		my $pg = "pg-$conn_loop";
		my $osd = "OSD";
		my $dataoffset = 0 + $conn_loop * $nops;
		my $datalen = 10 + $conn_loop * 10;
		push(@threads, threads->new (\&verify_data, $connid, $chash{$cname}, $counter, $pg, $osd, $dataoffset, $datalen, $nops));
	    }
	}
	$_->join for (@threads);

	@threads = ();
	for (1 .. $nconn/$ncntr) {
	    my $conn_loop = $_;
	    for (1 .. $ncntr){
		my $connid = ($conn_loop - 1) * $ncntr + $_;
		$cname = "ctr-" . $_;
		$cguid = $chash{$cname};
		# Inserte different descending keys to different containers.
		my $nops = 500;
		my $counter = ($conn_loop - 1)  * $nops;
		my $pg = "pg-$conn_loop-" . $_;
		my $osd = "OSD";
		my $dataoffset = 0 + $conn_loop * $nops;
		my $datalen = 10 + $conn_loop * 300;
		push(@threads, threads->new (\&verify_data, $connid, $chash{$cname}, $counter, $pg, $osd, $dataoffset, $datalen, $nops));
	    }
	}
	$_->join for (@threads);
    }
    
    return;

}


sub test_init {
    $node = Fdftest::Node->new(
                ip     => "127.0.0.1", 
                port   => "24422",
                nconn  => $nconn + 1,
		threads=> $nconn + 1,
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


