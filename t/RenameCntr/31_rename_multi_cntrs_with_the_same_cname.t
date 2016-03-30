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
# author: Runhan Mao(Monica)
# email: runhanmao@hengtiansoft.com
# date: April 8, 2015
# description:


#!/usr/bin/perl

use strict;
use warnings;
use threads;
use threads::shared;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::BasicTest;
use Fdftest::Stress;
use Test::More tests => 163;

my $node; 
my $ncntr = 10;
our $a:shared;
our $check:shared;
$a=0;


sub worker_rename {
    my ($con,$cguid) = @_;
    my $ret;
    $ret=ZSRename($con, $cguid, "rename_cntr");
    if($a == 0){
	like($ret,qr/OK.*/,$ret);
	$check = $cguid;
    }
    else{
	like($ret, qr/Error.*/, $ret);
    }

    $a=$a+1;

}

sub test_run {
    my ($ret, $cguid);
    my @cguids;
    my $loop = 20;
    my $size = 0;
    my $keyoff = 1000;
    my @prop  = ([3,"ZS_DURABILITY_HW_CRASH_SAFE","no"]);
    my @data = ([64, 16000, 3000], [64, 32000, 3000], [64, 64000, 3000], [64, 128000, 3000], [64, 48, 60000]);
    my @threads = ();
    my $cname;

    $ret = $node->start(ZS_REFORMAT  => 1,);
    like($ret, qr/OK.*/, 'Node started');

    $a =0;
    foreach my $p(@prop){
        @cguids = ();
        for(0 .. $ncntr-1){
            $ret=OpenContainer($node->conn(0), "ctr-$_","ZS_CTNR_CREATE",$size,$$p[0],$$p[1],$$p[2]);
	   
                $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
                push(@cguids, $cguid);
            
                foreach my $d(@data){
                    WriteObjects($node->conn, $cguid, $keyoff, $$d[0], $keyoff, $$d[1], $$d[2],"ZS_WRITE_MUST_NOT_EXIST");
                    $keyoff = $keyoff + $$d[0]*$$d[2];
		}
            $keyoff = 1000;
        }

	$a=0;
        @threads = ();
        foreach(0..$ncntr-1)
        {
            $cname = "ctr-$_";
            push(@threads, threads->new (\&worker_rename, $node->conn(2*$_), $cguids[$_]));
            push(@threads, threads->new (\&worker_rename, $node->conn(2*$_+1), $cguids[$_]));
        }
        $_->join for (@threads);

        $ret = $node->stop();
        like($ret, qr/OK.*/, "Node stop.");

        $ret = $node->start(ZS_REFORMAT => 0);
        like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=0");
        
	$a = 0;
        for(0 .. $ncntr-1){
	    if($cguids[$_]!=$check){
            $ret=OpenContainer($node->conn(0), "ctr-$_","ZS_CTNR_RW_MODE",$size,$$p[0],$$p[1],$$p[2]);
            
            foreach my $d(@data){
                $ret=ReadObjects($node->conn(0), $cguids[$_], $keyoff, $$d[0], $keyoff, $$d[1], $$d[2]);
                $keyoff = $keyoff + $$d[0]*$$d[2];
            }
	    }
	    else {
		$ret=OpenContainer($node->conn(0), "rename_cntr","ZS_CTNR_RW_MODE",$size,$$p[0],$$p[1],$$p[2]);
		foreach my $d(@data){
                    $ret=ReadObjects($node->conn(0), $cguids[$_], $keyoff, $$d[0], $keyoff, $$d[1], $$d[2]);
                    $keyoff = $keyoff + $$d[0]*$$d[2];
		
		}
	    }

            $keyoff = 1000;
        }

        for(@cguids){
                $ret = ZSClose($node->conn(0), $_);
                like ($ret, qr/^OK.*/, $ret);
            $ret = ZSDelete($node->conn(0), $_);
            like ($ret, qr/^OK.*/, $ret);
        }   
    }
}

sub test_init {
    $node = Fdftest::Node->new(
        ip     => "127.0.0.1",
        port   => "24422",
        nconn  => $ncntr*2,
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
                
