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
use Test::More tests =>266;

my $node; 
my $nthread = 10;
my $ncntr = 5;
my @data = ([50, "aa", "bbb", 160, 0.25], [100, "cdefm", "zzz", 320, 0.25], [150, "hhg", "osd", 640, 0.25], [200, "w", "nuyt", 1280, 0.25]);#counter,pg,osd,vallen,nops
my $cycles = 90;

sub read{
    my ($conn, $cguid, $valoff, $nops, $j) = @_;
    my $ret;

    foreach my $d(@data){
        my $osd_read = $$d[2] x $cycles;
        $ret = ReadLogObjects($conn, $cguid, $$d[0]+$nops*$$d[4]*$j, $$d[1], $osd_read, $valoff+$nops*$$d[4]*$j, $$d[3], $nops*$$d[4]);
        like($ret, qr/^OK.*/, $ret);
    }
}


sub test_run {
    my ($ret, $cguid, @threads, @cguids, $osd);
    my $size = 0;
    my $cname;
    my $ctr_type = "LOGGING";
    my $valoff = 1000;
    my $nobject = 5000;
    my @prop = ([3, "no", "ZS_DURABILITY_HW_CRASH_SAFE"],);
    

    $ret = $node->start(
               ZS_REFORMAT  => 1,
               nconn  => $nthread*$ncntr,
           );
    like($ret, qr/OK.*/, 'Node start');


    foreach my $p(@prop){
        @cguids = ();
        for(0 .. $ncntr-1)
        {
            $cname = "ctr-$_";
	    $ret=OpenContainer($node->conn(0), $cname, $$p[0], $size, "ZS_CTNR_CREATE", $$p[1], $$p[2], $ctr_type);
	    $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
	    like($ret, qr/OK.*/, $ret);
            push(@cguids, $cguid);

            foreach my $d(@data){
                my $osd = $$d[2] x $cycles;
                $ret = WriteLogObjects($node->conn(0), $cguid, $$d[0], $$d[1], $osd, $valoff, $$d[3], $nobject*$$d[4], "ZS_WRITE_MUST_NOT_EXIST");
                like($ret, qr/OK.*/, $ret);
            }
          }
        
        my $up_per_th = $nobject/$nthread;
        @threads = ();
        for my $i(0 .. $ncntr - 1){
            for my $j(0 .. $nthread - 1){
                push(@threads, threads->new(\&read, $node->conn($i*$nthread+$j),$cguids[$i],$valoff,$up_per_th,$j));    
            }
        }
        $_->join for (@threads); 
	
             

        for(0 .. $ncntr-1){
            print "cguid = $cguids[$_]\n";
            foreach my $d(@data){
                $osd = $$d[2] x $cycles;
                $ret = DeleteLogObjects($node->conn(0), $cguids[$_], $$d[0], $$d[1], $osd, $nobject*$$d[4]);
                like($ret, qr/OK.*/, $ret);

                #enumeratePG
                $ret = EnumeratePG($node->conn(0), $cguids[$_], $$d[0], $$d[1], $osd);
                like($ret, qr/OK.*/, $ret);

            }
        }


    }
    
    return;

}


sub test_init {
    $node = Fdftest::Node->new(
                ip     => "127.0.0.1", 
                port   => "24422",
                nconn  => $nthread*$ncntr,
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


