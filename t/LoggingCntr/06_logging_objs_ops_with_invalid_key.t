# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
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
#my @data = ([50, "aa", "bbb", 160, 0.25], [100, "cdefm", "z", 320, 0.25], [150, "hhg", "os", 640, 0.25], [200, "w", "n", 1280, 0.25]);#counter,pg,osd,vallen,nops
my @data = (["100", "aa", "bbb", 160, 0.25], [100, "_", "z", 320, 0.25], [150, "hhg", "", 640, 0.25], ["25", "_", "n", 1280, 0.25]);#counter,pg,osd,vallen,nops


sub read{
    my ($conn, $cguid, $valoff, $nops, $j) = @_;
    my $ret;

    foreach my $d(@data){
        $ret = ReadLogObjects($conn, $cguid, $$d[0]+$nops*$$d[4]*$j, $$d[1], $$d[2], $valoff+$nops*$$d[4]*$j, $$d[3], $nops*$$d[4]);
        like($ret, qr/^OK.*/, $ret);
    }
}


sub test_run {
    my ($ret, $cguid, @threads, @cguids);
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
                $ret = WriteLogObjects($node->conn(0), $cguid, $$d[0], $$d[1], $$d[2], $valoff, $$d[3], $nobject*$$d[4], "ZS_WRITE_MUST_NOT_EXIST");
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
            foreach my $d(@data){
                $ret = DeleteLogObjects($node->conn(0), $cguids[$_], $$d[0], $$d[1], $$d[2], $nobject*$$d[4]);
                like($ret, qr/OK.*/, $ret);

                #enumeratePG
                $ret = EnumeratePG($node->conn(0), $cguids[$_], $$d[0], $$d[1], $$d[2]);
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


