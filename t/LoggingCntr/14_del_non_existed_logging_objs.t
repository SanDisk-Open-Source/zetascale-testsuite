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
use Test::More tests =>26;

my $node; 
my $ncntr = 5;
my @data = ([50, "aa", "bbb", 160, 0.25], [100, "cdefm", "zzz", 320, 0.25], [150, "hhg", "osd", 640, 0.25], [200, "w", "nuyt", 1280, 0.25]);#counter,pg,osd,vallen,nops


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
        }

        for(0 .. $ncntr-1){
            #print "cguid= $_\n";
            foreach my $d(@data){
                $ret = DeleteLogObjects($node->conn(0), $cguids[$_], $$d[0], $$d[1], $$d[2], $nobject*$$d[4]);
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
                nconn  => 1,
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


