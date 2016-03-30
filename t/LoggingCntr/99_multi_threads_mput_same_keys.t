# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file: 
# author: shujing zhu
# email: shujingzhu@hengtiansoft.com
# date: April 16, 2015
# description: 

#!/usr/bin/perl

use strict;
use warnings;
use threads;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::UnifiedAPI;
use Test::More tests =>16;

my $node; 


sub EnumPG{
    my($conn, $cguid, $counter, $pg, $osd,$num) = @_;
    my $get_num = 0;
    
    my $ret = EnumeratePG($conn, $cguid, $counter, $pg, $osd);
    $get_num = $1 if($ret =~ /enumerate (\d+)/);
    
    if($get_num >= $num){
        like($ret, qr/pg=$pg OK.*enumerate $get_num objects.*/, "$ret");
    }
    else{
        like($ret, qr/pg=$pg OK.*enumerate $num objects.*/, "$ret");
    }
}

sub MPutLogObjs{
    my($conn, $cguid, $counter, $pg, $osd, $val, $len, $num) = @_;

    my $ret = MPutLogObjects($conn, $cguid, $counter, $pg, $osd, $val, $len, $num);
    like ($ret, qr/OK.*/, $ret);
    
}


sub test_run {
    my ($ret,$cname,$cguid,);
    my $size = 0;
    my $ncntr = 1;
    my $cntr_type = "LOGGING";
    my @prop = ([3, "no", "ZS_DURABILITY_HW_CRASH_SAFE"],);
    my @data = ([5000, "aa", "bbb", 21, 160, -500], [1000, "aa", "z", 22, 320, 100], [1500, "aa", "bb", 100, 640, 200], [10000,"aa", "z",0,5,-100]);
    #counter, pg, osd, valoffset, vallen, nops
    my (@cguids,@enums,);
    my $enum_num;
    my @threads = ();

    $ret = $node->start(
               ZS_REFORMAT  => 1,
           );
    like($ret, qr/OK.*/, 'Node start');


    foreach my $p(@prop){
        my $index = 0;
        for(1 .. $ncntr)
        {
            $cname = "ctr-$_";
            $ret=OpenContainer($node->conn(0), $cname, $$p[0], $size, "ZS_CTNR_CREATE", $$p[1], $$p[2], $cntr_type);
            $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
            like($ret, qr/OK.*/, $ret);
            push(@cguids, $cguid);

            $enum_num = 0;
            #multi thread mput same pg bjs
            foreach my $d(@data){
                push(@threads, threads->new(\&MPutLogObjs, $node->conn($index), $cguid, $$d[0], $$d[1], $$d[2], $$d[3], $$d[4], $$d[5]));
                $index++;
                $enum_num = $enum_num + abs($$d[5]);

            }
            push(@enums, $enum_num);
        }

        $_->join() for(@threads);

        for(@cguids){
            $cguid = pop(@cguids);
            $enum_num = pop(@enums);

            foreach my $d(@data){

                $ret = EnumeratePG($node->conn(0), $cguid, $$d[0], $$d[1], $$d[2]);
                like($ret, qr/pg=$$d[1] OK.*enumerate $enum_num objects.*/, "$ret");
                
                $ret = ReadLogObjects($node->conn(0), $cguid, $$d[0], $$d[1], $$d[2], $$d[3], $$d[4], $$d[5]);
                like($ret, qr/OK.*/, $ret);
            }
            
            $ret = CloseContainer($node->conn(0), $cguid);
            like($ret,qr/OK.*/, $ret);
            $ret = DeleteContainer($node->conn(0), $cguid);
            like($ret, qr/OK.*/, $ret);
        }

    }
    
    return;

}


sub test_init {
    $node = Fdftest::Node->new(
                ip     => "127.0.0.1", 
                port   => "24422",
                nconn  => 20,
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


