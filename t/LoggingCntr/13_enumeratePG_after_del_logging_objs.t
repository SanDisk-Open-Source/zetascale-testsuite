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

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::UnifiedAPI;
use Test::More tests =>126;

my $node; 

sub test_run {
    my ($ret,$cname,$cguid,);
    my $size = 0;
    my $ncntr = 5;
    my $cntr_type = "LOGGING";
    my @prop = ([3, "no", "ZS_DURABILITY_HW_CRASH_SAFE"],);
    my @data = ([50, "aa", "bbb", 21, 160, 50], [100, "cdefm", "z", 22, 320, 10], [150, "hhg", "os", 100, 640, 20]);
    #counter, pg, osd, valoffset, vallen, nops
    my @cguids;
    my $enum_num;

    $ret = $node->start(
               ZS_REFORMAT  => 1,
           );
    like($ret, qr/OK.*/, 'Node start');


    foreach my $p(@prop){
        for(1 .. $ncntr)
        {
            $cname = "ctr-$_";
            $ret=OpenContainer($node->conn(0), $cname, $$p[0], $size, "ZS_CTNR_CREATE", $$p[1], $$p[2], $cntr_type);
            $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
            like($ret, qr/OK.*/, $ret);
            push(@cguids, $cguid);

            $ret = EnumeratePG($node->conn(0), $cguid, 50, "aa", "bbb");
            like($ret, qr/pg=aa OK.*ZSNext: SERVER_ERROR ZS_OBJECT_UNKNOWN.*/, "$ret");
            
            #write logging objs,read enum and enum deleted objs
            foreach my $d(@data){
                $ret = WriteLogObjects($node->conn(0), $cguid, $$d[0], $$d[1], $$d[2], $$d[3], $$d[4], $$d[5], "ZS_WRITE_MUST_NOT_EXIST");
                like($ret, qr/OK.*/, $ret);
                
                $ret = ReadLogObjects($node->conn(0), $cguid, $$d[0], $$d[1], $$d[2], $$d[3], $$d[4], $$d[5]);
                like($ret, qr/OK.*/, $ret);
               
                $enum_num = $$d[5];
                $ret = EnumeratePG($node->conn(0), $cguid, $$d[0], $$d[1], $$d[2]);
                like($ret, qr/pg=$$d[1] OK.*enumerate $enum_num objects.*/, "$ret");

                #enum deleted objs
                $ret = DeleteLogObjects($node->conn(0), $cguid, $$d[0], $$d[1], $$d[2], 1);
                like($ret, qr/OK.*/, $ret);

                $enum_num = $enum_num - 1;
                $ret = EnumeratePG($node->conn(0), $cguid, $$d[0], $$d[1], $$d[2]);
                like($ret, qr/pg=$$d[1] OK.*enumerate $enum_num objects.*/, "$ret");
            }
        }

        for(1 .. $ncntr)
        {
            $cguid = pop(@cguids);
            foreach my $d(@data){
                #enum deleted objs
                $ret = DeleteLogObjects($node->conn(0), $cguid, $$d[0]+1, $$d[1], $$d[2], $$d[5]-1);
                like($ret, qr/OK.*/, $ret);

                $ret = EnumeratePG($node->conn(0), $cguid, $$d[0], $$d[1], $$d[2]);
                like($ret, qr/pg=$$d[1] OK.*ZSNext: SERVER_ERROR ZS_OBJECT_UNKNOWN.*/, "$ret");
            }
            $ret = CloseContainer($node->conn(0), $cguid);
            like($ret,qr/OK.*/,$ret);
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


