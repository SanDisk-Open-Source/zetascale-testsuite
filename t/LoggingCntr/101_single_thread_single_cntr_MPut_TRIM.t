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
use Test::More tests =>56;

my $node; 



sub test_run {
    my ($ret,$cname,$cguid,);
    my $size = 0;
    my $ncntr = 1;
    my $cntr_type = "LOGGING";
    my @prop = ([3, "no", "ZS_DURABILITY_HW_CRASH_SAFE"],);
    my @data = ([5000, "aa", "bbb", 21, 160, 500], [1000, "cdefm", "z", 22, 320, 100], [1500, "hhg", "os", 100, 640, 200], [10000,"pg", "osd",0,5,100]);
    #counter, pg, osd, valoffset, vallen, nops
    my (@cguids,);
    my $enum_num;
    my $objs_del;
    my $del;

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

            #write logging objs,and enum objs
            foreach my $d(@data){
                $ret = MPutLogObjects($node->conn(0), $cguid, $$d[0], $$d[1], $$d[2], $$d[3], $$d[4], $$d[5], "ZS_WRITE_MUST_NOT_EXIST");
                like($ret, qr/OK.*/, $ret);
               
                $enum_num = abs($$d[5]);
                $ret = EnumeratePG($node->conn(0), $cguid, $$d[0], $$d[1], $$d[2]);
                like($ret, qr/pg=$$d[1] OK.*enumerate $enum_num objects.*/, "$ret");
                
                $ret = ReadLogObjects($node->conn(0), $cguid, $$d[0], $$d[1], $$d[2], $$d[3], $$d[4], $$d[5]);
                like($ret, qr/OK.*/, $ret);
            }
        }

        #Trim 1st, 2st,midst,last one objs,and enum
        foreach $cguid(@cguids){
            foreach my $d(@data){
                $objs_del = $$d[5]/abs($$d[5]);
                $enum_num = abs($$d[5]);
                $ret = WriteLogObjects($node->conn(0), $cguid, $$d[0], $$d[1], $$d[2], $$d[3], $$d[4],$objs_del, "ZS_WRITE_TRIM");
                like($ret,qr/OK.*/, $ret);

                $enum_num = $enum_num - 1;
                $ret = EnumeratePG($node->conn(0), $cguid, $$d[0], $$d[1], $$d[2]);
                like($ret, qr/pg=$$d[1] OK.*enumerate $enum_num objects.*/, "$ret");

                #Trim 2st logging obj
                $del = abs($$d[5]/abs($$d[5]));
                $ret = WriteLogObjects($node->conn(0), $cguid, $$d[0]+$del, $$d[1], $$d[2], $$d[3]+$del, $$d[4],$objs_del, "ZS_WRITE_TRIM");
                like($ret,qr/OK.*/, $ret);

                $enum_num = $enum_num - 1;
                $ret = EnumeratePG($node->conn(0), $cguid, $$d[0], $$d[1], $$d[2]);
                like($ret, qr/pg=$$d[1] OK.*enumerate $enum_num objects.*/, "$ret");

                #Trim mid th logging obj
                $del = abs($$d[5]/2);
                $ret = WriteLogObjects($node->conn(0), $cguid, $$d[0]+$del, $$d[1], $$d[2], $$d[3]+$del, $$d[4],$objs_del, "ZS_WRITE_TRIM");
                like($ret,qr/OK.*/, $ret);
                
                $enum_num = abs($$d[5]) - $del -1;
                $ret = EnumeratePG($node->conn(0), $cguid, $$d[0], $$d[1], $$d[2]);
                like($ret, qr/pg=$$d[1] OK.*enumerate $enum_num objects.*/, "$ret");

                #Trim last obj
                $del = abs($$d[5]);
                $ret = WriteLogObjects($node->conn(0), $cguid, $$d[0]+$del, $$d[1], $$d[2], $$d[3]+$del, $$d[4],$objs_del, "ZS_WRITE_TRIM");
                like($ret,qr/OK.*/, $ret);
                
                $enum_num = 0;
                $ret = EnumeratePG($node->conn(0), $cguid, $$d[0], $$d[1], $$d[2]);
                like($ret, qr/pg=$$d[1] OK.*ZSNext: SERVER_ERROR ZS_OBJECT_UNKNOWN.*/, "$ret");
            }
        }

        #Mput same objs and enum
        foreach $cguid(@cguids){
            foreach my $d(@data){
                $ret = MPutLogObjects($node->conn(0), $cguid, $$d[0], $$d[1], $$d[2], $$d[3], $$d[4],$$d[5], "ZS_WRITE_MUST_NOT_EXIST");
                like($ret,qr/OK.*/, $ret);

                $enum_num = 0;
                $ret = EnumeratePG($node->conn(0), $cguid, $$d[0], $$d[1], $$d[2]);
                like($ret, qr/pg=$$d[1] OK.*ZSNext: SERVER_ERROR ZS_OBJECT_UNKNOWN.*/, "$ret");
            }
        }

        for(@cguids)
        {
            $cguid = pop(@cguids);
            $ret = CloseContainer($node->conn(0), $cguid);
            like($ret,qr/OK.*/,"Closecntr cguid=$cguid->$ret");
            $ret = DeleteContainer($node->conn(0), $cguid);
            like($ret, qr/OK.*/, "DeleteCntr cguid=$cguid->$ret");
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


