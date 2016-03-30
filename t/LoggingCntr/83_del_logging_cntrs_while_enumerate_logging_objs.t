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
use Test::More tests =>126;

my $node; 

sub DeleteCntr{
    my($conn, $cguid) = @_;
    
    my $ret = DeleteContainer($conn, $cguid);
    like($ret, qr/OK.*/, $ret);
}

sub EnumPG{
    my($conn, $cguid, $counter, $pg, $osd, $num) = @_;
    my $get_num = 0;
    
    my $ret = EnumeratePG($conn, $cguid, $counter, $pg, $osd);
    $get_num = $1 if($ret =~ /enumerate (\d+)/);
    
    if($get_num){
        like($ret, qr/pg=$pg OK.*enumerate $num objects.*/, "$ret");
    }
    else{#enum deleted cntr
        like($ret, qr/SERVER_ERROR ZS_CONTAINER_UNKNOWN cguid=$cguid ZSEnumeratePG.*/, "$ret");
    }
}

sub test_run {
    my ($ret,$cname,$cguid,);
    my $size = 0;
    my $ncntr = 6;
    my $cntr_type = "LOGGING";
    my @prop = ([3, "no", "ZS_DURABILITY_HW_CRASH_SAFE"],);
    my @data = ([50, "aa", "bbb", 21, 160, 500], [100, "cdefm", "z", 22, 320, 1000], [150, "hhg", "os", 100, 640, 200]);
    #counter, pg, osd, valoffset, vallen, nops
    my (@cguids,@cnames,);
    my $enum_num;
    my $ncntr_del = $ncntr/2;
    my @threads = ();

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
            push(@cnames, $cname);

            #write logging objs,read enum and enum deleted objs
            foreach my $d(@data){
                $ret = WriteLogObjects($node->conn(0), $cguid, $$d[0], $$d[1], $$d[2], $$d[3], $$d[4], $$d[5], "ZS_WRITE_MUST_NOT_EXIST");
                like($ret, qr/OK.*/, $ret);
                
                $ret = ReadLogObjects($node->conn(0), $cguid, $$d[0], $$d[1], $$d[2], $$d[3], $$d[4], $$d[5]);
                like($ret, qr/OK.*/, $ret);
               
                $enum_num = $$d[5];
                $ret = EnumeratePG($node->conn(0), $cguid, $$d[0], $$d[1], $$d[2]);
                like($ret, qr/pg=$$d[1] OK.*enumerate $enum_num objects.*/, "$ret");
            }
        }

        # delete part cntr while enum PGobjs
        my $index = 0;
        for(1 .. $ncntr_del)
        {
            $cguid = pop(@cguids);
            pop(@cnames);
            push(@threads, threads->new(\&DeleteCntr, $node->conn($index), $cguid));
            $index++;
        
            foreach my $d(@data){
                push(@threads, threads->new(\&EnumPG, $node->conn($index),$cguid,$$d[0], $$d[1], $$d[2],$$d[5]));
                $index++;
            }
        }
        foreach $cguid(@cguids){
            foreach my $d(@data){
                push(@threads, threads->new(\&EnumPG, $node->conn($index),$cguid,$$d[0], $$d[1], $$d[2],$$d[5]));
                $index++;
            }
        }

        $_->join for (@threads);

        $ret = GetContainers($node->conn(0), $ncntr-$ncntr_del);
        like($ret, qr/OK.*/, $ret);

        foreach $cguid(@cguids){
            foreach my $d(@data){
                $ret = ReadLogObjects($node->conn(0), $cguid, $$d[0], $$d[1], $$d[2], $$d[3], $$d[4], $$d[5]);
                like($ret, qr/OK.*/, $ret);
               
                $enum_num = $$d[5];
                $ret = EnumeratePG($node->conn(0), $cguid, $$d[0], $$d[1], $$d[2]);
                like($ret, qr/pg=$$d[1] OK.*enumerate $enum_num objects.*/, "$ret");
            }

            $ret = CloseContainer($node->conn(0), $cguid);
            like($ret,qr/OK.*/, $ret);
        }

        $ret = $node->stop();
        like($ret, qr/OK.*/, "Node stop");
        $ret = $node->start(ZS_REFORMAT => 0,);
        like ($ret, qr/OK.*/, 'Node restart');

        for(@cguids)
        {
            $cguid = pop(@cguids);
            $cname = pop(@cnames);
            $ret=OpenContainer($node->conn(0), $cname, $$p[0], $size, "ZS_CTNR_RW_MODE", $$p[1], $$p[2], $cntr_type);
            like($ret, qr/OK.*/, $ret);

            $ret = GetContainers($node->conn(0), $ncntr-$ncntr_del);
            like($ret, qr/OK.*/, $ret);
            
            foreach my $d(@data){
                $enum_num = $$d[5];
                $ret = EnumeratePG($node->conn(0), $cguid, $$d[0], $$d[1], $$d[2]);
                like($ret, qr/pg=$$d[1] OK.*enumerate $enum_num objects.*/, "$ret");
                
                $ret = ReadLogObjects($node->conn(0), $cguid, $$d[0], $$d[1], $$d[2], $$d[3], $$d[4], $enum_num);
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


