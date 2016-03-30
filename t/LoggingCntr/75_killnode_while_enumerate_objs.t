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
use Test::More tests =>89;

my $node; 

sub WriteLogObjs{
    my($conn, $cguid, $counter, $pg, $osd, $val, $len, $num) = @_;
    
    my $ret = WriteLogObjects($conn, $cguid, $counter, $pg, $osd, $val, $len, $num, "ZS_WRITE_MUST_NOT_EXIST");
    like($ret, qr/OK.*/, $ret);
}

sub EnumPG{
    my($conn, $cguid, $counter, $pg, $osd,$num) = @_;
    my $get_num = 0;
    
    my $ret = EnumeratePG($conn, $cguid, $counter, $pg, $osd);
    $get_num = $1 if($ret =~ /enumerate (\d+)/);
    
    if($get_num){
        like($ret, qr/pg=$pg OK.*enumerate $num objects.*/, "$ret");
    }
    else{#enum after kill node
        like($ret, qr/cguid=$cguid ZSEnumeratePG: pg=$pg.*/, "$ret");
    }
}

sub KillNode{
    
    my $ret = $node->kill ();
}


sub test_run {
    my ($ret,$cname,$cguid,);
    my $size = 0;
    my $ncntr = 6;
    my $cntr_type = "LOGGING";
    my @prop = ([3, "no", "ZS_DURABILITY_HW_CRASH_SAFE"],);
    my @data = ([50, "aa", "bbb", 21, 160, 500], [100, "cdefm", "z", 22, 320, 100], [150, "hhg", "os", 100, 640, 200]);
    #counter, pg, osd, valoffset, vallen, nops
    my (@cguids,@cnames,);
    my $enum_num;
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

            #write logging objs,while enum objs
            foreach my $d(@data){
                WriteLogObjs( $node->conn(0), $cguid, $$d[0], $$d[1], $$d[2], $$d[3], $$d[4], $$d[5]);
                EnumPG( $node->conn(0), $cguid, $$d[0], $$d[1], $$d[2],$$d[5]);
            }
        }

        my $index = 0;
        push(@threads, threads->new(\&KillNode,));
        $index++;
        foreach $cguid(@cguids){
            foreach my $d(@data){
                push(@threads, threads->new(\&EnumPG, $node->conn($index), $cguid, $$d[0], $$d[1], $$d[2], $$d[5]));
                $index++;
            }
        }

        $_->join for (@threads);


        $ret = $node->start(ZS_REFORMAT => 0,);
        like ($ret, qr/OK.*/, 'Node restart');

        for(@cguids)
        {
            $cguid = pop(@cguids);
            $cname = pop(@cnames);
            $ret=OpenContainer($node->conn(0), $cname, $$p[0], $size, "ZS_CTNR_RW_MODE", $$p[1], $$p[2], $cntr_type);
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


