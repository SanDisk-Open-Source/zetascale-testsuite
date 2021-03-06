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
# author: Shanshan Shen 
# email: ssshen@hengtiansoft.com
# date: Jan 10, 2013
# description: 

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::BasicTest;
use Test::More tests => 91;

my $node; 

sub test_run {
    my $ret;
    my $cguid;
    my @cguids;
    my $write_suc = 0;

    #my @data = ([50, 64000, 256], [100, 128000, 256], [150, 512, 1536]);
    my @data = ([64, 16000, 100], [74, 32000, 100], [84, 64000, 100], [94, 128000, 100], [104, 48, 2000]);
    my @prop = ([4, "ZS_DURABILITY_HW_CRASH_SAFE", "no"],);

    $ret = $node->start(
               ZS_REFORMAT  => 1,gdb_switch => 1,
           );
    like($ret, qr/OK.*/, 'Node start');

    foreach my $p(@prop){
    	foreach(1..3){
    		$ret=OpenContainer($node->conn($_), "c\_$_","ZS_CTNR_CREATE",0,$$p[0],$$p[1],$$p[2]);
    		$cguid = $1 if($ret =~ /OK cguid=(\d+)/);
        	push @cguids,$cguid;
    	}
        
        my @cguids_2 = @cguids;
        foreach(my $j=3; $j>0; $j--){
            my $cguid = pop(@cguids_2);
            foreach my $d(@data){
                $ret = ZSWriteObject (
                        $node->conn (0),
                        cguid       => "$cguid",
                        key_offset  => 1000,
                        key_len     => $$d[0],
                        data_offset => 1000,
                        data_len    => $$d[1],
                        nops        => $$d[2],
                        flags       => "ZS_WRITE_MUST_NOT_EXIST",
                        );
                chomp $ret;
                if($ret =~ qr/OK.*/){
                    like($ret, qr/OK.*/,"ZSWriteObject: cguid=$cguid key_off=1000 key_len=$$d[0] data_off=1000 data_len=$$d[1] nops=$$d[2]-->".$ret);
                    $write_suc = $$d[2];
                }
                else{
                    $write_suc = $1 if ($ret =~ /.*\s+(\d+).*/);
                    like(0, qr/0/,"ZSWriteObject: cguid=$cguid key_off=1000 key_len=$$d[0] data_off=1000 data_len=$$d[1] nops=$$d[2]-->".$ret);
                }
                ReadObjects($node->conn(0),$cguid,1000,$$d[0],1000,$$d[1],$write_suc);
            }
        }
   
        my @cguids_3 = @cguids;
        foreach(my $j=3; $j>0; $j--){
        	my $cguid = pop(@cguids_3);
            foreach my $d(@data){
     	   		DeleteObjects($node->conn($j),$cguid,1000,$$d[0],$$d[2]);   
            }
        	FlushContainer($node->conn($j),$cguid);
    	}
    
        my @cguids_4 =@cguids;
        foreach(my $j=3; $j>0; $j--){
            my $cguid= pop(@cguids_4);
    	    foreach my $d(@data){
                $ret = ZSWriteObject (
                        $node->conn (0),
                        cguid       => "$cguid",
                        key_offset  => 1000,
                        key_len     => $$d[0],
                        data_offset => 1000,
                        data_len    => $$d[1],
                        nops        => $$d[2],
                        flags       => "ZS_WRITE_MUST_NOT_EXIST",
                        );
                chomp $ret;
                if($ret =~ qr/OK.*/){
                    like($ret, qr/OK.*/,"ZSWriteObject: cguid=$cguid key_off=1000 key_len=$$d[0] data_off=1000 data_len=$$d[1] nops=$$d[2]-->".$ret);
                    $write_suc = $$d[2];
                }
                else{
                    $write_suc = $1 if ($ret =~ /.*\s+(\d+).*/);
                    like(0, qr/0/,"ZSWriteObject: cguid=$cguid key_off=1000 key_len=$$d[0] data_off=1000 data_len=$$d[1] nops=$$d[2]-->".$ret);
                }
                ReadObjects($node->conn(0),$cguid,1000,$$d[0],1000,$$d[1],$write_suc);
            }
        
            FlushContainer($node->conn($j),$cguid);
            CloseContainer($node->conn($j),$cguid);
            DeleteContainer($node->conn($j),$cguid);
        }
    }    
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


