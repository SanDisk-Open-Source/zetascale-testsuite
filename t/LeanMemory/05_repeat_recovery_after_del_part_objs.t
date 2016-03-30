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
# author: yiwen lu
# email: yiwenlu@hengtiansoft.com
# date: July 10, 2014
# description:

#!/usr/bin/perl

#use strict;
use warnings;
use Switch;
use threads;


use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::Stress;
use Test::More tests => 388;
#use Test::More 'no_plan';

my $node; 
my $nconn = 1;
#my $write_loop = 3;
my %cguid_write_obj;

sub test_run {
    my ($ret, $cguid, @cguids, @threads);
    my $nctr = 2*2;
    my $size = 0;
    my @prop = ([3, "no", "ZS_DURABILITY_HW_CRASH_SAFE" ],);
    #my @data = ([50, 64000, 625], [100, 128000, 625], [150, 512, 3750]);
    my @data = ([64, 16000, 300], [74, 32000, 300], [84, 64000, 300], [94, 128000, 300], [104, 48, 6000]);
    my $write_loop = @data;

    $ret = $node->start(ZS_REFORMAT => 1);    
    like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=1");

  foreach my $p(@prop){ 
    foreach(0..$nctr-1)
    {
        $ret = ZSOpen($node->conn(0),"ctr-$_",$$p[0],$size,"ZS_CTNR_CREATE",$$p[1],$$p[2]);
        like ($ret, qr/^OK.*/, $ret);
        $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
        $cguids[$_]=$cguid;
    }

    foreach(0..$nctr-1)
    {
        #my ($keyoff, $keylen, $datalen, $nops) = (1000, 50, 65000, 5000);
        $cguid = $cguids[$_]; 
        $cguid_write_obj{$cguid}{'obj_total'} = 0;
        foreach(0..$write_loop-1)
        {
            my $d = $data[$_];
            my ($keyoff, $keylen, $datalen, $nops) = (1000, $$d[0], $$d[1], $$d[2]);

            $ret = ZSSet($node->conn(0), $cguid, $keyoff, $keylen, $datalen, $nops, "ZS_WRITE_MUST_NOT_EXIST");
            like ($ret, qr/^OK.*/, $ret);
            $cguid_write_obj{$cguid}{'obj_total'} += $nops;

            $ret = ZSGet($node->conn(0), $cguid, $keyoff, $keylen, $datalen, $nops);
            like ($ret, qr/^OK.*/, $ret);

            $ret = ZSEnumerate($node->conn(0), $cguid);
            my $num_count = $1 if ($ret =~ /.*enumerate (\d+) objects.*/);
            like ($ret, qr/^OK.*/, $ret);  
            is ($cguid_write_obj{$cguid}{'obj_total'}, $num_count, "Enumerate:expect num is $cguid_write_obj{$cguid}{'obj_total'};".$ret);

            $cguid_write_obj{$cguid}{$_}{'keyoff'} = $keyoff;
            $cguid_write_obj{$cguid}{$_}{'keylen'} = $keylen;
            $cguid_write_obj{$cguid}{$_}{'datalen'} = $datalen;
            $cguid_write_obj{$cguid}{$_}{'nops'} = $nops;
            #$keyoff = $keyoff + 20;
            #$keylen = $keylen + 10;
        }
    }

    foreach(0..$nctr*$write_loop/2-1)
    {
        print "====================loop $_=================\n";
        while(1){
            $cguid = $cguids[rand($nctr)];
            $write_index = int(rand($write_loop));
            #print "keyoff=$cguid_write_obj{$cguid}{$write_index}{'keyoff'}\n";
            if ($cguid_write_obj{$cguid}{$write_index}{'keyoff'} ne "")
            {
                last;
            }
            print "keyoff is empty,continue\n";
        }
        my $koff = $cguid_write_obj{$cguid}{$write_index}{'keyoff'};
        my $klen = $cguid_write_obj{$cguid}{$write_index}{'keylen'};
        my $dlen = $cguid_write_obj{$cguid}{$write_index}{'datalen'};
        my $nps = $cguid_write_obj{$cguid}{$write_index}{'nops'};
        
        $ret = ZSDel($node->conn(0), $cguid, $koff, $klen, $nps);
        like ($ret, qr/^OK.*/, $ret);
        $cguid_write_obj{$cguid}{'obj_total'} -= $nps;
        $cguid_write_obj{$cguid}{$write_index}{'keyoff'} = "";
 
        $ret = $node->stop();
        like($ret,qr/OK.*/,"Node Stop"); 
        $ret = $node->start(ZS_REFORMAT => 0);
        like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=0");

        foreach(0..$nctr-1)
        {
            $ret = ZSOpen($node->conn(0),"ctr-$_",$$p[0],$size,"ZS_CTNR_RW_MODE",$$p[1],$$p[2]);
            like ($ret, qr/^OK.*/, $ret);
            
            $cguid = $cguids[$_];
            foreach (0..$write_loop-1)
            {
               if($cguid_write_obj{$cguid}{$_}{'keyoff'} eq "")
               {
                   next;
               } 
               my $koff = $cguid_write_obj{$cguid}{$_}{'keyoff'};
               my $klen = $cguid_write_obj{$cguid}{$_}{'keylen'};
               my $dlen = $cguid_write_obj{$cguid}{$_}{'datalen'};
               my $nps = $cguid_write_obj{$cguid}{$_}{'nops'};
 
               $ret = ZSGet($node->conn(0), $cguid, $koff, $klen, $dlen, $nps);
               like ($ret, qr/^OK.*/, $ret);
            }

            if($cguid_write_obj{$cguid}{'obj_total'} != 0)
            { 
                $ret = ZSEnumerate($node->conn(0), $cguid);
                my $num_count = $1 if ($ret =~ /.*enumerate (\d+) objects.*/);
                like ($ret, qr/^OK.*/, $ret);  
                is ($cguid_write_obj{$cguid}{'obj_total'}, $num_count, "Enumerate:expect num is $cguid_write_obj{$cguid}{'obj_total'};".$ret);
            }
        }
       
    }

    foreach $cguid (@cguids)
    {
        $ret = ZSClose($node->conn(0), $cguid);
        like ($ret, qr/^OK.*/, $ret);  
        $ret = ZSDelete($node->conn(0), $cguid);
        like ($ret, qr/^OK.*/, $ret);  
    }
    }
    return;
}

sub test_init {
    $node = Fdftest::Node->new(
                ip     => "127.0.0.1", 
                port   => "24422",
                nconn  => $nconn,
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


