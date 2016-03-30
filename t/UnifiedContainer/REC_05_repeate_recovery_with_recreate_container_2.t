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
use Test::More tests => 158;

my $node; 
my $nconn = 1;

sub test_run {
    my $cguid;
    my $ret;
    my @cguids;
    my @threads;    
    my %cguid_cname;

    my $nctr = 2*2;
    my @choice = (3,5); 
    my @ctr_type = ("BTREE","HASH");    
    my $ctype;
    my ($keyoff, $keylen, $datalen, $nops) = (0, 50, 100,5000);
    my %cguid_write_obj;
  
    $ret = $node->start(ZS_REFORMAT => 1);    
    like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=1");
   
    foreach(0..$nctr-1)
    {
        $ctype = $ctr_type[int(rand(2))];
        $ret = ZSOpen($node->conn(0),"ctr-$_",$choice[$_%2],0,"ZS_CTNR_CREATE","yes","ZS_DURABILITY_SW_CRASH_SAFE",$ctype);
        like ($ret, qr/^OK.*/, $ret);
        $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
        $cguids[$_]=$cguid;
        $cguid_cname{$cguid}="ctr-$_";
        $cguid_cname{$cguid}{'type'} = $ctype;
    }

    foreach(0..$nctr-1)
    {
        $cguid = $cguids[$_];
        $ret = ZSSet($node->conn(0), $cguid, $keyoff, $keylen, $datalen, $nops, "ZS_WRITE_MUST_NOT_EXIST");
        like ($ret, qr/^OK.*/, $ret);
        $ret = ZSGet($node->conn(0), $cguid, $keyoff, $keylen, $datalen, $nops);
        like ($ret, qr/^OK.*/, $ret);
        $ret = ZSEnumerate($node->conn(0), $cguid);
        my $num_count = $1 if ($ret =~ /.*enumerate (\d+) objects.*/);
        like ($ret, qr/^OK.*/, $ret);  
        is ($num_count, $nops, "Enumerate:expect num is $nops;".$ret);
    }    

   
    # start repeate recovery
    my $rep_time = 3;
    foreach(0..$rep_time-1)
    {
        print "======================================loop $_===============================\n";
        foreach $cguid (@cguids)
        {
            if ($cguid ne "")
            { 
                $ret = ZSClose($node->conn(0), $cguid);
                like ($ret, qr/^OK.*/, $ret);
            }
        }
       
        my $rand = int(rand($nctr));
        print "rand = $rand\n";
        $cguid = $cguids[$rand];
        $ret = ZSDelete($node->conn(0), $cguid);
        like ($ret, qr/^OK.*/, $ret);  
        $cguids[$rand] = "";
        delete $cguid_cname{$cguid};
      
        $ret = ZSGetConts($node->conn(0), $nctr-1);
        like ($ret, qr/^OK.*/, $ret);

        foreach (0..$nctr-1)
        {   
            $cguid = $cguids[$_];
            $ctype = $cguid_cname{$cguid}{type};
            if($cguid ne "")
            {
                $ret = ZSOpen($node->conn(0),"ctr-$_",$choice[$_%2],0,"ZS_CTNR_RW_MODE","yes","ZS_DURABILITY_SW_CRASH_SAFE",$ctype);
                like ($ret, qr/^OK.*/, $ret);
                $ret = ZSEnumerate($node->conn(0), $cguid);
                my $num_count = $1 if ($ret =~ /.*enumerate (\d+) objects.*/);
                like ($ret, qr/^OK.*/, $ret);  
                is ($nops, $num_count, "Enumerate:expect num is $nops;".$ret);
            }
        }
         
        $ctype = $ctr_type[int(rand(2))];
        $ret = ZSOpen($node->conn(0),"ctr-$rand",$choice[$_%2],0,"ZS_CTNR_CREATE","yes","ZS_DURABILITY_SW_CRASH_SAFE",$ctype);
        like ($ret, qr/^OK.*/, $ret);
        $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
        $cguids[$rand]=$cguid;
        $cguid_cname{$cguid}="ctr-$rand";
        $cguid_cname{$cguid}{'type'} = $ctype;

        foreach $cguid (@cguids)
        {
            $ret = ZSClose($node->conn(0), $cguid);
    	    like ($ret, qr/^OK.*/, $ret);
        }

        $ret = $node->stop();
        like($ret,qr/OK.*/,"Node Stop"); 
        $ret = $node->start(ZS_REFORMAT => 0);
        like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=0");

        foreach(0..$nctr-1)
        { 
            $cguid = $cguids[$_]; 
            $ctype = $cguid_cname{$cguid}{'type'};
            $ret = ZSOpen($node->conn(0),"ctr-$_",$choice[$_%2],0,"ZS_CTNR_RW_MODE","yes","ZS_DURABILITY_SW_CRASH_SAFE",$ctype);
            like ($ret, qr/^OK.*/, $ret);
            if($rand == $_)
            {
                $ret = ZSSet($node->conn(0), $cguid, $keyoff, $keylen, $datalen, $nops, "ZS_WRITE_MUST_NOT_EXIST");
                like ($ret, qr/^OK.*/, $ret);
            }         
            $ret = ZSGet($node->conn(0), $cguid, $keyoff, $keylen, $datalen, $nops); 
            like ($ret, qr/^OK.*/, $ret);
        }

        foreach $cguid (@cguids)
        {   
            $ret = ZSEnumerate($node->conn(0), $cguid);
            my $num_count = $1 if ($ret =~ /.*enumerate (\d+) objects.*/);
            like ($ret, qr/^OK.*/, $ret);  
            is ($num_count, $nops, "Enumerate:expect num is $nops;".$ret);

            ZSFlushRandom($node->conn(0), $cguid, 0, 1);
            like ($ret, qr/^OK.*/, $ret);
        }

    }

    foreach $cguid (@cguids)
    {
        $ret = ZSClose($node->conn(0), $cguid);
        like ($ret, qr/^OK.*/, $ret);  
        $ret = ZSDelete($node->conn(0), $cguid);
        like ($ret, qr/^OK.*/, $ret);  
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


