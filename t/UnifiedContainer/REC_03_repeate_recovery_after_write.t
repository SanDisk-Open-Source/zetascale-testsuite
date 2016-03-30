# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
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
use Test::More tests => 205;

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
    my ($keyoff, $keylen, $datalen, $nops) = (0, 50, 100,5000);
    my $write_index = 0;
    my %cguid_write_obj;
  
    $ret = $node->start(ZS_REFORMAT => 1);    
    like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=1");
   
    foreach(0..$nctr-1)
    {
        $ret = ZSOpen($node->conn(0),"ctr-$_",$choice[$_%2],0,"ZS_CTNR_CREATE","yes","ZS_DURABILITY_SW_CRASH_SAFE",$ctr_type[$_%2]);
        like ($ret, qr/^OK.*/, $ret);
        $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
        $cguids[$_]=$cguid;
        $cguid_cname{$cguid}="ctr-$_";
    }

    $cguid_write_obj{$cguid}{'obj_total'} = 0;
    foreach(0..$nctr-1)
    {
        $cguid = $cguids[$_];
        $ret = ZSSet($node->conn(0), $cguid, $keyoff, $keylen, $datalen, $nops, "ZS_WRITE_MUST_NOT_EXIST");
        like ($ret, qr/^OK.*/, $ret);
        $cguid_write_obj{$cguid}{'obj_total'} += $nops;

        $ret = ZSGet($node->conn(0), $cguid, $keyoff, $keylen, $datalen, $nops);
        like ($ret, qr/^OK.*/, $ret);

        $ret = ZSEnumerate($node->conn(0), $cguid);
        my $num_count = $1 if ($ret =~ /.*enumerate (\d+) objects.*/);
        like ($ret, qr/^OK.*/, $ret);  
        is ($cguid_write_obj{$cguid}{'obj_total'}, $num_count, "Enumerate:expect num is $cguid_write_obj{$cguid}{'obj_total'};".$ret);

        $cguid_write_obj{$cguid}{0}{'keyoff'} = $keyoff;
        $cguid_write_obj{$cguid}{0}{'keylen'} = $keylen;
        $cguid_write_obj{$cguid}{0}{'datalen'} = $datalen;
        $cguid_write_obj{$cguid}{0}{'nops'} = $nops;
    }    

   
    # start repeate recovery
    foreach(0..3)
    {
        print "======================================loop $_===============================\n";
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
            $ret = ZSOpen($node->conn(0),"ctr-$_",$choice[$_%2],0,"ZS_CTNR_RW_MODE","yes","ZS_DURABILITY_SW_CRASH_SAFE",$ctr_type[$_%2]);
            like ($ret, qr/^OK.*/, $ret);
   
            foreach(0..$write_index)
            {
                $keyoff = $cguid_write_obj{$cguid}{$_}{'keyoff'};
                $keylen = $cguid_write_obj{$cguid}{$_}{'keylen'};
                $datalen = $cguid_write_obj{$cguid}{$_}{'datalen'};
                $nops = $cguid_write_obj{$cguid}{$_}{'nops'};
                $ret = ZSGet($node->conn(0), $cguid, $keyoff, $keylen, $datalen, $nops); 
                like ($ret, qr/^OK.*/, $ret);
            }                 
 
            $ret = ZSEnumerate($node->conn(0), $cguid);
            my $num_count = $1 if ($ret =~ /.*enumerate (\d+) objects.*/);
            like ($ret, qr/^OK.*/, $ret);
            is ($cguid_write_obj{$cguid}{'obj_total'}, $num_count, "Enumerate:expect num is $cguid_write_obj{$cguid}{'obj_total'};".$ret);
        }

        $keyoff = $keyoff + 20;
        $keylen = $keylen + 10;
        $datalen = $datalen + 20;
        $write_index++;

        foreach $cguid (@cguids)
        {   
            $ret = ZSSet($node->conn(0), $cguid, $keyoff, $keylen, $datalen, $nops, "ZS_WRITE_MUST_NOT_EXIST");
            like ($ret, qr/^OK.*/, $ret);
            $cguid_write_obj{$cguid}{'obj_total'} += $nops;

            $ret = ZSGet($node->conn(0), $cguid, $keyoff, $keylen, $datalen, $nops);
            like ($ret, qr/^OK.*/, $ret);

            $ret = ZSEnumerate($node->conn(0), $cguid);
            my $num_count = $1 if ($ret =~ /.*enumerate (\d+) objects.*/);
            like ($ret, qr/^OK.*/, $ret);  
            is ($cguid_write_obj{$cguid}{'obj_total'}, $num_count, "Enumerate:expect num is $cguid_write_obj{$cguid}{'obj_total'};".$ret);

            $cguid_write_obj{$cguid}{$write_index}{'keyoff'} = $keyoff;
            $cguid_write_obj{$cguid}{$write_index}{'keylen'} = $keylen;
            $cguid_write_obj{$cguid}{$write_index}{'datalen'} = $datalen;
            $cguid_write_obj{$cguid}{$write_index}{'nops'} = $nops;
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


