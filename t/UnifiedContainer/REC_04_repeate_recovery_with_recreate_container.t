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
use Test::More tests => 116;

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
        is ($nops, $num_count, "Enumerate:expect num is $nops;".$ret);
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
      
        $ret = $node->stop();
        like($ret,qr/OK.*/,"Node Stop"); 
        $ret = $node->start(ZS_REFORMAT => 0);
        like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=0");

        ZSGetConts($node->conn(0), $nctr-1);
        like($ret, qr/^OK.*/, $ret);

        foreach(0..$nctr-1)
        {
            $cguid = $cguids[$_]; 
            if ($cguid ne "")
            { 
                $ctype = $cguid_cname{$cguid}{type};
                $ret = ZSOpen($node->conn(0),"ctr-$_",$choice[$_%2],0,"ZS_CTNR_RW_MODE","yes","ZS_DURABILITY_SW_CRASH_SAFE",$ctype);
                like ($ret, qr/^OK.*/, $ret);
                $ret = ZSGet($node->conn(0), $cguid, $keyoff, $keylen, $datalen, $nops); 
                like ($ret, qr/^OK.*/, $ret);
            }
            else
            {
                $ctype = $ctr_type[int(rand(2))];
                print "ctype = $ctype\n";
                $ret = ZSOpen($node->conn(0),"ctr-$_",$choice[$_%2],0,"ZS_CTNR_CREATE","yes","ZS_DURABILITY_SW_CRASH_SAFE",$ctype);
                like ($ret, qr/^OK.*/, $ret);
                $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
                $cguids[$_]=$cguid;
                $cguid_cname{$cguid}="ctr-$_";
                $cguid_cname{$cguid}{'type'} = $ctype;

                $ret = ZSSet($node->conn(0), $cguid, $keyoff, $keylen, $datalen, $nops, "ZS_WRITE_MUST_NOT_EXIST");
                like ($ret, qr/^OK.*/, $ret);

                $ret = ZSGetConts($node->conn(0), $nctr);
                like ($ret, qr/^OK.*/, $ret);
            }    
        }

        foreach $cguid (@cguids)
        {   
            $ret = ZSEnumerate($node->conn(0), $cguid);
            my $num_count = $1 if ($ret =~ /.*enumerate (\d+) objects.*/);
            like ($ret, qr/^OK.*/, $ret);  
            is ($nops, $num_count, "Enumerate:expect num is $nops;".$ret);

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


