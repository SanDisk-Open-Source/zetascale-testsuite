# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file:
# author: yiwen lu
# email: yiwenlu@hengtiansoft.com
# date: Jan 3, 2013
# description:

#!/usr/bin/perl

use strict;
use warnings;
use Switch;


use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::Stress;
use Test::More ;

my $node; 
sub test_run {
    my $cguid;
    my $ret; 
    my @cguids;    
    my %cguid_cname;
    my $nctr = 128;
    my $loop = 1000;
    my $rand;
    my $option;
    my %cguid_key_data;
    my $ctr_count=0;    
    my @ctr_type = ("BTREE","HASH");    
    $ret = $node->start(ZS_REFORMAT => 1);    
    like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=1");
    
    foreach(0..$nctr-1)
    {
        $ret = ZSOpen($node->conn(0),"ctr-$_",int(rand(8)),0,"ZS_CTNR_CREATE","yes","ZS_DURABILITY_SW_CRASH_SAFE",$ctr_type[rand(2)]);
        like ($ret, qr/^OK.*/, $ret);
        $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
        $cguids[$_]=$cguid;
        $cguid_cname{$cguid}="ctr-$_";
        $ctr_count++;
    }
    print "ctr_count=$ctr_count";
    foreach (keys %cguid_cname)
    {
        print "$_ -- $cguid_cname{$_}\n";
    }
    
    
    for(my $i=0; $i<$loop; $i++){
        print "=loop:$i=\n";
        while(1){
            $rand=int(rand($nctr));
            $cguid=$cguids[$rand];
            if($cguid ne ""){
                $option=int(rand(6));
                last;
            }
            else{
                $option=6;
                last;
            }
        }
       
        print "<rand=$rand--cguid=$cguid---$option>\n"; 
        switch($option) {
            case (0) {
                print "Close&DeleteContainer\n";
                $ret = ZSClose($node->conn(0),$cguid);
                like ($ret, qr/^OK.*/, $ret);
                $ret = ZSDelete($node->conn(0),$cguid);
                like ($ret, qr/^OK.*/, $ret);
                $cguids[$rand] = "";
                delete $cguid_cname{"$cguid"};
                delete $cguid_key_data{"$cguid"};
                $ctr_count--;
            }
            case (1) {
                print "GetContainers\n";
                $ret = ZSGetConts($node->conn(0),$ctr_count);
                like ($ret, qr/^OK.*/, $ret);
            }
            case (2) {
                print "WriteReadObjects\n";
                print "cguid=$cguid,rand=$rand\n";
                if(exists $cguid_key_data{$cguid}){
                    print "container is not empty!\n";
                    last;
                }
                my $keyoff = int(rand(10000));
                my $keylen = int(rand(240))+10;
                my $datalen = int(rand(200000))+100;
                my $nops = int(1048576*1024/$datalen/5);
                print "($keyoff,$keylen,$keyoff,$datalen,$nops)\n";
                $ret = ZSSet($node->conn(0),$cguid,$keyoff,$keylen,$datalen,$nops,0);
                like ($ret, qr/^OK.*/, $ret);
                $ret = ZSGet ($node->conn(0),$cguid,$keyoff,$keylen,$datalen,$nops);
                like ($ret, qr/^OK.*/, $ret);
                $cguid_key_data{$cguid}[0]=$keyoff;
                $cguid_key_data{$cguid}[1]=$keylen;
                $cguid_key_data{$cguid}[2]=$keyoff;
                $cguid_key_data{$cguid}[3]=$datalen;
                $cguid_key_data{$cguid}[4]=$nops;
            }
            case (3) {
                print "EnumerateObjects\n";
                if(exists $cguid_key_data{$cguid}){  
                    $ret = ZSEnumerate($node->conn(0),$cguid,$cguid_key_data{$cguid}[4]);      
                    like ($ret, qr/^OK.*/, $ret);
                }else{
                    print "no objects exit\n";
                }
            } 
            case (4) {
                $ret = ZSFlushRandom($node->conn(0),$cguid,0,1);
                like ($ret, qr/^OK.*/, $ret);
            }
            case (5) {
                if(exists $cguid_key_data{$cguid}){
                    $ret = ZSDel($node->conn(0),$cguid,$cguid_key_data{$cguid}[0],$cguid_key_data{$cguid}[1],$cguid_key_data{$cguid}[4]);
                    like ($ret, qr/^OK.*/, $ret);
                    delete $cguid_key_data{$cguid};
                }else{
                    print "no objects exist\n";
                }
            }
            case (6) {
                $ret = ZSOpen($node->conn(0),"ctr-$rand",int(rand(8)),0,"ZS_CTNR_CREATE","yes","ZS_DURABILITY_SW_CRASH_SAFE",$ctr_type[rand(2)]);
                like ($ret, qr/^OK.*/, $ret);
                $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
                $cguids[$rand]=$cguid;
                $cguid_cname{$cguid}="ctr-$rand";
                $ctr_count++;
            }
            else{
                print "do nothing!\n";
            }
        }
       
        print "---ctr_count=$ctr_count\n"; 
        if(! %cguid_cname){
            print "No Container any more!";
            return;
        }

    }
    done_testing();
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


