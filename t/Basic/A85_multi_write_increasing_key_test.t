# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file:
# author: Jie,Wang
# email: jiewang@hengtiansoft.com
# date: Feb 11, 2015
# description:

#!/usr/bin/perl

use strict;
use warnings;
use Switch;
use threads;


use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::Stress;
use Test::More tests => 6607;

my $node; 
my $nconn = 128;
#my @data = ([150, 64000, 1000], [150, 128000, 1000], [150, 512, 6000]);
my @data = ([64, 16000, 500], [64, 32000, 500], [64, 64000, 500], [64, 128000, 500], [64, 48, 10000]);
my $keyoff = 50;
my $num = 20;
sub worker_write{
	my($con,$cguid,$keyoff)=@_;
	my $ret;
	foreach(0..$num-1)
	{
        	foreach my $d(@data)
		{
        		my ($keylen,$datalen,$nops)=($$d[0],$$d[1],$$d[2]);
			
			$ret = ZSWriteObject(
				$node->conn($con),
				cguid		=>$cguid,
				key		=>$keyoff,
				key_len		=>$keylen,
				data_offset	=>$keyoff,
				data_len	=>$datalen,
				nops		=>$nops,
				flags		=>"ZS_WRITE_MUST_NOT_EXIST",
				);
        		like ($ret, qr/^OK.*/, "OK connection=$con ZSWriteObject cguid=$cguid key=$keyoff,keylen=$keylen,datalen=$datalen,nops=$nops");
			$keyoff = $keyoff+$nops;
		}
	}
}

sub worker_read{
	my($con,$cguid,$keyoff)=@_;
	my $ret;
	foreach(0..$num-1)
	{
        	foreach my $d(@data)
		{
			my ($keylen,$datalen,$nops)=($$d[0],$$d[1],$$d[2]);
			$ret = ZSReadObject(
				$node->conn($con),
				cguid		=>$cguid,
				key		=>$keyoff,
				key_len		=>$keylen,
				data_offset	=>$keyoff,
				data_len	=>$datalen,
				nops		=>$nops,
				check		=>"yes",
				);
        		like ($ret, qr/^OK.*/, "OK ZSReadObject cguid=$cguid key=$keyoff,keylen=$keylen,datalen=$datalen,nops=$nops");
			$keyoff = $keyoff+$nops;
		}
	}
}

sub worker_delete{
	my($con,$cguid,$keyoff)=@_;
	my $ret;

	foreach(0..$num-1)
	{
		foreach my $d(@data)
		{
			my ($keylen,$nops)=($$d[0],$$d[2]);
			$ret = ZSDeleteObject(
				$node->conn($con),
				cguid		=>$cguid,
				key		=>$keyoff,
				key_len		=>$keylen,
				nops		=>$nops,
			);
		 	like ($ret,qr/^OK.*/,"OK ZSDeleteObject cguid=$cguid key=$keyoff,keylen=$keylen,nops=$nops");
			$keyoff = $keyoff+$nops;
		
		}
	}	
}

sub test_run {
    my $cguid;
    my $ret;
    my @cguids;
    my @threads;    
    my %cguid_cname;
    my $nctr = 2;
    my @ctr_type = ("BTREE","BTREE");   
    my @prop = (["yes","no","yes","ZS_DURABILITY_HW_CRASH_SAFE","no"],);

    $ret = $node->start(ZS_REFORMAT => 1);    
    like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=1");
   
    foreach(0..$nctr-1)
    {
            $ret = ZSOpen($node->conn(0),"ctr-$_",3,0,"ZS_CTNR_CREATE","yes","ZS_DURABILITY_HW_CRASH_SAFE",$ctr_type[$_%2]);
            like ($ret, qr/^OK.*/, $ret);
            $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
            $cguids[$_]=$cguid;
            $cguid_cname{$cguid}="ctr-$_";
    }
	
    @threads = ();
    foreach my $ctr(0..$nctr-1) 
    {
            my $cguid = $cguids[$ctr];
	    foreach(0..10)
	    {
	    	push(@threads,threads->new(\&worker_write,11*$ctr+$_,$cguid,$keyoff));
	 	$keyoff = $keyoff+($num*12000+1);   
	    }
    }
    $_->join for (@threads);
	
    @threads = ();
    $keyoff = 50;
    foreach my $ctr(0..$nctr-1)
    {
	    my $cguid = $cguids[$ctr];
	    foreach(0..10)
	    {
	    	push(@threads,threads->new(\&worker_read,11*$ctr+$_,$cguid,$keyoff));
		$keyoff = $keyoff+($num*12000+1);
	    }
    }

    $_->join for (@threads);
    
    @threads = ();
    $keyoff = 50;
    foreach my $ctr(0..$nctr-1)
    {
	    my $cguid = $cguids[$ctr];
	    foreach(0..10)
	    {
	    	push(@threads,threads->new(\&worker_delete,11*$ctr+$_,$cguid,$keyoff));
	    	$keyoff = $keyoff+($num*12000+1);
	    }
    } 
    $_->join for (@threads);

	 
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



