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
use Test::More tests => 505;

my $node; 
my $nconn = 128;
#my @data = ([150, 64000, -10000], [150, 128000, -10000], [150, 512, -60000]);
my @data = ([64, 16000, -500], [64, 32000, -500], [64, 64000, -500], [64, 128000, -500], [64, 48, -10000]);
my $keyoff = 900000;
sub worker_write{
	my($con,$cguid,$keyoff)=@_;
	my $ret;
	foreach(0..10)
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
        		like ($ret, qr/^OK.*/, "OK ZSWriteObject cguid=$cguid key=$keyoff,keylen=$keylen,datalen=$datalen,nops=$nops");
			$keyoff = $keyoff+$nops;
		}
	}
}

sub worker_read{
	my($con,$cguid,$keyoff)=@_;
	my $ret;
	foreach(0..10)
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

	foreach(0..10)
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
    my $nctr = 3;
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
    foreach(0..$nctr-1) 
    {
            my $cguid = $cguids[$_];
	    push(@threads,threads->new(\&worker_write,$_,$cguid,$keyoff));
    }
    $_->join for (@threads);

	
    @threads = ();
    foreach(0..$nctr-1)
    {
	    my $cguid = $cguids[$_];
	    push(@threads,threads->new(\&worker_read,$_,$cguid,$keyoff));
    }

    $_->join for (@threads);
    
    @threads = ();
    foreach(0..$nctr-1)
    {
	    my $cguid = $cguids[$_];
	    push(@threads,threads->new(\&worker_delete,$_,$cguid,$keyoff));
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


