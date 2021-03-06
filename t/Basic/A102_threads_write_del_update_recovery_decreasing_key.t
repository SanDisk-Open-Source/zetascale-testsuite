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
use Fdftest::BasicTest;
use Test::More tests => 6023;

my $node; 
my $num = 3;
my $ncntr = 5;
my $nthread = 10;      #thread num per cntr
#my @data = ([150, 64000, -1000], [150, 128000, -1000], [150, 512, -6000]);
my @data = ([64, 16000, -500], [64, 32000, -500], [64, 64000, -500], [64, 128000, -500], [64, 48, -10000]);

sub worker_write{
    my($con, $cguid, $keyoff, $valoff)=@_;
    my $ret;

    foreach(0..$num-1)
    {
        foreach my $d(@data)
        {
            $ret = ZSWriteObject(
                    $node->conn($con),
                    cguid           =>$cguid,
                    key             =>$keyoff,
                    key_len         =>$$d[0],
                    data_offset     =>$valoff,
                    data_len        =>$$d[1],
                    nops            =>$$d[2],
                    flags           =>"ZS_WRITE_MUST_NOT_EXIST",
                    );
            like ($ret, qr/^OK.*/, "OK conid=$con ZSWriteObject cguid=$cguid key=$keyoff,keylen=$$d[0],datalen=$$d[1],nops=$$d[2]");
            $keyoff = $keyoff+$$d[2];
            $valoff = $valoff+$$d[2];
        }
    }
}

sub worker_update{
    my($con, $cguid, $keyoff, $valoff)=@_;
    my $ret;

    foreach(0..$num-1)
    {
        foreach my $d(@data)
        {
            $ret = ZSWriteObject(
                    $node->conn($con),
                    cguid           =>$cguid,
                    key             =>$keyoff,
                    key_len         =>$$d[0],
                    data_offset     =>$valoff,
                    data_len        =>$$d[1],
                    nops            =>$$d[2]/2,
                    flags           =>"ZS_WRITE_MUST_NOT_EXIST",
                    );
            like ($ret, qr/^OK.*/, "OK conid=$con ZSWriteObject cguid=$cguid key=$keyoff,keylen=$$d[0],datalen=$$d[1],nops=".$$d[2]/2);

            $ret = ZSWriteObject(
                    $node->conn($con),
                    cguid           =>$cguid,
                    key             =>$keyoff+$$d[2]/2,
                    key_len         =>$$d[0],
                    data_offset     =>$valoff-$$d[2]/2,
                    data_len        =>$$d[1],
                    nops            =>$$d[2]/2,
                    flags           =>"ZS_WRITE_MUST_EXIST",
                    );
            like ($ret, qr/^OK.*/, "OK conid=$con ZSWriteObject cguid=$cguid key=".($keyoff+$$d[2]/2).",keylen=$$d[0],datalen=$$d[1],nops=".$$d[2]/2);
            $keyoff = $keyoff+$$d[2];
            $valoff = $valoff+$$d[2];
        }
    }
}

sub worker_read{
    my($con, $cguid, $keyoff, $valoff)=@_;
    my $ret;

    foreach(0..$num-1)
    {
        foreach my $d(@data)
        {
            $ret = ZSReadObject(
                $node->conn($con),
                cguid           =>$cguid,
                key             =>$keyoff,
                key_len         =>$$d[0],
                data_offset     =>$valoff,
                data_len        =>$$d[1],
                nops            =>$$d[2],
                check           =>"yes",
                );
            like ($ret, qr/^OK.*/, "OK conid=$con ZSReadObject cguid=$cguid key=$keyoff,keylen=$$d[0],datalen=$$d[1],nops=$$d[2]");
            $keyoff = $keyoff+$$d[2];
            $valoff = $valoff+$$d[2];
        }
    }
}

sub worker_read_after_del{
    my($con, $cguid, $keyoff, $valoff)=@_;
    my $ret;

    foreach(0..$num-1)
    {
        foreach my $d(@data)
        {
            $ret = ZSReadObject(
                $node->conn($con),
                cguid           =>$cguid,
                key             =>$keyoff+$$d[2]/2,
                key_len         =>$$d[0],
                data_offset     =>$valoff-$$d[2]/2,
                data_len        =>$$d[1],
                nops            =>$$d[2]/2,
                check           =>"yes",
                );
            like ($ret, qr/^OK.*/, "OK conid=$con ZSReadObject cguid=$cguid key=".($keyoff+$$d[2]/2).",keylen=$$d[0],datalen=$$d[1],nops=".$$d[2]/2);

            $ret = ZSReadObject(
                $node->conn($con),
                cguid           =>$cguid,
                key             =>$keyoff,
                key_len         =>$$d[0],
                data_offset     =>$valoff,
                data_len        =>$$d[1],
                nops            =>$$d[2]/2,
                check           =>"yes",
                );
            like ($ret, qr/^SERVER_ERROR ZS_OBJECT_UNKNOWN.*/, "ZSReadObject ZS_OBJECT_UNKNOWN cguid=$cguid key=$keyoff,keylen=$$d[0],datalen=$$d[1],nops=".$$d[2]/2);
            $keyoff = $keyoff+$$d[2];
            $valoff = $valoff+$$d[2];
        }
    }
}

sub worker_delete{
    my($con, $cguid, $keyoff)=@_;
    my $ret;

    foreach(0..$num-1)
    {
        foreach my $d(@data)
        {
            $ret = ZSDeleteObject(
                    $node->conn($con),
                    cguid           =>$cguid,
                    key             =>$keyoff,
                    key_len         =>$$d[0],
                    nops            =>$$d[2]/2,
                 );
            like($ret,qr/^OK.*/,"OK conid=$con ZSDeleteObject cguid=$cguid key=$keyoff,keylen=$$d[0],nops=".$$d[2]/2);
            $keyoff = $keyoff+$$d[2];
        }
    }	
}

sub test_run {
    my ($ret, $cguid);
    my (@cguids, @threads);
    my $size = 0;
    my $keyoff = 9000000;
    my @prop = (["yes","no","yes","ZS_DURABILITY_HW_CRASH_SAFE","no"],);

    $ret = $node->start(ZS_REFORMAT => 1, threads => $ncntr*$nthread,);    
    like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=1");
   
    foreach(0..$ncntr-1)
    {
        $ret=OpenContainer($node->conn(0),"cntr-$_","ZS_CTNR_CREATE",$size,4,"ZS_DURABILITY_HW_CRASH_SAFE","no");
        $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
        push(@cguids, $cguid);
    }
	
    @threads = ();
    $keyoff = 9000000;
    foreach my $c(0..$ncntr-1) 
    {
	foreach my $t(0..$nthread-1)
	{
            push(@threads,threads->new(\&worker_write, $nthread*$c+$t,$cguids[$c],$keyoff,$keyoff));
            $keyoff = $keyoff-($num*12000+1);   
        }
    }
    $_->join for (@threads);
	
    @threads = ();
    $keyoff = 9000000;
    foreach my $c(0..$ncntr-1)
    {
	foreach my $t(0..$nthread-1)
	{
            push(@threads,threads->new(\&worker_read, $nthread*$c+$t,$cguids[$c],$keyoff,$keyoff));
            $keyoff = $keyoff-($num*12000+1);
	}
    }
    $_->join for (@threads);
    
    @threads = ();
    $keyoff = 9000000;
    foreach my $c(0..$ncntr-1)
    {
        foreach my $t(0..$nthread-1)
        {
            push(@threads,threads->new(\&worker_delete, $nthread*$c+$t,$cguids[$c],$keyoff));
            $keyoff = $keyoff-($num*12000+1);
        }
    } 
    $_->join for (@threads);

	 
    foreach(@cguids)
    {
        FlushContainer($node->conn(0), $_);
        CloseContainer($node->conn(0), $_);
    } 

    $ret = $node->stop();
    like($ret, qr/OK.*/, "Node Stop");
    $ret = $node->start(ZS_REFORMAT => 0,);
    like($ret, qr/OK.*/, "Node Restart");

    foreach(0..$ncntr-1)
    {
        $ret=OpenContainer($node->conn(0),"cntr-$_","ZS_CTNR_RW_MODE",$size,4,"ZS_DURABILITY_HW_CRASH_SAFE","no");
    }

    @threads = ();
    $keyoff = 9000000;
    foreach my $c(0..$ncntr-1)
    {
        foreach my $t(0..$nthread-1)
        {
            push(@threads,threads->new(\&worker_read_after_del, $nthread*$c+$t,$cguids[$c],$keyoff,$keyoff));
            $keyoff = $keyoff-($num*12000+1);
        }
    }
    $_->join for (@threads);

    @threads = ();
    $keyoff = 9000000;
    foreach my $c(0..$ncntr-1)
    {
        foreach my $t(0..$nthread-1)
        {
            push(@threads,threads->new(\&worker_update, $nthread*$c+$t,$cguids[$c],$keyoff,$keyoff-1));
            $keyoff = $keyoff-($num*12000+1);
        }
    }
    $_->join for (@threads);

    @threads = ();
    $keyoff = 9000000;
    foreach my $c(0..$ncntr-1)
    {
        foreach my $t(0..$nthread-1)
        {
            push(@threads,threads->new(\&worker_read, $nthread*$c+$t,$cguids[$c],$keyoff,$keyoff-1));
            $keyoff = $keyoff-($num*12000+1);
        }
    }
    $_->join for (@threads);

    return;
}

sub test_init {
    $node = Fdftest::Node->new(
        ip     => "127.0.0.1", 
        port   => "24422",
        nconn  => $ncntr*$nthread,
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



