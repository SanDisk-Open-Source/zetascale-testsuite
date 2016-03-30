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
# date: Jan 3, 2013
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
use Test::More tests => 65;

my $node; 
my $nctr = 50;
my $nconn = 50;

sub worker_write{
    my ($con,$cguid) = @_;
    my $res;
    my @setparmarray = ([0,25,64000,4000],[0,26,128000,4000],[0,27,512,24000]);
    foreach $parms (@setparmarray)
    {
        my ($offset, $keylen, $datalen, $nops) = @{$parms};
        $res = ZSSet($node->conn($con), $cguid, $offset, $keylen, $datalen, $nops, "ZS_WRITE_MUST_NOT_EXIST");
        like ($res, qr/^OK.*/, $res);
    }
    sleep(20);
}

sub worker_delete{
    my ($con,$cguid) = @_;
    my $res;
    my @setparmarray = ([0,25,500],[0,26,500],[0,27,3000]);
    foreach $parms (@setparmarray)
    {
        my ($offset, $keylen, $nops) = @{$parms};
        $res = ZSDel ($node->conn($con), $cguid, $offset, $keylen, $nops);
        like ($res, qr/^OK.*/, $res);
    }
}

sub worker_read{
    my ($con,$cguid, $delete_flag) = @_;
    my $res;
    my @setparmarray = ([0,25,64000,4000],[0,26,128000,4000],[0,27,512,24000]);
    if (defined($delete_flag) && $delete_flag == 1) {
    @setparmarray = ([500,25,64000,3500],[500,26,128000,3500],[3000,27,512,21000]);
    }
    foreach $parms (@setparmarray)
    {
        my ($offset, $keylen, $datalen, $nops) = @{$parms};
        $res = ZSGet($node->conn($con), $cguid, $offset, $keylen, $datalen, $nops);
        like ($res, qr/^OK.*/, $res);
    }
    sleep(50);
}

sub worker_rangeall {
    my ($con,$cguid, $nobject) = @_;
    my $res;
            $ret = ZSGetRange (
                $node->conn($con),
                cguid         => $cguid,
                );
        like($ret, qr/OK.*/,"ZSGetRange only with cguid");

        $ret = ZSGetNextRange (
                $node->conn($con),
                n_in          => $nobject+2,
                check         => "no",
                );
        my $n_out = $1 if($ret =~ /OK n_out=(\d+)/);
        like($ret, qr/OK n_out=$nobject*/, "ZSGetNextRange:Get $n_out objects ,$ret");

        $ret = ZSGetRangeFinish($node->conn($con));
        like($ret, qr/OK.*/, "ZSGetRangeFinish");

}

sub worker_close_and_delete{
    my ($con,$cguid) = @_;
    my $res;
    $res = ZSClose($node->conn($con%$nconn), $cguid);
    like ($res, qr/^OK.*/, $res);
    $res = ZSDelete($node->conn($con), $cguid);
    like ($res, qr/^OK.*/, $res);
}

sub test_run {
    my $cguid;
    my $ret;
    my $choice; 
    my @cguids;
    my @threads;    
    my %cguid_cname;
    my @ctr_type = ("BTREE","HASH");    
    $ret = $node->start(ZS_REFORMAT => 1);    
    like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=1");
   
        $choice = 3;
        print "choice=$choice\n";  
        foreach(0..$nctr-1)
        {   
	    my $cname="ctr-$_";
            $ret = ZSOpen($node->conn(0),"ctr-$_",$choice,0,"ZS_CTNR_CREATE","no","ZS_DURABILITY_SW_CRASH_SAFE","BTREE");
            like ($ret, qr/^OK.*/, $ret);
            $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
            $cguid_cname{$cname} = $cguid;
        }
         
        $ret = ZSGetConts($node->conn(0), $nctr);
        like ($ret, qr/^OK.*/, $ret);

	foreach (0.. $nctr/$nconn-1) {
	my $count =  $_;
        @threads = ();
        foreach(0..$nconn-1) 
        {
	    my $cname= "ctr-".($count*$nconn + $_);
	    my $cguid = $cguid_cname{$cname};
            push(@threads, threads->new (\&worker_write, $_, $cguid));
        }
        $_->join for (@threads);
	}
	foreach (0.. $nctr/$nconn-1) {
	my $count =  $_;
        @threads = ();
        foreach(0..$nconn-1) 
        {
	    my $cname= "ctr-".($count*$nconn + $_);
	    my $cguid = $cguid_cname{$cname};
            push(@threads, threads->new (\&worker_delete, $_, $cguid));
        }
        $_->join for (@threads);
	}
        $ret = ZSGetConts($node->conn(0), $nctr);
        like ($ret, qr/^OK.*/, $ret);

        $ret = $node->kill();
        $ret = $node->start(
               ZS_REFORMAT  => 0,
           );
        like($ret, qr/OK.*/, 'Node restart');

        $ret = ZSGetConts($node->conn(0), $nctr);
        like ($ret, qr/^OK.*/, $ret);

        $choice = 3;
        print "choice=$choice\n";  
        foreach(0..$nctr-1)
        {   
            $ret = ZSOpen($node->conn(0),"ctr-$_",$choice,0,"ZS_CTNR_RW_MODE","no","ZS_DURABILITY_SW_CRASH_SAFE","BTREE");
            like ($ret, qr/^OK.*/, $ret);
            $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
            $cguid_cname{"ctr-$_"} = $cguid;
        }
         
	foreach (0.. $nctr/$nconn-1) {
        my $count =  $_;
        @threads = ();
        foreach (0..$nconn-1)
        {
	    my $cname= "ctr-".($count*$nconn + $_);
            my $cguid = $cguid_cname{$cname};
            push(@threads, threads->new (\&worker_read, $_, $cguid));
        }
        $_->join for (@threads);
	}
	return;
}

sub test_init {
    $node = Fdftest::Node->new(
                ip     => "127.0.0.1", 
                port   => "24422",
                nconn  => $nconn,
		thread => $nconn,
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


