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

# file: basic.pl
# author: yiwen sun
# email: yiwensun@hengtiansoft.com
# date: Oct 15, 2012
# description: basic sample for testcase

#!/usr/bin/perl

use strict;
use warnings;
use Switch;
use threads;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Stress;
use Fdftest::Node;
use Test::More tests => 502;

my $node;
my @cguids;
my $nconn = 32;
my $nctr = 4;
#my @data = ([64000, 7000], [128000, 7000], [512, 42000]);
my @data = ([16000, 3000], [32000, 3000], [64000, 3000], [128000, 3000], [48, 60000]);

sub worker_write {
    my ($connid, $cguid, $keyoffset, $keylen) = @_;
    my $i = 0;

    foreach my $d(@data){
        my $ret = ZSSet ($node->conn ($connid), $cguid, $keyoffset, $keylen+$i, $$d[0], $$d[1], "ZS_WRITE_MUST_NOT_EXIST");
        like ($ret, qr/^OK.*/, $ret);
        $i++;
    }
}

sub worker_read {
    my ($connid, $cguid, $keyoffset, $keylen) = @_;
    my $i = 0;

    foreach my $d(@data){
        my $ret = ZSGet ($node->conn ($connid), $cguid, $keyoffset, $keylen+$i, $$d[0], $$d[1]);
        like ($ret, qr/^OK.*/, $ret);
        $i++;
    }
}

sub test_run {
    my $ret;
    my @threads;
    my ($keyoffset, $keylen);
    my $size = 0;
    my ($cguid,$cname,$connid);
    my @cguids;
    my %cguid_cname;
    my %cguid_key_data;
    my @prop = ([3, "ZS_DURABILITY_HW_CRASH_SAFE", "no"],);

    $ret = $node->start (
#        gdb_switch   => 1,
        ZS_REFORMAT => 1,
    );
    like ($ret, qr/^OK.*/, 'Node start');

    foreach my $p(@prop){
        @cguids = ();        
        foreach (1..$nctr){
            $cname = "ctr-$_";
	    $ret = ZSOpen ($node->conn (0), $cname, $$p[0], $size, "ZS_CTNR_CREATE", $$p[2], $$p[1]);
	    like ($ret, qr/^OK.*/, $ret);
	    $cguid = $1 if ($ret =~ /^OK cguid=(\d+)/) ; 
	    push @cguids, $cguid;
	    $cguid_cname{$cguid} = $cname;
	}

#    print @cguids;
	@threads = ();
	($keyoffset, $keylen) = (1000,30);
	foreach(1..$nconn){
            $connid = $_;
	    $cguid = $cguids[$connid%$nctr];
	    $cguid_key_data{$cguid}{$connid}[0]= $keyoffset;
	    $cguid_key_data{$cguid}{$connid}[1]= $keylen;
#        print "Write:connid=$connid,cguid=$cguid,$keyoffset-$keylen-$datalen-$nops\n";
	    push(@threads, threads->new (\&worker_write, $_, $cguid, $keyoffset, $keylen));
	    $keyoffset = $keyoffset+20;
	    $keylen = $keylen+3 ;
	}
	$_->join for (@threads);

	@threads = ();
	foreach(@cguids){
            $cguid=$_;
	    my @obj_cguid=keys %{$cguid_key_data{$cguid}};
	    foreach(@obj_cguid){
	        $keyoffset = $cguid_key_data{$cguid}{$_}[0];
		$keylen = $cguid_key_data{$cguid}{$_}[1];
#            print "Read:cguid=$cguid,connid=$_,$keyoffset-$keylen-$datalen-$nops\n";
		push(@threads, threads->new (\&worker_read, $_, $cguid, $keyoffset, $keylen));
	    }
	}
	$_->join for (@threads);

        foreach(@cguids){
            $ret = ZSClose($node->conn (0),$_);
            like ($ret, qr/^OK.*/,$ret);
        }

	$node->stop ();
	$ret = $node->start (
#        gdb_switch   => 1,
            ZS_REFORMAT => 0,
        );
	like ($ret, qr/^OK.*/, 'Node start');

	foreach(@cguids){
            $cname=$cguid_cname{$_};
	    $ret = ZSOpen ($node->conn (0), $cname, $$p[0], $size, "ZS_CTNR_RW_MODE", $$p[2], $$p[1]);
	    like ($ret, qr/^OK.*/, $ret);
	}
	@threads = ();
	foreach(@cguids){
            $cguid=$_;
	    my @obj_cguid=keys %{$cguid_key_data{$cguid}};
	    foreach(@obj_cguid){
                $keyoffset = $cguid_key_data{$cguid}{$_}[0];
		$keylen = $cguid_key_data{$cguid}{$_}[1];
#            print "Read:cguid=$cguid,connid=$_,$keyoffset-$keylen-$datalen-$nops\n";
		push(@threads, threads->new (\&worker_read, $_, $cguid, $keyoffset, $keylen));
	    }
	}
	$_->join for (@threads);

        foreach(@cguids){
            $ret = ZSClose($node->conn (0),$_);
            like ($ret, qr/^OK.*/,$ret);
            $ret = ZSDelete($node->conn (0),$_);
            like ($ret, qr/^OK.*/,$ret);
        }
    }
    return;
}

sub test_init {
    $node = Fdftest::Node->new (
        ip    => "127.0.0.1",
        port  => "24422",
        nconn => $nconn,
		prop  => "$Bin/../../conf/stress.prop",
    );
    return;
}

sub test_clean {
    $node->stop ();
    $node->set_ZS_prop (ZS_REFORMAT => 1);

    return;
}

#
# main
#
{
    test_init ();

    test_run ();

    test_clean ();
}

