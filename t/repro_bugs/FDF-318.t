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
# date: Apr 9, 2015
# description:

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::UnifiedAPI;
use Test::More tests => 18;

my $node; 
sub test_run {
    my $cguid;
    my $ret; 
    my $i = 0;
    my $repeat_time = 1;
    my %keyoff_keylen;
    my @prop = ([3, "ZS_DURABILITY_HW_CRASH_SAFE", "no"],);
    my @data = ([64000, 700], [128000, 700], [512, 4200]);
    my @flog_mode = ("ZS_FLOG_FILE_MODE","ZS_FLOG_NVRAM_MODE");
    #my @flog_mode = ("ZS_FLOG_NVRAM_MODE","ZS_FLOG_FILE_MODE");

    foreach my $p(@prop){
        $node->set_ZS_prop(ZS_FLOG_MODE => $flog_mode[0]);
        print "set ZS_FLOG_MODE = $flog_mode[0]\n";
        $ret = $node->start(ZS_REFORMAT => 1);    
	like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=1");

        my $ret = OpenContainer($node->conn(0), "ctr-1", $$p[0], 0, "ZS_CTNR_CREATE", $$p[2], $$p[1], "BTREE");
        like ($ret, qr/^OK.*/, $node->{'port'}.":".$ret);
	$cguid = $1 if ($ret =~ /OK cguid=(\d+)/);

	my $keyoffset = 0;
	my $keylen = 50;
	%keyoff_keylen = ();
	foreach(1..$repeat_time){
            foreach my $d(@data){
                #$ret =  WriteObjects($node->conn(0),$cguid,$keyoffset,$keylen+$i,1000,$$d[0],$$d[1],"ZS_WRITE_MUST_NOT_EXIST");
	        #like($ret,qr/OK.*/,"$ret");
                #$ret = ReadObjects($node->conn(0),$cguid,$keyoffset,$keylen+$i,1000,$$d[0],$$d[1]);
	        #like($ret,qr/OK.*/,"$ret");
                #$i++;
            }
            $i = 0;
	    $ret = CloseContainer($node->conn(0),$cguid);
            like($ret,qr/OK.*/,"$ret");
            $keyoff_keylen{$keyoffset}=$keylen;
            $keyoffset = $keyoffset+100;
            $keylen = $keylen+20;

	    $ret = $node->stop();
	    like($ret,qr/OK.*/,"Node Stop");
            $node->set_ZS_prop(ZS_FLOG_MODE => $flog_mode[$_%2]);
            print "set ZS_FLOG_MODE = $flog_mode[$_%2]\n";
	    $ret = $node->start(ZS_REFORMAT => 0);    
	    like($ret,qr/OK.*/,"Node Start: REFORMAT=0");

            my $ret = OpenContainer($node->conn(0), "ctr-1", $$p[0], 0, "ZS_CTNR_RW_MODE", $$p[2], $$p[1], "BTREE");
	    like ($ret, qr/^OK.*/, $node->{'port'}.":".$ret);
	    $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
            foreach(keys %keyoff_keylen)
	    {
                foreach my $d(@data){
                    #$ret = ReadObjects($node->conn(0),$cguid,$_,$keyoff_keylen{$_}+$i,1000,$$d[0],$$d[1]);
	            #like($ret,qr/OK.*/,"$ret");
                    #$i++;
                }
                $i = 0;
            }
        }
	$ret = CloseContainer($node->conn(0),$cguid);
	like($ret,qr/OK.*/,"$ret");
	$ret = DeleteContainer($node->conn(0),$cguid);
	like($ret,qr/OK.*/,"$ret");
	$ret = $node->stop();
	like($ret,qr/OK.*/,"Node Stop");
    }
    foreach my $p(@prop){
        $node->set_ZS_prop(ZS_FLOG_MODE => $flog_mode[0]);
        print "set ZS_FLOG_MODE = $flog_mode[0]\n";
        $ret = $node->start(ZS_REFORMAT => 1);    
	like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=1");

        my $ret = OpenContainer($node->conn(0), "ctr-1", $$p[0], 0, "ZS_CTNR_CREATE", $$p[2], $$p[1], "BTREE");
        like ($ret, qr/^OK.*/, $node->{'port'}.":".$ret);
	$cguid = $1 if ($ret =~ /OK cguid=(\d+)/);

	my $keyoffset = 0;
	my $keylen = 50;
	%keyoff_keylen = ();
	foreach(1..$repeat_time){
            foreach my $d(@data){
                #$ret =  WriteObjects($node->conn(0),$cguid,$keyoffset,$keylen+$i,1000,$$d[0],$$d[1],"ZS_WRITE_MUST_NOT_EXIST");
	        #like($ret,qr/OK.*/,"$ret");
                #$ret = ReadObjects($node->conn(0),$cguid,$keyoffset,$keylen+$i,1000,$$d[0],$$d[1]);
	        #like($ret,qr/OK.*/,"$ret");
                #$i++;
            }
            $i = 0;
	    $ret = CloseContainer($node->conn(0),$cguid);
            like($ret,qr/OK.*/,"$ret");
            $keyoff_keylen{$keyoffset}=$keylen;
            $keyoffset = $keyoffset+100;
            $keylen = $keylen+20;

	    $ret = $node->stop();
	    like($ret,qr/OK.*/,"Node Stop");
            $node->set_ZS_prop(ZS_FLOG_MODE => $flog_mode[$_%2]);
            print "set ZS_FLOG_MODE = $flog_mode[$_%2]\n";
	    $ret = $node->start(ZS_REFORMAT => 0);    
	    like($ret,qr/OK.*/,"Node Start: REFORMAT=0");

            my $ret = OpenContainer($node->conn(0), "ctr-1", $$p[0], 0, "ZS_CTNR_RW_MODE", $$p[2], $$p[1], "BTREE");
	    like ($ret, qr/^OK.*/, $node->{'port'}.":".$ret);
	    $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
            foreach(keys %keyoff_keylen)
	    {
                foreach my $d(@data){
                    #$ret = ReadObjects($node->conn(0),$cguid,$_,$keyoff_keylen{$_}+$i,1000,$$d[0],$$d[1]);
	            #like($ret,qr/OK.*/,"$ret");
                    #$i++;
                }
                $i = 0;
            }
        }
	$ret = CloseContainer($node->conn(0),$cguid);
	like($ret,qr/OK.*/,"$ret");
	$ret = DeleteContainer($node->conn(0),$cguid);
	like($ret,qr/OK.*/,"$ret");
	$ret = $node->stop();
	like($ret,qr/OK.*/,"Node Stop");
    }

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
    $node->set_ZS_prop(ZS_FLOG_MODE => "ZS_FLOG_FILE_MODE");
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


