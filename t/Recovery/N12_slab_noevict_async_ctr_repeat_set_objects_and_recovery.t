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

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::TestCase;
use Test::More tests => 80;

my $node; 
sub test_run {
    my $cguid;
    my $ret; 
    my $i = 0;
    my $repeat_time = 3;
    my %keyoff_keylen;
    my @prop = ([4, "ZS_DURABILITY_HW_CRASH_SAFE", "no"],);
    #my @data = ([64000, 7000], [128000, 7000], [512, 42000]);
    my @data = ([16000, 3000], [32000, 3000], [64000, 3000], [128000, 3000], [48, 60000]);

    foreach my $p(@prop){
        $ret = $node->start(ZS_REFORMAT => 1);    
	like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=1");

	$ret = OpenContainer($node->conn(0),"ctr-1","ZS_CTNR_CREATE",0,$$p[0],$$p[1],$$p[2]);
	$cguid = $1 if ($ret =~ /OK cguid=(\d+)/);

	my $keyoffset = 0;
	my $keylen = 50;
	%keyoff_keylen = ();
	foreach(0..$repeat_time-1){
            foreach my $d(@data){
                WriteReadObjects($node->conn(0),$cguid,$keyoffset,$keylen+$i,1000,$$d[0],$$d[1]);
                $i++;
            }
            $i = 0;
	    FlushContainer($node->conn(0),$cguid);
	    CloseContainer($node->conn(0),$cguid);
            $keyoff_keylen{$keyoffset}=$keylen;
            $keyoffset = $keyoffset+100;
            $keylen = $keylen+20;

	    $ret = $node->stop();
	    like($ret,qr/OK.*/,"Node Stop");
	    $ret = $node->start(ZS_REFORMAT => 0);    
	    like($ret,qr/OK.*/,"Node Start: REFORMAT=0");

	    OpenContainer($node->conn(0),"ctr-1","ZS_CTNR_RW_MODE",0,$$p[0],$$p[1],$$p[2]);
            foreach(keys %keyoff_keylen)
	    {
                foreach my $d(@data){
                    ReadObjects($node->conn(0),$cguid,$_,$keyoff_keylen{$_}+$i,1000,$$d[0],$$d[1]);
                    $i++;
                }
                $i = 0;
            }
        }
	CloseContainer($node->conn(0),$cguid);    
	DeleteContainer($node->conn(0),$cguid);
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
#                prop   => "$Bin/../../conf/stress.prop", 
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


