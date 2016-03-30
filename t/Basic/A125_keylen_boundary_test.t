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
# author: Yiwen Lu
# email: yiwenlu@hengtiansoft.com
# date: Dec 10, 2014
# description: 

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::Stress;
use Test::More 'no_plan';

my $node; 
my @ctr_type = ("BTREE","HASH");
my @data = (1950,254);
sub test_run {
    my $ret;
    my $cguid;
    $ret = $node->start(
           ZS_REFORMAT  => 1,
           );
    like($ret, qr/OK.*/, 'Node start');
	for (my $j=1;$j<=2;$j++){
        $ret = ZSOpen($node->conn(0),"ctr-$j",4,0,"ZS_CTNR_CREATE","yes","ZS_DURABILITY_SW_CRASH_SAFE",$ctr_type[$j%2]);
        like ($ret, qr/^OK.*/, $ret);
      	$cguid = $1 if($ret =~ /OK cguid=(\d+)/);
        
        $ret = ZSSet($node->conn(0),$cguid,0,$data[$j%2],10,3,0);  
        like ($ret, qr/^OK.*/, "Max_Keylen=$data[$j%2]->match $ret");
        $ret = ZSSet($node->conn(0),$cguid,0,$data[$j%2]+1,10,3,0);  
        like ($ret, qr/SERVER_ERROR ZS_KEY_TOO_LONG.*/, "Set keylen=$data[$j%2]+1,expect report SERVER_ERROR ZS_KEY_TOO_LONG");
        
        $ret = ZSClose($node->conn(0),$cguid);
        like ($ret, qr/^OK.*/, $ret);
	}
    
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


