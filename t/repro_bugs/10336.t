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
use Test::More tests => 6;

my $node; 
my $nconn=256;
my $ncntr=128;
my $loop =100;
my %chash;
my @cguids;

$nconn=20;
$ncntr=0;
$loop=1;

sub test_run {
    my $ret;
    my @threads;
    my $nop = 5;
    my $myret; 
    $ret = $node->start(
	       gdb_switch   => 1,
               ZS_REFORMAT  => 1,
           );
    like($ret, qr/OK.*/, 'Node start');
    
	# Create containers 
		my $ctrname	= 'ctrn-0';
		my $cguid;
		$ret = ZSOpen($node->conn(0),$ctrname,3,108000,"ZS_CTNR_CREATE","no","ZS_DURABILITY_HW_CRASH_SAFE");
		chomp($ret);
	    $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
		if ( defined $cguid ){
			push(@cguids,$cguid);
			$chash{$ctrname} = $cguid;
		}
                like($ret, qr/OK.*/,"ZSopenContainer: $ctrname, cguid=$cguid flags=CREATE");
		$myret=ZSSetGet($node->conn(0), $cguid,0,50,50,100);
                
                like($myret, qr/OK.*/,"$myret");
		$ret = $node->stop();
		like($ret, qr/OK.*/, 'Node stop');
	    $ret = $node->start(
	           gdb_switch  => 1,
               ZS_REFORMAT  => 0,
    	);
	    like($ret, qr/OK.*/, 'Node start');
		
            
            $ret =ZSOpen($node->conn(0),$ctrname,3,108000,"ZS_CTNR_RW_MODE","no","ZS_DURABILITY_HW_CRASH_SAFE");

	   $myret= ZSEnumerate($node->conn(0),$cguid);
           like($myret, qr/OK.*/,$myret);
		return;
}


sub test_init {
    $node = Fdftest::Node->new(
                ip     => "127.0.0.1", 
                port   => "24422",
                nconn  => $nconn,
            );
    
    return;
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


