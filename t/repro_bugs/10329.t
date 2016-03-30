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
use Test::More tests => 261;

my $node; 
my $nconn=256;
my $ncntr=128;
#my $nconn=2;
#my $ncntr=0;
my $loop =0;
my %chash;
my @cguids;

sub open_close_delete {
	my ($connid, $cname) = @_;
	my $keyoffset = int(rand(250));
	my $ret;
	my $cguid = $chash{$cname};
	my @flags=("ZS_CTNR_RW_MODE", "ZS_CTNR_RO_MODE");

	$ret = ZSClose($node->conn($connid), $cguid);
	if (! $ret =~ /OK.*/){
		return;
	}

	$ret = ZSDelete($node->conn($connid), $cguid);
	if (! $ret =~ /OK.*/){
		return;
	}

	@cguids = grep { $_ ne "$cguid" } @cguids;
	#$ret=ZSOpen($node->conn($connid), $cname,"ZS_CTNR_CREATE",1080000,3);
        $ret = ZSOpen($node->conn($connid),$cname,3,0,"ZS_CTNR_CREATE","no","ZS_DURABILITY_HW_CRASH_SAFE");
   
        $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
        if ( defined $cguid ){
                $chash{$cname} = $cguid;
		push(@cguids,$cguid);
            }else{
		delete $chash{$cname};
        	}
	$ret = ZSSetGet($node->conn($connid), $cguid,$keyoffset,100,50,100,10000);
	like($ret, qr/OK.*/,"$ret");
        return;
}

sub test_run {
    my $ret;
    my $cguid;
    my @threads;
    my $nop = 2;
    $ret = $node->start(
	       gdb_switch   => 1,
               ZS_REFORMAT  => 1,
           );
    like($ret, qr/OK.*/, 'Node start');
    
	# Create containers 
	for ( 0 .. $ncntr) { 
		my $ctrname	= 'ctrn-'.$_;
		my $flags="ZS_CTNR_CREATE";
	       #$ret = ZSOpen($node->conn(0),$ctrname,"ZS_CTNR_CREATE",1080000,3);
		$ret = ZSOpen($node->conn(0),$ctrname,3,0,"ZS_CTNR_CREATE","no","ZS_DURABILITY_HW_CRASH_SAFE");
                chomp($ret);
               
	        $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
                like($ret, qr/OK.*/,"ZSopenContainer: $ctrname, cguid=$cguid flags=CREATE");
		if ( defined $cguid ){
			push(@cguids,$cguid);
			$chash{$ctrname} = $cguid;
		}
	}

	for( 0 .. $loop){
		my $keyoffset = 0 + $_*$nconn;
		@threads=();
		for ( 0 .. $nconn){
			#$keyoffset = $keyoffset + $_;
			my $val = $_ % $nop;
			switch ($val) {
				case (0) {
					my $ctrname = 'ctrn-'.int(rand($ncntr));
                    open_close_delete(($_),$ctrname);
				}
				case (1) {
                    push(@threads, threads->new(\&ZSEnumerate,$node->conn($_),$cguids[rand($ncntr)]));
				}
			}
		}
		$_->join for (@threads);

		$ret = $node->stop();
		like($ret, qr/OK.*/, 'Node stop');
	    $ret = $node->start(
	             # gdb_switch  => 1,
               ZS_REFORMAT  =>	0, 
    	);
	    like($ret, qr/OK.*/, 'Node start');

	}

	$ret = ZSGetContainers($node->conn(0));
	print $ret;
		
	for ( 0 .. $ncntr ) {
		ZSClose($node->conn(0), $cguids[$_]);
		ZSDelete($node->conn(0), $cguids[$_]);
	}
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


