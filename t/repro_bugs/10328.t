# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
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
use Test::More 'no_plan';

my $node; 
#my $nconn=256;
#my $ncntr=128;
my $nconn=3;
my $ncntr=0;
my $loop =100;
my %chash;
my @cguids;

sub open_close_delete {
	my ($connid, $cname) = @_;
	my $keyoffset = int(rand(250));
	my $ret;
	my $cguid = $chash{$cname};
	my @flags=("ZS_CTNR_RW_MODE", "ZS_CTNR_RO_MODE");

	$ret = ZSClose($node->conn($connid),$cguid);
        if (! $ret =~ /OK.*/){
		return;
	}
        #like ($ret, qr/OK.*/, "ZSCloseContainer: cguid=$cguid");

	$ret = ZSDelete($node->conn($connid),$cguid);
	if (! $ret =~ /OK.*/){
		return;
	}
        #like ($ret, qr/OK.*/, "ZSDeleteContainer: cguid=$cguid");

	@cguids = grep { $_ ne "$cguid" } @cguids;
	$ret=ZSOpen($node->conn($connid), $cname,3,0,"ZS_CTNR_CREATE","no","ZS_DURABILITY_HW_CRASH_SAFE");
        $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
        like ($ret, qr/OK.*/,"ZSopenContainer: $cname, cguid=$cguid");
        if ( defined $cguid ){
                $chash{$cname} = $cguid;
		push(@cguids,$cguid);
             }else{
		delete $chash{$cname};
	}
	$ret = ZSSetGet($node->conn($connid), $cguid,$keyoffset,50,10000,10000);
        like($ret, qr/OK.*/,"$ret");
	return;
}


sub test_run {
    my $ret;
    my $connid;
    my @threads;
    my $nop = 2;
    my $cguid;
    $ret = $node->start(
	       gdb_switch   => 1,
               ZS_REFORMAT  => 1,
           );
    like($ret, qr/OK.*/, 'Node start');
    
	# Create containers 
	for ( 0 .. $ncntr) { 
		my $ctrname	= 'ctrn-'.$_;
		my $flags="ZS_CTNR_CREATE";
		$ret = ZSOpen($node->conn(0),$ctrname,3,0,$flags,"no","ZS_DURABILITY_HW_CRASH_SAFE");
		chomp($ret);
	        $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
                like($ret,qr/OK.*/,"Create container,cguid=$cguid");
		if ( defined $cguid ){
			push(@cguids,$cguid);
			$chash{$ctrname} = $cguid;
		}
	}

	for( 0 .. $loop){
		my $keyoffset = 0 + $_*$nconn;
		@threads=();
		for (0 .. $nconn){
			$keyoffset = $keyoffset + $_;
			my $val = $_ % $nop;
			switch ($val) {				
                                case (0) {
					my $ctrname = 'ctrn-'.int(rand($ncntr));
                                        open_close_delete(($_),$ctrname);
				}
				case (1) {
					push(@threads, threads->new(\&ZSSetGet,$node->conn($_),$cguids[rand($ncntr)], $keyoffset,50,10000,10000));
				}
			}
		}
     	$_->join for (@threads);
        #print @threads;
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


