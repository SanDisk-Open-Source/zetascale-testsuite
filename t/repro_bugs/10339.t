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
use Test::More tests => 262;

my $node; 
my $ncntr=128;
my %chash;
my @cguids;


sub test_run {
    my $ret;
    my @threads;
    my $nop = 5;
   
    my $ctrname;
    $ret = $node->start(
	       gdb_switch   => 1,
               ZS_REFORMAT  => 1,
           );
    like($ret, qr/OK.*/, 'Node start');
    
	# Create containers 
	for ( 0 ..$ncntr) { 
	       $ctrname	= 'ctrn-'.$_;
	       my $cguid;
		my $flags="ZS_CTNR_CREATE";
	      #$ret = ZSOpen($node->conn(0),$ctrname,"ZS_CTNR_CREATE",1080000,0);
                $ret=ZSOpen($node->conn(0),$ctrname,3,0,"ZS_CTNR_CREATE","no","ZS_DURABILITY_HW_CRASH_SAFE");  
		chomp($ret);
	        $cguid = $1 if($ret =~ /OK cguid=(\d+)/);                 
                like ($ret, qr/OK.*/,"ZSopenContainer: $ctrname, cguid=$cguid flags=CREATE");
		if ( defined $cguid ){
			push(@cguids,$cguid);
			$chash{$ctrname} = $cguid;
		    }
                }
	
	$ret = $node->stop();
	like($ret, qr/OK.*/, 'Node stop');
	$ret = $node->start(
	   # gdb_switch  => 1,
             ZS_REFORMAT  => 0,
    	);
	like($ret, qr/OK.*/, 'Node start');
		
	#@cguids=();
        my $res = "";
	$res=ZSGetConts($node->conn(0));
	print "$res\n";
	my $key;
	my $value;
	my %tmp=(%chash,"demo"=>136);
	%chash=();
        
        while (($key,$value) = each %tmp){
               if(!(grep /$value/,@cguids)){
	               $ret = ZSOpen($node->conn(0),$key,3,0,"ZS_CTNR_CREATE","no","ZS_DURABILITY_HW_CRASH_SAFE");
			chomp($ret);
		         my $value = $1 if($ret =~ /OK cguid=(\d+)/);
		        like ($ret, qr/OK.*/,"ZSopenContainer: $key, cguid=$value,flags=CRETATE");
                  	if ( defined $value){
				push(@cguids, $value);
				$tmp{$key} = $value;
		  		}	
	                       
                       } else{
				$ret=ZSOpen($node->conn(0),$key,3,0, "ZS_CTNR_RW_MODE","no","ZS_DURABILITY_HW_CRASH_SAFE");
                                my $value =$1 if ($ret =~ /OK cguid=(\d+)/);
                                like ($ret, qr/OK.*/,"ZSopenContainer: $key, cguid=$value,flags=ZS_CTNR_RW_MODE"); 
			
		}
        }
    return;
}


sub test_init {
    $node = Fdftest::Node->new(
                ip     => "127.0.0.1", 
                port   => "24422",
                nconn  => 2,
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


