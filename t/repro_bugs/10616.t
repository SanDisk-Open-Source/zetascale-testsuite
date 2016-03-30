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
use Test::More tests => 26;

#tests =( 6*($nconn) + 2) * ($loop) + 4
#tests = 66 + 482 * $loop
my $node;
my (@cguids, %chash);
my $nconn = 8;
my $ncntr = 4;
my $loop  = 1;
#$loop  = 100;

sub worker {
    my ($connid, $cname, $keyoffset, $keylen, $datalen, $nops) = @_;
    my ($ret, $msg, $cguid);
    my $size = int(rand(4194304));
    $cguid = $chash{$cname};
	if ( $cname eq "ctrn-30" ) {
		$size = int(rand(209715200));
	}

    $ret = ZSSet ($node->conn ($connid), $cguid, $keyoffset, $keylen, $datalen, $nops, 0);
    like ($ret, qr/^OK.*/, $ret);
}

sub test_run {
    my ($ret, $msg);
    my @threads;
	my $cname;
	my %cntr_obj;
	my ($keyoffset, $keylen, $datalen, $maxops, $nops);
    my $size = 10240;

    $ret = $node->start (
#        gdb_switch   => 1,
        ZS_REFORMAT => 1,
    );
    like ($ret, qr/^OK.*/, 'Node start');

    # Create containers with $nconn connections
    for (1 .. $ncntr) {
	#my $mode  = int(rand(2)) ? 3 : 5;
        $cname = 'ctrn-' . "$_";
		
        $ret = ZSOpen ($node->conn (0), $cname, 3 , $size, "ZS_CTNR_CREATE", "no","ZS_DURABILITY_HW_CRASH_SAFE");
        like ($ret, qr/^OK.*/, $ret);

        if ($ret =~ /^OK cguid=(\d+)/) {
            push(@cguids, $1);
            $chash{$cname} = $1;
	    $cntr_obj{$cname}{times} = 0; 
        }
        else {
            return;
        }
    }

    @threads = ();
    for (1 .. $nconn) {
	    my $id		= int(rand($ncntr) + 1);
            my $connid		= $_;
            $cname		= 'ctrn-' . $id;
	    if ( $cntr_obj{$cname}{times} == 0 ) { 
		 $cntr_obj{$cname}{times}  = 1;
                 $keyoffset	= int(rand(50000000))+1;
	         $keylen	= int(rand(240-$nconn))+1;
    	         $datalen	= int(rand(2048000))+1;
		 $maxops	= int((6000)/(($datalen)/1000000));
		 $nops		= int($maxops/$nconn);
=comment
            	$keyoffset	= 2000;
	            $keylen		= 20;
    	        $datalen	= 2048;
            	$nops		= 50000; 
=cut

				$cntr_obj{$cname}{keyoff}	= $keyoffset;
				$cntr_obj{$cname}{keylen}	= $keylen;
				$cntr_obj{$cname}{datalen}	= $datalen;
				$cntr_obj{$cname}{nops}	        = $nops;
			}else{
				$keyoffset	= $cntr_obj{$cname}{keyoff};
				$keylen		= $cntr_obj{$cname}{keylen} + $cntr_obj{$cname}{times};
				$datalen	= $cntr_obj{$cname}{datalen} + 10 * $cntr_obj{$cname}{times};
				$nops		= $cntr_obj{$cname}{nops};
				$cntr_obj{$cname}{times} = $cntr_obj{$cname}{times} + 1;
			}
            push(@threads, threads->new (\&worker, $_, $cname, $keyoffset, $keylen, $datalen, $nops));
        }
        $_->join for (@threads);

		for (1 .. $ncntr) {
			$cname = 'ctrn-'. "$_";
			my $cguid		= $chash{$cname};
			if ($cntr_obj{$cname}{times} != 0 ) {
			for (0 .. $cntr_obj{$cname}{times} -1){
				#print "===times: $_ =====\n";
				$nops		= $cntr_obj{$cname}{nops};
				$keyoffset	= $cntr_obj{$cname}{keyoff}; 
				$keylen		= $cntr_obj{$cname}{keylen} + $_; 
				$datalen	= $cntr_obj{$cname}{datalen} + 10 * ($_ ); 
				
				$ret = ZSGet($node->conn($_), $cguid, $keyoffset, $keylen, $datalen, $nops);
				like ($ret, qr/OK.*/, $ret);
			}
			}
			$cntr_obj{$cname}{times} = 0; 
		}

    $ret = ZSGetConts ($node->conn (0), $ncntr);
    $msg = substr($ret, 0, index($ret, "ZSGet"));
    like ($ret, qr/^OK.*/, $msg . "ZSGetContainers");
    for (@cguids) {
        $ret = ZSClose ($node->conn (0), $_);
        like ($ret, qr/^OK.*/, $ret);
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

=comemnt for long run
    my $mode = "short";
    my $arg = shift(@ARGV);
    while ($arg)
    {
        if ($arg eq "-m" or $arg eq "-M")
        {
            $mode = shift(@ARGV);
        }
        $arg = shift(@ARGV);
    }

    if ( defined ($mode) & 'x'.$mode eq 'x'.'long') {
        #$loop   = 1000;
        $loop   = 50;
        #$loop   = 10;
        $nconn  = 64;
        $ncntr  = 32;
    }
=cut

    test_init ();

    test_run ();

    test_clean ();
}

