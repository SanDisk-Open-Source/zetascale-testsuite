# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file:
# author: yiwen lu
# email: yiwenlu@hengtiansoft.com
# date: Oct 13, 2014
# description:

#!/usr/bin/perl

use strict;
use warnings;
use Switch;
use threads;


use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::Stress;
use Test::More tests => 76;

my $node; 
my $nconn = 128;
sub worker_write{
    my ($con,$cguid) = @_;
    my $res;
    $res = ZSSet($node->conn($con), $cguid, 50, 50, 64000, 10000, "ZS_WRITE_MUST_NOT_EXIST");
    like ($res, qr/^OK.*/, $res);
    $res = ZSSet($node->conn($con), $cguid, 50, 100, 512, 30000, "ZS_WRITE_MUST_NOT_EXIST");
    like ($res, qr/^OK.*/, $res);
}

sub worker_read{
    my ($con,$cguid) = @_;
    my $res;
    $res = ZSGet($node->conn($con), $cguid, 50, 50, 64000, 10000);
    like ($res, qr/^OK.*/, $res);
    $res = ZSGet($node->conn($con), $cguid, 50, 100, 512, 30000);
    like ($res, qr/^OK.*/, $res);
}

sub test_zsck {
    my ($cmd,$log_name,$grep_content) = @_;
    my $load_env = "export ZS_PROPERTY_FILE=$node->{prop}; \
                    LD_PRELOAD=$node->{sdk}/lib/libpthread.so.0 ZS_LIB=$node->{sdk}/lib/libzs.so";

    my $zsck_bin        = "$Bin/../../bin/zsck";

    my $log_dir = "r/Zsck/03_dd_check_meta_suc_btree_fail_2/"."$log_name";
    system("dd if=/dev/urandom of=/schooner/data/schooner0 seek=5000000 count=5000");
    $cmd = "$zsck_bin  $cmd &> $log_dir";
    #print $cmd, "\n";
    system("$load_env $cmd ");
    my $res_zsck = readpipe('grep \''."$grep_content".'\''." $log_dir");
    chomp($res_zsck);
    return $res_zsck;
}


sub test_run {
    my $cguid;
    my $ret;
    my @cguids;
    my @threads;    
    my %cguid_cname;
    my $nctr = 2*2;
    my @choice = (3,5); 
    my @ctr_type = ("BTREE");

    foreach(@ctr_type){
        my $ctr_type = $_;
        print "=== Test with $ctr_type type  container ===\n";
   
	$ret = $node->start(
			ZS_REFORMAT => 1,
			);    
	like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=1");

	foreach(0..$nctr-1)
	{
	    $ret = ZSOpen($node->conn(0),"ctr-$_",$choice[$_%2],0,"ZS_CTNR_CREATE","yes","ZS_DURABILITY_SW_CRASH_SAFE", $ctr_type);
	    like ($ret, qr/^OK.*/, $ret);
	    $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
	    $cguids[$_]=$cguid;
	    $cguid_cname{$cguid}="ctr-$_";
	}

	@threads = ();
	foreach(0..$nctr-1) 
	{
	    push(@threads, threads->new (\&worker_write,$_, $cguids[$_]));
	}
	$_->join for (@threads);

	@threads = ();
	foreach(0..$nctr-1)
	{
	    push(@threads, threads->new (\&worker_read, $_, $cguids[$_]));
	}
	$_->join for (@threads);

	foreach $cguid (@cguids)
	{
	    $ret = ZSClose($node->conn(0), $cguid);
	    like ($ret, qr/^OK.*/, $ret);
	}

	$ret = $node->stop();
	like($ret,qr/OK.*/,"Node Stop"); 
        
	my $zsck_result;
	$zsck_result = test_zsck("", "$ctr_type"."_zsck_no_btree.log", 'meta check succeeded');
	is($zsck_result, 'meta check succeeded', "$ctr_type type container, no btree check,".' meta check succeed');
	$zsck_result = test_zsck("--btree", "$ctr_type"."_zsck_btree.log", 'btree check failed: ZS_FAILURE');
	is($zsck_result, 'btree check failed: ZS_FAILURE',"$ctr_type type container, btree check,".'catch btree check failed');

    unlink("/tmp/zsck.log");

    $ret = $node->start(
			ZS_REFORMAT => 0,
			);    
	like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=0");

	foreach(0..$nctr-1)
	{
	    $ret = ZSOpen($node->conn(0),"ctr-$_",$choice[$_%2],0,"ZS_CTNR_RW_MODE","yes","ZS_DURABILITY_SW_CRASH_SAFE", $ctr_type);
	    like ($ret, qr/^OK.*/, $ret);
	}

	@threads = ();
	foreach(0..$nctr-1)
	{
	    push(@threads, threads->new (\&worker_read, $_, $cguids[$_]));
	}
	$_->join for (@threads);

	foreach $cguid (@cguids)
	{
	    $ret = ZSClose($node->conn(0), $cguid);
	    like ($ret, qr/^OK.*/, $ret);
	    $ret = ZSDelete($node->conn(0), $cguid);
	    like ($ret, qr/^OK.*/, $ret);
	}
	$ret = $node->stop();
	like($ret,qr/OK.*/,"Node Stop"); 
    
        print "\n";
    }       
    
    
    return;
}

sub test_init {
    $node = Fdftest::Node->new(
                ip     => "127.0.0.1", 
                port   => "24422",
                time_out => 120,
                nconn  => $nconn,
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


