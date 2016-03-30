# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
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
use Test::More tests => 53;

my $node; 
sub test_run {
    my $cguid;
    my $ret; 
    my $i = 0;
    my $repeat_time = 3;
    my %keyoff_keylen;
    my @prop = ([3, "ZS_DURABILITY_HW_CRASH_SAFE", "no"],);
    my @data = ([64000, 700], [128000, 700], [512, 4200]);
    my @flog_mode = ("ZS_FLOG_FILE_MODE","ZS_FLOG_NVRAM_MODE");

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
                $ret =  WriteObjects($node->conn(0),$cguid,$keyoffset,$keylen+$i,1000,$$d[0],$$d[1],"ZS_WRITE_MUST_NOT_EXIST");
	        like($ret,qr/OK.*/,"$ret");
                $ret = ReadObjects($node->conn(0),$cguid,$keyoffset,$keylen+$i,1000,$$d[0],$$d[1]);
	        like($ret,qr/OK.*/,"$ret");
                $i++;
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
                    $ret = ReadObjects($node->conn(0),$cguid,$_,$keyoff_keylen{$_}+$i,1000,$$d[0],$$d[1]);
	            like($ret,qr/OK.*/,"$ret");
                    $i++;
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


