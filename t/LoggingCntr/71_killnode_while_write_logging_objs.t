# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
#clean file:
# author: Jing Xu(Lee)
# email: leexu@hengtiansoft.com
# date: Jul 8, 2014
# description:
 
#!/usr/bin/perl

use strict;
use warnings;
use threads;
use Switch;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::UnifiedAPI;
use Test::More 'no_plan';
use threads::shared;
use threads 'exit' => 'threads_only';


my $node;
my $ncntr = 5;
my @count:shared;
my $size = 0;
my $ctype = "LOGGING";
my $valoff = 100;
my $nobject = 5000;

my @data = ([50, "aa", "bbb", 160, 0.25], [100, "cdefm", "z", 320, 0.25], [150, "hhg", "os", 640, 0.25], [200, "w", "n", 1280, 0.25]);
my @cguids:shared;

sub worker_write{
    $SIG{'KILL'} = sub { print "killed\n"; threads->exit(); };
    my ($cguid,$index) = @_;
    my $ct=50;
    for(1..$nobject){
        my $ret = WriteLogObjects($node->conn($index), $cguid, $ct, "abc", "efg", $valoff, 160, 1, "ZS_WRITE_MUST_NOT_EXIST");
        like ($ret, qr/^OK.*/, $node->{'port'}.":".$ret);
        $count[$index]++ if ($ret =~ /.*OK.*/);
        $ct++;
    }
}

sub worker_read{
    my ($cguid,$index) = @_;
    my $ct=50;
    for(1..$count[$index]){
        my $ret = ReadLogObjects($node->conn($index), $cguid, $ct, "abc", "efg", $valoff, 160, 1);
        like($ret, qr/^OK.*/, $ret);
        $ct++;
    }
}

sub test_run {
    my (@cguids, @threads, $ret);
    my @prop = ([3, "no", "ZS_DURABILITY_HW_CRASH_SAFE"]);
    $ret = $node->start(
        ZS_REFORMAT  => 1,
    );
    like($ret, qr/OK.*/, 'Node start');

    foreach my $p(@prop){
        foreach(1..$ncntr)
        {
            my $ret = OpenContainer($node->conn(0), "ctr-$_", $$p[0], $size, "ZS_CTNR_CREATE", $$p[1], $$p[2], $ctype);
            like ($ret, qr/^OK.*/, $node->{'port'}.":".$ret);
            my $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
            push(@cguids, $cguid);
        }
        
        foreach(1..$ncntr){
            my $cguid = $cguids[$_-1];
            print "start to write, cguid=$cguid\n";
            push(@threads, threads->new(\&worker_write,$cguid,$_));
        }
        sleep(60);
        
        $node->kill(); 
        foreach(@threads){
            $_->kill('KILL');
        }
        $ret = $node->start(ZS_REFORMAT  => 0,);
        like($ret, qr/OK.*/, 'Node restart');

        foreach(1..$ncntr){
            print "count = $count[$_]\n";
        }

        foreach(1..$ncntr)
        {
            my $ret = OpenContainer($node->conn(0), "ctr-$_", $$p[0], $size, "ZS_CTNR_RW_MODE", $$p[1], $$p[2], $ctype);
            like ($ret, qr/^OK.*/, $node->{'port'}.":".$ret);
            my $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
            push(@cguids, $cguid);
        }

        @threads = ();
        foreach(1..$ncntr){
            my $cguid = $cguids[$_-1];
            push(@threads, threads->new(\&worker_read,$cguid,$_));
        }
        $_->join for (@threads);
    }





    return;
}

sub test_init {
    $node = Fdftest::Node->new(
        ip     => "127.0.0.1",
        port   => "24422",
        nconn  => 4,
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


