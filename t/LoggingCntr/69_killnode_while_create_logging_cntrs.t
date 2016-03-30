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


my $node;
my $ncntr = 256;
my $count:shared;
my $size = 0;
my $ctype = "LOGGING";
my @data = ([50, "aa", "bbb", 160, 0.25], [100, "cdefm", "z", 320, 0.25], [150, "hhg", "os", 640, 0.25], [200, "w", "n", 1280, 0.25]);

sub worker_open{
    #$SIG{'KILL'} = sub { print "killed\n"; threads->exit(); };
    my ($node, $p) = @_;
    $count = 0;
    foreach(1..$ncntr)
   {
        my $ret = OpenContainer($node->conn(0), "ctr-$_", $$p[0], $size, "ZS_CTNR_CREATE", $$p[1], $$p[2], $ctype);
        like ($ret, qr/^OK.*/, $node->{'port'}.":".$ret);
        $count++ if ($ret =~ /OK .*/);
   }
}

sub worker_reopen{
    my ($node, $p) = @_;
    foreach(1..$count)
    {
        my $ret = OpenContainer($node->conn(0), "ctr-$_", $$p[0], $size, "ZS_CTNR_RW_MODE", $$p[1], $$p[2], $ctype);
        like ($ret, qr/^OK.*/, $node->{'port'}.":".$ret);
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
        @threads = ();
        push(@threads, threads->new(\&worker_open,$node,$p)); 
        sleep(1);
        
        $node->kill(); 
        $ret = $node->start(ZS_REFORMAT  => 0,);
        like($ret, qr/OK.*/, 'Node restart');

       
        print "count = $count\n";
        @threads = ();
        push(@threads, threads->new(\&worker_reopen,$node,$p));
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


