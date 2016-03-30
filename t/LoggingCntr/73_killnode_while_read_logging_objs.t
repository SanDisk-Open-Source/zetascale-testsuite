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
my $size = 0;
my $ctype = "LOGGING";
my $valoff = 100;
my $nobject = 5000;

my @data = ([50, "aa", "bbb", 160, 0.25], [100, "cdefm", "z", 320, 0.25], [150, "hhg", "os", 640, 0.25], [200, "w", "n", 1280, 0.25]);
my @cguids:shared;

sub worker_write{
    my ($cguid,$index) = @_;
    my $ct=50;
    for(1..$nobject){
        my $ret = WriteLogObjects($node->conn($index), $cguid, $ct, "abc", "efg", $valoff, 160, 1, "ZS_WRITE_MUST_NOT_EXIST");
        like ($ret, qr/^OK.*/, $node->{'port'}.":".$ret);
        $ct++;
    }
}


sub worker_read{
    $SIG{'KILL'} = sub { print "killed\n"; threads->exit(); };
    my ($cguid,$index,$info) = @_;
    my $ct=50;
    for(1..$nobject){
        my $ret = ReadLogObjects($node->conn($index), $cguid, $ct, "abc", "efg", $valoff, 160, 1);
        like($ret, qr/.*/,$info. $ret);
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
            #print "start to write, cguid=$cguid\n";
            push(@threads, threads->new(\&worker_write,$cguid,$_));
        }
        $_->join for (@threads);

        foreach(1..$ncntr){
            my $cguid = $cguids[$_-1];
            push(@threads, threads->new(\&worker_read,$cguid,$_,""));
        }

        sleep(30);

        $node->kill(); 
        foreach(@threads){
            #$_->kill('KILL');
        }
        $ret = $node->start(ZS_REFORMAT  => 0,);
        like($ret, qr/OK.*/, 'Node restart');

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
            push(@threads, threads->new(\&worker_read,$cguid,$_,"Recovery:"));
        }
        $_->join for (@threads);
    }





    return;
}

sub test_init {
    $node = Fdftest::Node->new(
        ip     => "127.0.0.1",
        port   => "24422",
        nconn  => 128,
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


