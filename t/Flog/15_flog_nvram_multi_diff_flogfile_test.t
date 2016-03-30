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

# file:
# author: Yiwen Lu 
# email: yiwenlu@hengtiansoft.com
# date: Apr 9, 2015
# description:


#!/usr/bin/perl

use strict;
use warnings;
use threads;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::MultiNode;
use Fdftest::BasicTest;
use Test::More tests => 316;

my @nodes; 
my $ncntr = 5;
my $nnode = 2;
my @data = ([30, 16000, 3000], [60, 32000, 3000], [90, 64000, 3000], [120, 128000, 3000], [150, 48, 60000]);

sub test_start_node {
    my $ret;
    my $i = 1;

    for(@nodes){
        $ret = $_->start(ZS_REFORMAT  => 1,);
        like($ret, qr/OK.*/, "$i"."th instance started");
        $i++;
    }
}

sub test_restart_node {
    my $ret;

    for(@nodes){
        $ret = $_->stop();
        like ($ret, qr/OK.*/, 'Node stop');
        $ret = $_->start (ZS_REFORMAT => 0,);
        like ($ret, qr/OK.*/, 'Node restart');
    }
}

sub test_run_node {
    my ($node) = @_;
    my ($ret, $cguid);

    for(1 .. $ncntr){
       	$ret=OpenContainer($node->conn(0), "c$_","ZS_CTNR_CREATE",0,4,"ZS_DURABILITY_HW_CRASH_SAFE","no");
      	$cguid = $1 if($ret =~ /OK cguid=(\d+)/);
        foreach my $d(@data){
            WriteReadObjects($node->conn(0), $cguid, 1000, $$d[0], 1000, $$d[1], $$d[2]);
            WriteReadObjects($node->conn(0), $cguid, 1000, $$d[0], 1000 - 1, $$d[1], $$d[2], "ZS_WRITE_MUST_EXIST");
	}
        FlushContainer($node->conn(0), $cguid);
        CloseContainer($node->conn(0), $cguid);
    }
    return;
}

sub test_recovery {
    my ($node) = @_;
    my ($ret, $cguid);

    for(1 .. $ncntr){
        $ret=OpenContainer($node->conn(0), "c$_","ZS_CTNR_RW_MODE",0,4,"ZS_DURABILITY_HW_CRASH_SAFE","no");
        $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
        foreach my $d(@data){
            ReadObjects($node->conn(0), $cguid, 1000, $$d[0], 1000 - 1, $$d[1], $$d[2]);
	}
        CloseContainer($node->conn(0), $cguid);
        DeleteContainer($node->conn(0), $cguid);        
    }
    return;
}

sub test_init {
    for(1 .. $nnode){
        my $port = 24422 + $_;
        my $node = Fdftest::MultiNode->new(
                     ip          => "127.0.0.1", 
                     port        => "$port",
                     nconn       => $nnode,
                     stats_log   => "/tmp/$port/zsstats.log",
                     zs_log      => "/tmp/$port/zs.log",
                     unix_socket => "/tmp/$port/sock",
                     flog_mode => "ZS_FLOG_NVRAM_MODE",
                     flog_nvram_file => "/tmp/nvram_file_".$port,
                     flog_nvram_file_offset => 0,                     
                   );
        push(@nodes, $node);       
        $node->set_ZS_prop(ZS_LOG_BLOCK_SIZE  => 4096);
        print "ZS_LOG_BLOCK_SIZE = ".$node->get_ZS_prop("ZS_LOG_BLOCK_SIZE")."\n";
    }
}

sub test_clean {
    for(@nodes){
        $_->stop();
        $_->set_ZS_prop(ZS_REFORMAT  => 1);
    }
    return;
}

#
# main
#
{
    my @threads;

    # export multi instance flag
    $ENV{ZS_TEST_FRAMEWORK_SHARED} = 1;
    test_init();
    test_start_node();

    @threads = ();
    for(@nodes){
        push(@threads, threads->new(\&test_run_node, $_));
    }
    $_->join for (@threads);

    test_restart_node();

    @threads = ();
    for(@nodes){
        push(@threads, threads->new(\&test_recovery, $_));
    }
    $_->join for (@threads);

    test_clean();
}


# clean ENV
END {
    for(@nodes){
        $_->clean();
    }
}
