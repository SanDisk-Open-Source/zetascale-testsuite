# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file: 
# author: Yiwen Lu
# email: yiwenlu@hengtiansoft.com
# date: Nov 11, 2014
# description: 

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::RemoteNode;
use Fdftest::Stress;
use Test::More 'no_plan';
use threads;
use threads::shared;
use threads 'exit' => 'threads_only';
use Data::Dumper;

my $node;
my @count:shared;
my $num_nodes = 1;
my @nodes;
my @threads;
my %node_cguids;
my $nctr = 65000;
my @ctr_type = ("BTREE","BTREE");
my $ret;
my $cguid;

sub worker_open_ctn{
    $SIG{'KILL'} = sub { print "killed\n"; threads->exit(); };
    my ($node, $index) = @_;
    $count[$index] = 0;
    foreach(1..$nctr)
    {
        $ret = ZSOpen($node->conn(1),"ctr-$_",3,0,"ZS_CTNR_CREATE","yes","ZS_DURABILITY_HW_CRASH_SAFE",$ctr_type[$_%2]);
        like ($ret, qr/^OK.*/, $node->{'port'}.":".$ret);
        $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
        $node_cguids{$node}{$_} = $cguid;   
        $count[$index]++ if ($ret =~ /OK .*/);
    }
}

sub worker_reopen_ctn{
    my ($node, $count) = @_;
    foreach(1..$count)
    {
        $ret = ZSOpen($node->conn(1),"ctr-$_",3,0,"ZS_CTNR_RW_MODE","yes","ZS_DURABILITY_HW_CRASH_SAFE",$ctr_type[$_%2]);
        like ($ret, qr/^OK.*/, $node->{'port'}.":".$ret);
    }
}

sub test_run_node {
    foreach(@nodes)
    {
        $ret = $_->start(
            ZS_REFORMAT  => 1
            );
        like($ret, qr/OK.*/, 'remote engine started');
    }

    
    @threads = ();
    foreach(0..@nodes-1)
    {
        $node = $nodes[$_];
        push(@threads,threads->new(\&worker_open_ctn,$node,$_));
    }
}

sub test_run_recovery {

    print "<<< Test recovery with async_write=yes on remote engine >>>.\n";
    foreach(@nodes)
    {   
        $ret = $_->start(
            ZS_REFORMAT  =>0 
            );
        like($ret, qr/OK.*/, 'remote engine restarted');
    }


    @threads = ();
    foreach(0..@nodes-1)
    {
      
        $node = $nodes[$_];
        $ret = ZSGetConts($node->conn(0),$count[$_]);
        print "ZSGetConts:$ret\n";

        print "start to worker_reopen_ctn,count=$count[$_]\n";
        push(@threads,threads->new(\&worker_reopen_ctn,$node,$count[$_]));
    }
    $_->join for (@threads);
    return;
}

my $remote_dir = "/schooner/data/remote_ftf";
sub test_init {
    my $ssh = shift;
    my $ip = "10.197.16.176";
    my $port = "24422";

    foreach(1..$num_nodes)
    {
        $node = Fdftest::RemoteNode->new(
            ip          => $ip,
            port        => $port+$_,
            nconn       => 128,
            stats_log   => "/tmp/".($port+$_)."/zsstats.log",
            zs_log      => "/tmp/".($port+$_)."/zs.log",
            prop        => "$remote_dir/conf/zs_ins".$_.".prop",
            local_prop  => "$Bin/../../conf/zs_ins".$_.".prop",
            ssh         => $ssh,
        );
        push @nodes, $node;

    }

}

sub test_clean {
    foreach(@nodes){
        print "node $_ to stop\n";
        $_->stop();
        $_->set_ZS_prop(ZS_REFORMAT  => 1);
    }
    return;
}

#
# main
#
{
    my $ssh = prepare(ip => "10.197.16.176");
    shared($ssh);
    test_init($ssh);

    foreach(@nodes){print "node = $_\n";}
    test_run_node();
    sleep(60);
    $nodes[0]->power_reset();
    foreach(@threads){
        $_->kill('KILL');
    }
    foreach(@nodes)
    {
        $_->{ssh} = $nodes[0]->{ssh};
    }
    test_run_recovery();
    test_clean();
}


# clean ENV
END {
    foreach(@nodes){
        $_->clean();
    }
}

