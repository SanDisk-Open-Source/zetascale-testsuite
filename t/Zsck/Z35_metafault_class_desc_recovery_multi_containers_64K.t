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

# author: Runhan Mao
# email: runhanmao@hengtiansoft.com
# date: Dec 11, 2014
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
use Fdftest::BasicTest;
use Fdftest::Stress;
use Test::More tests => 212;
use File::Copy;

my $node;
my $c_id;
my $command;
my $nconn = 128;
my $res;
sub worker_write{
    my ($con,$cguid) = @_;     
    $res = ZSSet($node->conn($con), $cguid, 50, 50, 1000*64, 5000,"ZS_WRITE_MUST_NOT_EXIST");
    like ($res, qr/^OK.*/, $res);
    $res = ZSSet($node->conn($con), $cguid, 50, 51, 512, 15000, "ZS_WRITE_MUST_NOT_EXIST");
    like ($res, qr/^OK.*/, $res);               
}

sub worker_read{
    my ($con,$cguid) = @_; 
    my $res;
    $res = ZSGet($node->conn($con), $cguid, 50, 50, 1000*64, 5000);
    like ($res, qr/^OK.*/, $res); 
    $res = ZSGet($node->conn($con), $cguid, 50, 51, 512, 15000);     
    like ($res, qr/^OK.*/, $res);
}

    
sub test_run
{
    my $ret;
    my $cguid;
    my @cguids;
    my @threads;
    my $cguid_cname;
    my $nctr = 2*2;
    my @ctr_type = ("BTREE");
    my @choice = (3,5);
    
    foreach(@ctr_type) {
        my $ctr_type = $_;
        print "=== Test with $ctr_type type  container ===\n";
   
        $ret = $node->start(
               ZS_REFORMAT  => 1,
               time_out => 60,
           );
        like($ret, qr/OK.*/, 'Node start: ZS_REFORMAT=1');
        
        foreach(0..$nctr-1)
        {
            $ret = ZSOpen($node->conn(0),"ctr-$_",$choice[$_%2],0,"ZS_CTNR_CREATE","yes","ZS_DURABILITY_SW_CRASH_SAFE",$ctr_type);
            like($ret, qr/^OK.*/, $ret);
            $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
            $cguids[$_]=$cguid;
        }
        
        @threads = ();
        foreach(0..$nctr-1)
        {
            push(@threads, threads->new (\&worker_write, $_, $cguids[$_]));
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
        like($ret, qr/^OK.*/, $ret);
        test_zsmetafault_and_zsck();
    
        $ret = $node->start(
                    ZS_REFORMAT => 0,
                );
        like($ret,qr/fail.*/, "Node restart failed");
    }
        

    return;
}

sub test_zsmetafault_and_zsck {
    my $ret;
    my $load_env = "export ZS_PROPERTY_FILE=$node->{prop};
                    LD_PRELOAD=$node->{sdk}/lib/libpthread.so.0:/usr/lib64/liblz4.so ZS_LIB=$node->{sdk}/lib/libzs.so";
    my $zs_log_path="r/Zsck/Z35_metafault_class_desc_recovery_multi_containers_64K/log_container_".$c_id.".log";
    my $zs_metafault_log_path="r/Zsck/Z35_metafault_class_desc_recovery_multi_containers_64K/metafault.log";

    my $zsmetafault = "$node->{sdk}/utils/zsmetafault"; 
    my $zsck        = "$node->{sdk}/utils/zsck";
    my $zsformat    = "$node->{sdk}/utils/zsformat";

    # zsmetafault test
    my $cmd = "";
    $cmd = $cmd . "$zsmetafault --container=".$c_id." --class_desc 2>".$zs_metafault_log_path;
    print $cmd, "\n";
    system("$load_env $cmd");
    open(INSERT, "< $zs_metafault_log_path") or die "Cannot open log file $zs_metafault_log_path !\n";
    while(<INSERT>)
    {
        if(/meta corruption/)
        {
            like($_, qr/meta corruption succeeded/, "meta corruption succeeded");                                                 
        }
    }

    # zsck test
    $cmd = "";
    $cmd = $cmd . "$zsck --btree 2>".$zs_log_path;
    print $cmd, "\n";
    system("$load_env $cmd");
    open(LOG, "< $zs_log_path") or die "Cannot open log file $zs_log_path !\n";
    while(<LOG>) 
    {
        if(/mcd_check_class_descriptors/)
        {
            like($_, qr/mcd_check_class_descriptors failed.*/, "mcd_check_class_descriptors failed in meta check.\n");
        }
    }

    open(LOG, "< /tmp/zsck.log") or die "Cannot open log file /tmp/zsck.log ! \n";
    while(<LOG>) 
    {
        if(/invalid/)
        {
            like($_, qr/ZSCHECK_MAGIC_ERROR class descriptor.*/, "class descriptor invalid in /tmp/zsck.log.\n"); 
        }
    }

    unlink("/tmp/zsck.log");

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

    return;
}

#
# main
#
{
    for($c_id=-1;$c_id<=2;$c_id++)
    {
        print "<<<<<< Test with container=".$c_id." >>>.\n";
        test_init();

        test_run();
        test_clean();
        copy("./r/Zsck/Z35_metafault_class_desc_recovery_multi_containers_64K/server.log", "./r/Zsck/Z35_metafault_class_desc_recovery_multi_containers_64K/server_".$c_id.".log")
    }

}

# clean ENV
END {
    $node->clean();
}
