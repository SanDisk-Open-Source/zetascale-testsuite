# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# author: Runhan Mao
# email: runhanmao@hengtiansoft.com
# date: Dec 11, 2014
# description:

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::BasicTest;
use Test::More tests => 32;
use File::Copy;

my $node;
my $c_id;
my $command;
sub test_run {
    my $ret;
    my $cguid;

    $ret = $node->start(
               ZS_REFORMAT  => 1,
               time_out => 60,
           );
    like($ret, qr/OK.*/, 'Node start');

    subtest "Test with ctnr c0" => sub {

        $ret=OpenContainer($node->conn(0), "c0","ZS_CTNR_CREATE",1048576,0,"ZS_DURABILITY_PERIODIC","yes");
        $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
        WriteReadObjects($node->conn(0),$cguid,1000,50,1000,128000,500);
        WriteReadObjects($node->conn(0),$cguid,1000,51,1000,512,500);
        WriteReadObjects($node->conn(0),$cguid,1000,52,1000,512,500);
        WriteReadObjects($node->conn(0),$cguid,1000,53,1000,512,500);
        CloseContainer($node->conn(0),$cguid);
    };
    $node->stop();

    return;
}

sub test_recovery {
    my $ret;
    my $cguid;

    $ret = $node->start(
               ZS_REFORMAT  => 0,
           );
     like($ret, qr/fail.*/, 'Node recovery failed');

    return;
}

sub test_zsmetafault_and_zsck {
    my $ret;
    my $load_env = "export ZS_PROPERTY_FILE=$node->{prop};
                     LD_PRELOAD=$node->{sdk}/lib/libpthread.so.0:/usr/lib64/liblz4.so ZS_LIB=$node->{sdk}/lib/libzs.so";
    my $zs_log_path="r/Zsck/Z10_metafault_seg_list_recovery_128K/log_container_".$c_id.".log";
    my $zs_metafault_log_path="r/Zsck/Z10_metafault_seg_list_recovery_128K/metafault.log";

    my $zsmetafault = "$node->{sdk}/utils/zsmetafault";
    my $zsck        = "$node->{sdk}/utils/zsck";
    my $zsformat    = "$node->{sdk}/utils/zsformat";

    # zsmetafault test
    my $cmd = "";
    $cmd = $cmd . "$zsmetafault --container=".$c_id." --seg_list 2>".$zs_metafault_log_path;
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
        if(/mcd_check_segment_lists/)
        {
            like($_, qr/mcd_check_segment_lists failed.*/, "mcd_check_segment_lists failed in meta check.\n");
        }
        if(/Storm log check/)
        {
            like($_, qr/Storm log check failed. continuing checks.*/, "Storm log check failed in meta check.\n");
        }
    }

    open(LOG, "< /tmp/zsck.log") or die "Cannot open log file /tmp/zsck.log ! \n";
    while(<LOG>)
    {
        if(/ERROR/)
        {
            if(/segment list/)
            {
                like($_, qr/ZSCHECK_CHECKSUM_ERROR segment list.*/, "segment list invalid in /tmp/zsck.log.\n");
            }
            else
            {
                like($_, qr/ZSCHECK_STORM_LOG.*/, "storm log invalid in /tmp/zsck.log.\n");
            }
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
        test_zsmetafault_and_zsck();
        test_recovery();
        test_clean();
        copy("./r/Zsck/Z10_metafault_seg_list_recovery_128K/server.log", "./r/Zsck/Z10_metafault_seg_list_recovery_128K/server_".$c_id.".log")
    }

}

# clean ENV
END {
    $node->clean();
}
