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

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::BasicTest;
use Test::More tests => 31;
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
        WriteReadObjects($node->conn(0),$cguid,1000,50,1000,64000,500);
        WriteReadObjects($node->conn(0),$cguid,1000,51,1000,512,500);
        WriteReadObjects($node->conn(0),$cguid,1000,52,1000,512,500);
        WriteReadObjects($node->conn(0),$cguid,1000,53,1000,512,500);
        
        CloseContainer($node->conn(0),$cguid);
    };
    $node->stop();
    
    $ret = $node->start(
        ZS_REFORMAT => 0,
        );
    like($ret, qr/OK.*/, 'Node recovery succeed');
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
    my $zs_log_path="r/Zsck/Z17_metafault_pot_recovery_64K/log_container_".$c_id.".log";
    my $zs_metafault_log_path="r/Zsck/Z17_metafault_pot_recovery_64K/metafault.log";

    my $zsmetafault = "$node->{sdk}/utils/zsmetafault";
    my $zsck        = "$node->{sdk}/utils/zsck";
    my $zsformat    = "$node->{sdk}/utils/zsformat";

    # zsmetafault test
    my $cmd = "";
    $cmd = $cmd . "$zsmetafault --container=".$c_id." --pot 2>".$zs_metafault_log_path;
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
	if(/failed/)
	{
            if(/POT check/)
            {
                 like($_, qr/POT check failed.*/, "POT check failed in meta check.\n");
            }
	    if(/potbm/)
	    {
	         like($_, qr/vdc potbm: failed.*/, "vdc potbm check failed in meta check.\n");
	    }
	}
    }

    open(LOG, "< /tmp/zsck.log") or die "Cannot open log file /tmp/zsck.log ! \n";
    while(<LOG>)
    {
        if(/ERROR/)
        {
            like($_, qr/ZSCHECK_CHECKSUM_ERROR pot checksum invalid.*/, "pot checksum failed in /tmp/zsck.log.\n");
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
        #if($c_id != 0)
        #{
            print "<<<<<< Test with container=".$c_id." >>>.\n";
            test_init();

            test_run();
            test_zsmetafault_and_zsck();
            test_recovery();
            test_clean();
           copy("./r/Zsck/Z17_metafault_pot_recovery_64K/server.log", "./r/Zsck/Z17_metafault_pot_recovery_64K/server_".$c_id.".log");
        #}
    }

}

# clean ENV
END {
    $node->clean();
}
