# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file: 14_multi_slab_writethru_noevicting_container_repeate_recovery.pl
# author: yiwen lu
# email: yiwenlu@hengtiansoft.com
# date: Nov 15, 2012
# description: recovery several times test for multi persistent container which fifo_mode=no,writethru=yes,evicting=no

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Test::More tests => 16;

my $node; 

sub test_run {

    my $ret;
    my $cguid;
    #my $nops=1310000;
    my $nops=131000;
    my @cguids;
    my @cnames;
    my $i;
    my $rec_times=1;
    my $num_container=2;
    foreach my $async ("no") {
        print "<< test with async=$async >>\n";
        $ret = $node->start(
                ZS_REFORMAT  => 1,
                );
        like($ret, qr/OK.*/, 'Node start');

       for($i=1; $i<=$num_container; $i++){    
            $ret = ZSOpenContainer(
                    $node->conn(0), 
                    cname            => "c$i",
                    fifo_mode        => "no",
                    persistent       => "yes",
                    evicting         => "no",
                    writethru        => "yes",
                    async_writes     => "$async",
                    #size             => 10485760,
                    size             => 20971520,
                    durability_level => "ZS_DURABILITY_SW_CRASH_SAFE",
                    num_shards       => 1,
                    flags            => "ZS_CTNR_CREATE",
                    );
            $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
            like($ret, qr/OK.*/, "ZSOpenContainer canme=c$i,cguid=$cguid,fifo_mode=no,persis=yes,evicting=no,writethru=yes,async=$async,flags=CREATE");

            $ret = ZSWriteObject(
                    $node->conn(0),
                    cguid         => "$cguid",     
                    key_offset    => 0, 
                    key_len       => 25, 
                    data_offset   => 1000, 
                    data_len      => 5000, 
                    nops          => "$nops",
                    flags         => "ZS_WRITE_MUST_NOT_EXIST",
                    );
            like($ret, qr/OK.*/, "ZSWriteObject-->cguid=$cguid nops=$nops");

            $ret = ZSReadObject(
                    $node->conn(0),
                    cguid         => "$cguid",     
                    key_offset    => 0, 
                    key_len       => 25, 
                    data_offset   => 1000, 
                    data_len      => 5000, 
                    nops          => "$nops",
                    check         => "yes",
                    keep_read     => "yes",
                    );
            like($ret, qr/OK.*/, "ZSReadObject->cguid=$cguid nops=$nops");

            $ret = ZSCloseContainer(
                    $node->conn(0),
                    cguid        => "$cguid",
                    );
            like($ret, qr/OK.*/, "ZSCloseContainer->cguid=$cguid");

            push @cguids,$cguid;
        }

        for(my $j=1;$j<=$rec_times;$j++){
            print "=CYCLE$j=\n";
            $ret = $node->stop();
            like($ret, qr/OK.*/, 'Node stop');

            $ret = $node->start(
                    ZS_REFORMAT  => 0,
                    );
            like($ret, qr/OK.*/, 'Node restart');

            for($i=1; $i<=$num_container; $i++){
                $ret = ZSOpenContainer(
                        $node->conn(0), 
                        cname            => "c$i",
                        fifo_mode        => "no",
                        persistent       => "yes",
                        evicting         => "no",
                        writethru        => "yes",
                        async_writes     => "$async",
                        #size             => 1048576,
                        size             => 20971520,
                        durability_level => "ZS_DURABILITY_SW_CRASH_SAFE",
                        num_shards       => 1,
                        flags            => "ZS_CTNR_RW_MODE",
                        );
                $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
                like($ret, qr/OK.*/, "ZSOpenContainer cguid=$cguid flags=RW_MODE");

                $ret = ZSReadObject(
                        $node->conn(0),
                        cguid         => "$cguids[$i-1]",     
                        key_offset    => 0, 
                        key_len       => 25, 
                        data_offset   => 1000, 
                        data_len      => 5000, 
                        nops          => "$nops",
                        check         => "yes",
                        keep_read     => "yes",
                        );
                like($ret, qr/OK.*/, "ZSReadObject->cguid=$cguid nops=$nops");
            }
        }
        $ret = $node->stop();
        like($ret, qr/OK.*/, 'Node stop');

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


