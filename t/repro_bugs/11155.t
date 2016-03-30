# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# refer to t/Recovery/N25_recovery_after_increase_container_size_and_del_objects.t
# file:
# author: yiwen lu
# email: yiwenlu@hengtiansoft.com
# date:
# description: 

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Test::More tests => 72;

my $node; 

sub test_run {

    my $ret;
    my $cguid;
    my $repeat = 5;
    my $ctr_size = 500000;
    my ($offset,$len)=(0,10);
    my %off_len;
    my @offsets;
    $ret = $node->start(
               ZS_REFORMAT  => 1,
           );
    like($ret, qr/OK.*/, 'Node start');
    
    $ret = ZSOpenContainer(
               $node->conn(0), 
               cname            => "demo0",
               fifo_mode        => "no",
               persistent       => "yes",
               evicting         => "no",
               writethru        => "yes",
               async_writes     => "yes", 
               size             => "$ctr_size",
               durability_level => "ZS_DURABILITY_PERIODIC",
               num_shards       => 1,
               flags            => "ZS_CTNR_CREATE",
           );
    $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
    like($ret, qr/OK.*/, "ZSOpenContainer canme=demo0,cguid=$cguid,size=$ctr_size,fifo=no,persistent=yes,evict=no,writethru=yes,flags=CREATE");
    
    for(my $i=0; $i<$repeat; $i++){
        $ret = ZSWriteObject(
                $node->conn(0),
                cguid         => "$cguid",     
                key_offset    => $offset, 
                key_len       => $len, 
                data_offset   => $offset, 
                data_len      => $len*100, 
                nops          => 5000,
                flags         => "ZS_WRITE_MUST_NOT_EXIST",
                );
        like($ret, qr/OK.*/, "ZSWriteObject-->cguid=$cguid offset=$offset len=$len nops=5000");

        $ret = ZSReadObject(
                $node->conn(0),
                cguid         => "$cguid",     
                key_offset    => $offset, 
                key_len       => $len, 
                data_offset   => $offset, 
                data_len      => $len*100, 
                nops          => 5000,
                check         => "yes",
                keep_read     => "yes",
                );
        like($ret, qr/OK.*/, "ZSReadObject->cguid=$cguid offset=$offset len=$len nops=5000");

        $off_len{$offset} = $len;
        $offset = 500+$offset;
        $len = 20+$len;
     }
    
    for(my $i=0; $i<$repeat; $i++){
        print "== cycle $i ==\n";
        $ctr_size = $ctr_size*2;
        $ret = ZSSetContainerProps(
                $node->conn(0),
                cguid            => "$cguid",
                fifo_mode        => "no",
                persistent       => "yes",
                evicting         => "no",
                writethru        => "yes",
                size             => "$ctr_size",
                durability_level => "ZS_DURABILITY_PERIODIC",
                num_shards       => 1,
                flags            => "ZS_CTNR_CREATE",
                );
        like($ret, qr/OK.*/, "ZSSetContainerProps->cguid=$cguid reset size=$ctr_size");

        $ret = ZSGetContainerProps(
                $node->conn(0),
                cguid   =>   "$cguid",
                );    
        $ret =~ /.*size=(\d+) kb/;
        is($1,$ctr_size,"ZSGetContainerProps:size=$1 kb");

        @offsets = keys %off_len;
        $offset = $offsets[int(rand(@offsets))];
#        print "offset=$offset";
#        print "len=$off_len{$offset}";
        $ret = ZSDeleteObject ( 
                $node->conn(0),
                cguid      => "$cguid",
                key_offset => $offset,
                key_len    => $off_len{$offset},
                nops       => 5000,
               );
        like($ret, qr/OK.*/, "ZSDeleteObject:cguid=$cguid key_offset=$offset key_len=$off_len{$offset}");
        delete $off_len{$offset};

        foreach(keys %off_len){
            $ret = ZSReadObject(
                    $node->conn(0),
                    cguid         => "$cguid",
                    key_offset    => "$_",
                    key_len       => $off_len{$_},
                    data_offset   => "$_",
                    data_len      => $off_len{$_}*100,
                    nops          => 5000,
                    check         => "yes",
                    keep_read     => "yes",
                    );
            like($ret, qr/OK.*/, "ZSReadObject->cguid=$cguid offset=$_ lenth=$off_len{$_} nops=5000");
        }


        $ret = ZSCloseContainer(
                $node->conn(0),
                cguid     => "$cguid",
                );
        like($ret, qr/OK.*/, "ZSCloseContainer->cguid=$cguid");


        $ret = $node->stop();
        like($ret, qr/OK.*/, 'Node stop');

        $ret = $node->start(
                ZS_REFORMAT  => 0,
                );
        like($ret, qr/OK.*/, 'Node restart');

        $ret = ZSOpenContainer(
                $node->conn(0), 
                cname            => "demo0",
                fifo_mode        => "no",
                persistent       => "yes",
                evicting         => "no",
                writethru        => "yes",
                async_writes     => "yes", 
                size             => "$ctr_size",
                durability_level => "ZS_DURABILITY_PERIODIC",
                num_shards       => 1,
                flags            => "ZS_CTNR_RW_MODE",
                );
        $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
        like($ret, qr/OK.*/, "ZSOpenContainer cguid=$cguid flags=RW_MODE");

        $ret = ZSGetContainerProps(
                $node->conn(0),
                cguid   =>   "$cguid",
                );    
        $ret =~ /.*size=(\d+) kb/;
        is($1,$ctr_size,"ZSGetContainerProps:size=$1 kb");

        foreach(keys %off_len){
            $ret = ZSReadObject(
                    $node->conn(0),
                    cguid         => "$cguid",     
                    key_offset    => "$_", 
                    key_len       => $off_len{$_}, 
                    data_offset   => "$_", 
                    data_len      => $off_len{$_}*100,
                    nops          => 5000,
                    check         => "yes",
                    keep_read     => "yes",
                    );
            like($ret, qr/OK.*/, "ZSReadObject->cguid=$cguid offset=$_ lenth=$off_len{$_} nops=5000");
        }
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


