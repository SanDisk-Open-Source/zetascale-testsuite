# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file:
# author: shujing zhu
# email: shujingzhu@hengtiansoft.com
# date: Jan 5, 2015
# description:

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::BasicTest;
use Test::More tests => 10;
use threads;

my $node;

sub Minitranx_MPut{
    my ($connid, $cguid, $key_offset,$keylen,$val_offset,$datalen,$numobjs,$flag) = @_;

    my $ret = ZSTransactionStart($connid);
    like($ret, qr/OK.*/, 'ZSTransactionStart->$ret');

    $ret = ZSMPut (
            $connid,
            cguid       => $cguid,
            key_offset  => $key_offset,
            key_len     => $keylen,
            data_offset => $val_offset,
            data_len    => $datalen,
            num_objs    => $numobjs,
            flags       => $flag,
            );
    like ($ret, qr/OK.*/, "ZSMPut: load $numobjs objects keylen= $keylen datalen=$datalen,flags=$flag, cguid=$cguid");
    
    $ret = ZSReadObject (
            $connid,
            cguid       => $cguid,
            key	        => $key_offset,
            key_len     => $keylen,
            data_offset => $val_offset,
            data_len    => $datalen,
            nops        => $numobjs,
            check       => "yes",
            keep_read   => "yes",
            );
    like ($ret, qr/OK.*/, "ZSReadObject: cguid=$cguid nops=$numobjs->$ret");

    $ret = ZSTransactionCommit($connid);
    like($ret, qr/OK.*/, 'ZSTransactionCommit->$ret');
}

sub MPut_enum{
    my ($connid, $cguid, $key_offset,$keylen,$val_offset,$datalen,$numobjs,$flag,$enum_num) = @_;

    my $ret = ZSTransactionStart($connid);
    like($ret, qr/OK.*/, 'ZSTransactionStart->$ret');

    $ret = ZSMPut (
            $connid,
            cguid       => $cguid,
            key_offset  => $key_offset,
            key_len     => $keylen,
            data_offset => $val_offset,
            data_len    => $datalen,
            num_objs    => $numobjs,
            flags       => $flag,
            );
    like ($ret, qr/OK.*/, "ZSMPut: load $numobjs objects keylen= $keylen datalen=$datalen,flags=$flag, cguid=$cguid");
    
    $ret = ZSEnumerateContainerObjects(
            $connid,
            cguid  => "$cguid",
            );
    chomp($ret);
    like($ret, qr/OK.*/, "ZSEnumerateContainerObjects: cguid=$cguid--> ".($ret));

    $ret = ZSNextEnumeratedObject($connid );
    like($ret, qr/OK enumerate $enum_num objects/, "ZSNextEnumeratedObject: cguid=$cguid-->".($ret));
    $ret = ZSFinishEnumeration($connid );
    like($ret, qr/OK.*/, "ZSFinishEnumeration: cguid=$cguid-->".($ret));

    $ret = ZSTransactionCommit($connid);
    like($ret, qr/OK.*/, 'ZSTransactionCommit->$ret');
}


sub test_run {
    my $ret;
    my $cguid;
    my $cname      = "Cntr";
    my $key_offset = 1000;
    my $val_offset = 1000;
    my $size       = 0;
    my @prop = (["yes","yes","no","ZS_DURABILITY_HW_CRASH_SAFE","no"],);
    #my @data = ([50, 64000, 6250], [100, 128000, 6250], [150, 512, 37500]);
    my @data = ([64, 16000, 3000], [74, 32000, 3000], [84, 64000, 3000], [94, 128000, 3000], [104, 48, 60000]);

    $ret = $node->start (ZS_REFORMAT => 1);
    like ($ret, qr/OK.*/, 'Node start');

    foreach my $p(@prop){
    	$ret = ZSOpenContainer (
        	    $node->conn (0),
		        cname            => $cname,
                fifo_mode        => "no",
                persistent       => $$p[0],
                writethru        => $$p[1],
                evicting         => $$p[2],
                size             => $size,
                durability_level => $$p[3],
                async_writes     => $$p[4],
                num_shards       => 1,
                flags            => "ZS_CTNR_CREATE"
                );
        $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
        like ($ret, qr/OK.*/, "ZSopenContainer: $cname, cguid=$cguid flags=CREATE");

        my $enum_num = 0;
        my @thd_insert = ();
        foreach my $i (0..$#data){
            my $d = $data[$i];
            push(@thd_insert, threads->new(\&Minitranx_MPut,$node->conn($i),$cguid,$key_offset,$$d[0],$val_offset,$$d[1],$$d[2],"ZS_WRITE_MUST_NOT_EXIST"));
            $enum_num = $enum_num + $$d[2];
        }

        $_->join() for(@thd_insert);

        MPut_enum($node->conn(0),$cguid,$key_offset,$data[0][0],$val_offset,$data[0][1],$data[0][2],0,$enum_num);
        
        my @thd_update = ();
        foreach my $i (0..$#data){
            my $d = $data[$i];
            push(@thd_update, threads->new(\&MPut_enum,$node->conn($i),$cguid,$key_offset,$$d[0],$val_offset+10,$$d[1],$$d[2],"ZS_WRITE_MUST_EXIST",$enum_num));
        }

        $_->join() for(@thd_update);

            
        $ret = ZSCloseContainer ($node->conn (0), cguid => $cguid,);
        like ($ret, qr/OK.*/, "ZSCloseContainer: cguid=$cguid");
        $ret = ZSDeleteContainer ($node->conn (0), cguid => $cguid,);
        like ($ret, qr/OK.*/, "ZSDeleteContainer: cguid=$cguid");
    }

    return;
}

sub test_init {
    $node = Fdftest::Node->new (
        ip    => "127.0.0.1",
        port  => "24422",
        nconn => 20,
    );
}

sub test_clean {
    $node->stop ();
    $node->set_ZS_prop (ZS_REFORMAT => 1);

    return;
}

#
# main
#
{
    test_init ();

    test_run ();

    test_clean ();
}

# clean ENV
END {
    $node->clean ();
}

