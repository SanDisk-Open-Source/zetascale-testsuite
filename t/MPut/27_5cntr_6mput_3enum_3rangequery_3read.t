# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file:
# author: shujing zhu(lisa)
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
use Test::More tests => 61;
use threads;

my $node;

sub MPut{
    my ($connid, $cguid, $key_offset,$keylen,$val_offset,$datalen,$numobjs,$flag) = @_;
    my $ret = ZSMPut (
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
}

sub enum{
    my ($connid, $cguid, $enum_num) = @_;
    
    my $ret = ZSEnumerateContainerObjects(
            $connid,
            cguid  => "$cguid",
            );
    chomp($ret);
    like($ret, qr/OK.*/, "ZSEnumerateContainerObjects: cguid=$cguid--> ".($ret));

    $ret = ZSNextEnumeratedObject($connid );
    like($ret, qr/OK enumerate $enum_num objects/, "ZSNextEnumeratedObject: cguid=$cguid-->".($ret));
    $ret = ZSFinishEnumeration($connid );
    like($ret, qr/OK.*/, "ZSFinishEnumeration: cguid=$cguid-->".($ret));
}
sub RangeQuery{
    my($connid, $cguid, $nobject,) = @_;
    
    my $ret = ZSGetRange (
            $connid,
            cguid         => $cguid,
            );
    like($ret, qr/OK.*/,"ZSGetRange: all-> $ret");

    $ret = ZSGetNextRange (
            $connid,
            n_in          => $nobject+10,
            check         => "yes",
            );
    my $n_out = $1 if($ret =~ /OK n_out=(\d+)/);
    like($ret, qr/OK n_out=$nobject.*/, "ZSGetNextRange:Get $n_out objects ,$ret");

    $ret = ZSGetRangeFinish($connid);
    like($ret, qr/OK.*/, "ZSGetRangeFinish");
}

sub Read{
    my($connid, $cguid, $key_offset,$keylen,$val_offset,$datalen,$numobjs,) = @_;
    my $ret = ZSReadObject (
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
}

sub test_run {
    my $ret;
    my @cguids;
    my $cname      = "Cntr-";
    my $key_offset = 1000;
    my $val_offset = 1000;
    my $size       = 0;
    my $ncntr      = 5;
    my $enum_num;
    my @prop = (["yes","yes","no","ZS_DURABILITY_HW_CRASH_SAFE","no"],);
    #my @data = ([50, 64000, 6250], [100, 128000, 6250], [150, 512, 37500]);
    my @data = ([64, 16000, 3000], [74, 32000, 3000], [84, 64000, 3000], [94, 128000, 3000], [104, 48, 60000]);

    $ret = $node->start (ZS_REFORMAT => 1);
    like ($ret, qr/OK.*/, 'Node start');

    foreach my $p(@prop){
        for(my $j = 0; $j < $ncntr; $j++){
            $ret = ZSOpenContainer (
                    $node->conn (0),
                    cname            => $cname.$j,
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
            $cguids[$j] = $1 if ($ret =~ /OK cguid=(\d+)/);
            like ($ret, qr/OK.*/, "ZSopenContainer: $cname.$j, cguid=$cguids[$j] flags=CREATE");
        }

        # mput  objects and start multi threads to mput,enum,range query ,read diff cntr.
        $enum_num = 0;
        foreach my $d(@data){
            for(my $k = 0; $k < $ncntr; $k++){
                MPut($node->conn(0),$cguids[$k],$key_offset,$$d[0],$val_offset,$$d[1],$$d[2],"ZS_WRITE_MUST_NOT_EXIST");
            }
            $enum_num = $enum_num + $$d[2];

            my @threads = ();
            for(my $i = 0;$i< 25; $i=$i+5){
                push(@threads, threads->new(\&MPut,$node->conn($i),$cguids[$i% $ncntr],$key_offset,$$d[0],$val_offset,$$d[1],$$d[2],0));
                push(@threads, threads->new(\&MPut,$node->conn($i+1),$cguids[$i % $ncntr],$key_offset,$$d[0],$val_offset,$$d[1],$$d[2],0));
                push(@threads, threads->new(\&enum,$node->conn($i+2),$cguids[$i % $ncntr],$enum_num));
                push(@threads, threads->new(\&RangeQuery,$node->conn($i+3),$cguids[$i % $ncntr],$enum_num));
                push(@threads, threads->new(\&Read,$node->conn($i+4),$cguids[$i % $ncntr],$key_offset,$$d[0],$val_offset,$$d[1],$$d[2]));
            }

            $_->join() for (@threads);

            Read($node->conn(0),$cguids[0],$key_offset,$$d[0],$val_offset,$$d[1],$$d[2]);
            enum($node->conn(0),$cguids[2],$enum_num);
        }

        for(my $j =0; $j < $ncntr;$j++){
            $ret = ZSCloseContainer ($node->conn (0), cguid => $cguids[$j],);
            like ($ret, qr/OK.*/, "ZSCloseContainer: cguid=$cguids[$j]");
            $ret = ZSDeleteContainer ($node->conn (0), cguid => $cguids[$j],);
            like ($ret, qr/OK.*/, "ZSDeleteContainer: cguid=$cguids[$j]");
        }
    }

    return;
}

sub test_init {
    $node = Fdftest::Node->new (
        ip    => "127.0.0.1",
        port  => "24422",
        nconn => 30,
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

