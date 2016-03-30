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
use Test::More tests => 28;
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
    like ($ret, qr/OK.*/, "ZSMPut: load $numobjs objects key=$key_offset,keylen=$keylen data=$val_offset,datalen=$datalen,flags=$flag,cguid=$cguid");
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
    my $cname      = "Cntr";
    my $key_offset = 1000;
    my $val_offset = 1000;
    my $size       = 0;
    my $ncntr = 3;
    my @prop = (["yes","yes","no","ZS_DURABILITY_HW_CRASH_SAFE","no"],);
    #my @data = ([50, 64000, 6250], [100, 128000, 6250], [150, 512, 37500]);
    my @data = ([64, 16000, 3000], [74, 32000, 3000], [84, 64000, 3000], [94, 128000, 3000], [104, 48, 60000]);

    $ret = $node->start (ZS_REFORMAT => 1);
    like ($ret, qr/OK.*/, 'Node start');

    foreach my $p(@prop){
        foreach my $i(0..$ncntr-1){
            $ret = ZSOpenContainer (
                    $node->conn (0),
                    cname            => $cname.$i,
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
            $cguids[$i] = $1 if ($ret =~ /OK cguid=(\d+)/);
            like ($ret, qr/OK.*/, "ZSopenContainer: $cname.$i, cguid=$cguids[$i] flags=CREATE");
        }
        MPut($node->conn(0),$cguids[0],$key_offset,$data[0][0],$val_offset,$data[0][1],$data[0][2],"ZS_WRITE_MUST_NOT_EXIST");
        MPut($node->conn(0),$cguids[1],$key_offset,$data[1][0],$val_offset,$data[1][1],$data[1][2],"ZS_WRITE_MUST_NOT_EXIST");
        MPut($node->conn(0),$cguids[2],$key_offset,$data[2][0],$val_offset,$data[2][1],$data[2][2],"ZS_WRITE_MUST_NOT_EXIST");

        my @threads = ();
        my $id = 0;
        foreach my $i (0..$ncntr-1){
            push(@threads, threads->new(\&MPut,$node->conn($id++),$cguids[$i],$key_offset,$data[$i][0],$val_offset,$data[$i][1],$data[$i][2],0));
            push(@threads, threads->new(\&enum,$node->conn($id++),$cguids[$i],$data[$i][2]));
            push(@threads, threads->new(\&RangeQuery,$node->conn($id++),$cguids[$i],$data[$i][2]));
            push(@threads, threads->new(\&Read,$node->conn($id++),$cguids[$i],$key_offset,$data[$i][0],$val_offset,$data[$i][1],$data[$i][2]));
        }

        $_->join() for (@threads);
        
        $ret = $node->kill ();

        $ret = $node->start (ZS_REFORMAT => 0,);
        like ($ret, qr/OK.*/, 'Node restart');

        foreach my $i (0..$ncntr-1){
            $ret = ZSOpenContainer (
                    $node->conn (0),
                    cname            => $cname.$i,
                    flags            => "ZS_CTNR_RW_MODE"
                    );
            $cguids[$i] = $1 if ($ret =~ /OK cguid=(\d+)/);
            like ($ret, qr/OK.*/, "ZSopenContainer: $cname, cguid=$cguids[$i] flags=RW_MODE->$ret");
        }

        Read($node->conn(0),$cguids[0],$key_offset,$data[0][0],$val_offset,$data[0][1],$data[0][2]);
        enum($node->conn(0),$cguids[1],$data[1][2]);
        
        MPut($node->conn(0),$cguids[0],$key_offset,$data[0][0],$val_offset+100,$data[0][1],$data[0][2],"ZS_WRITE_MUST_EXIST");
        MPut($node->conn(0),$cguids[2],$key_offset,$data[2][0],$val_offset+10,$data[2][1],$data[2][2],0);
        Read($node->conn(0),$cguids[0],$key_offset,$data[0][0],$val_offset+100,$data[0][1],$data[0][2]);
        Read($node->conn(0),$cguids[2],$key_offset,$data[2][0],$val_offset+10,$data[2][1],$data[2][2]);
        enum($node->conn(0),$cguids[2],$data[2][2]);

        foreach my $i (0..$ncntr-1){
            $ret = ZSCloseContainer ($node->conn (0), cguid => $cguids[$i],);
            like ($ret, qr/OK.*/, "ZSCloseContainer: cguid=$cguids[$i]");
            $ret = ZSDeleteContainer ($node->conn (0), cguid => $cguids[$i],);
            like ($ret, qr/OK.*/, "ZSDeleteContainer: cguid=$cguids[$i]");
        }
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

