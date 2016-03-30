# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file:
# author: Jing Xu(Lee)
# email: leexu@hengtiansoft.com
# date: Sep 16, 2014
# description:


#!/usr/bin/perl

use strict;
use warnings;
use threads;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::Stress;
use Fdftest::BasicTest;
use Test::More tests => 573;

my $node; 

sub test_run {
    my ($ret, $cguid);
    my @cguids;
    my $loop = 100;
    my $ncntr = 10;
    my $size = 0;
    my $keyoff = 1000;
    my $key = 1;
    my $val_offset = $key;
    my $flags = 'ZS_RANGE_START_GE|ZS_RANGE_END_LE|ZS_RANGE_KEYS_ONLY';
    my @prop  = ([3,"ZS_DURABILITY_HW_CRASH_SAFE","no"]);
    my @data = ([64, 16000, 3000], [64, 32000, 3000], [64, 128000, 3000], [64, 48, 60000]);

    $ret = $node->start(ZS_REFORMAT  => 1,);
    like($ret, qr/OK.*/, 'Node started');

    foreach my $p(@prop){
        @cguids = ();
        for(0 .. $ncntr-1){
            $ret=OpenContainer($node->conn(0), "ctr-$_","ZS_CTNR_CREATE",$size,$$p[0],$$p[1],$$p[2]);
            $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
            push(@cguids, $cguid);

            $ret = ZSRename($node->conn(0), $cguids[$_], "rename_a_ctr-$_");
            like ($ret, qr/^OK.*/, $ret);

            foreach my $d(@data){
                $ret = ZSWriteObject(
                        $node->conn(0),
                        cguid         => $cguids[$_],
                        key           => $key,
                        key_len       => $$d[0],
                        data_offset   => $val_offset,
                        data_len      => $$d[1],
                        nops          => $$d[2],
                        flags         => "ZS_WRITE_MUST_NOT_EXIST",
                        );
                like($ret, qr/OK.*/, "ZSWriteObject: load $$d[2] objects keylen=$$d[0] datalen=$$d[1] to rename_a_ctr-$_, cguid=$cguids[$_]");
                $ret = ZSReadObject(
                        $node->conn(0),
                        cguid         => $cguids[$_],
                        key           => $key,
                        key_len       => $$d[0],
                        data_offset   => $val_offset,
                        data_len      => $$d[1],
                        nops          => $$d[2],
                        check         => "yes",
                        keep_read     => "yes",
                        );
                like($ret, qr/OK.*/, "ZSReadObject: read $$d[2] objects keylen=$$d[0] datalen=$$d[1] from rename_a_ctr-$_, cguid=$cguids[$_]");
                $key = $key+$$d[2];
            }
            $key = 1;
            
        }
      
        for(0 .. $ncntr - 1){
            foreach my $d(@data){
                $ret = ZSGetRange (
                            $node->conn(0),
                            cguid         => $cguids[$_],
                            keybuf_size   => 60,
                            databuf_size  => 1024,
                            keylen_start  => $$d[0],
                            keylen_end    => $$d[0],
                            start_key     => $key,
                            end_key       => $$d[2]+$key,
                            flags         => $flags,
                            );
                like($ret, qr/OK.*/,"ZSGetRange:Get $key ~ $key+$$d[2],cntr$_,cguid= $cguids[$_],flags=$flags");

                $ret = ZSGetNextRange (
                            $node->conn(0),
                            n_in          => $$d[2],
                            check         => "yes",
                            );
                my $n_out = $1 if($ret =~ /OK n_out=(\d+)/);
                like($ret, qr/OK n_out=$$d[2].*/, "ZSGetNextRange:Get $n_out objects,cguid= $cguids[$_] ,$ret");

                $ret = ZSGetRangeFinish($node->conn(0));
                like($ret, qr/OK.*/, "ZSGetRangeFinish");
                $key = $key+$$d[2];
            }
            $key = 1;
        }
        
        for(0 .. $ncntr-1){
            foreach my $d(@data){        
                $ret = ZSDeleteObject(
                        $node->conn(0),
                        cguid         => $cguids[$_],
                        key           => $key,
                        key_len       => $$d[0],
                        nops          => $$d[2],
                );
                like($ret, qr/OK.*/, "ZSDeleteObject: cguid=$cguids[$_] key_=$key key_len=$$d[0] nops=$$d[2]-->".($ret));
                $key = $key+ $$d[2];
            }
            $key = 1;
        }

            for(@cguids){
                FlushContainer($node->conn(0), $_);
                CloseContainer($node->conn(0), $_);
            }

            $ret = $node->stop();
            like($ret,qr/OK.*/,"Node Stop");
            $ret = $node->start(ZS_REFORMAT => 0);
            like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=0");

            for(0 .. $ncntr-1){
                $ret=OpenContainer($node->conn(0), "rename_a_ctr-$_", "ZS_CTNR_RW_MODE",$size,$$p[0],$$p[1],$$p[2]);
            }

            for(0 .. $ncntr-1){
                $ret = ZSRename($node->conn(0), $cguids[$_], "rename_b_ctr-$_");
                like ($ret, qr/^OK.*/, $ret);
            }

            for(0 .. $ncntr-1){
                $ret = ZSRename($node->conn(0), $cguids[$_], "rename_b_ctr-$_");
                like ($ret, qr/^Error.*/, $ret);
            }
       

        for(0 .. $ncntr-1){
            foreach my $d(@data){
                $ret = ZSWriteObject(
                        $node->conn(0),
                        cguid         => $cguids[$_],
                        key           => $key,
                        key_len       => $$d[0],
                        data_offset   => $val_offset,
                        data_len      => $$d[1],
                        nops          => $$d[2],
                        flags         => "ZS_WRITE_MUST_NOT_EXIST",
                        );
                like($ret, qr/OK.*/, "ZSWriteObject: load $$d[2] objects keylen=$$d[0] datalen=$$d[1] to rename_b_ctr-$_, cguid=$cguids[$_]");
                $ret = ZSReadObject(
                        $node->conn(0),
                        cguid         => $cguids[$_],
                        key           => $key,
                        key_len       => $$d[0],
                        data_offset   => $val_offset,
                        data_len      => $$d[1],                                                       nops          => $$d[2],
                        check         => "yes",
                        keep_read     => "yes",
                        );
                like($ret, qr/OK.*/, "ZSReadObject: read $$d[2] objects keylen=$$d[0] datalen=$$d[1] from rename_b_ctr-$_, cguid=$cguids[$_]");
                $key = $key+$$d[2];
            }
            $key = 1;
        }

        for(0 .. $ncntr - 1){
            foreach my $d(@data){
                $ret = ZSGetRange (
                            $node->conn(0),
                            cguid         => $cguids[$_],
                            keybuf_size   => 60,
                            databuf_size  => 1024,
                            keylen_start  => $$d[0],
                            keylen_end    => $$d[0],
                            start_key     => $key,
                            end_key       => $$d[2]+$key,
                            flags         => $flags,
                            );
                like($ret, qr/OK.*/,"ZSGetRange:Get $key ~ $$d[2]+$key,rename_b_ctr-$_,cguid= $cguids[$_],flags= $flags");

                $ret = ZSGetNextRange (
                            $node->conn(0),
                            n_in          => $$d[2],
                            check         => "yes",
                            );
                my $n_out = $1 if($ret =~ /OK n_out=(\d+)/);
                like($ret, qr/OK n_out=$$d[2].*/, "ZSGetNextRange:Get $n_out objects,cguid= $cguids[$_] ,$ret");

                $ret = ZSGetRangeFinish($node->conn(0));
                like($ret, qr/OK.*/, "ZSGetRangeFinish");
                $key = $key+$$d[2];
            }
            $key = 1;
        }

        for(0 .. $ncntr-1){
            foreach my $d(@data){
                $ret = ZSDeleteObject(
                        $node->conn(0),
                        cguid         => $cguids[$_],
                        key           => $key,
                        key_len       => $$d[0],
                        nops          => $$d[2],
                        );
                like($ret, qr/OK.*/, "ZSDeleteObject: cguid=$cguids[$_] key=$key key_len=$$d[0] nops=$$d[2]-->".($ret));
                $key = $key+ $$d[2];                                                       }
            $key = 1;
        }

        for(@cguids){
            $ret = ZSClose($node->conn(0), $_);
            like ($ret, qr/^OK.*/, $ret);
            $ret = ZSDelete($node->conn(0), $_);
            like ($ret, qr/^OK.*/, $ret);
        }   
    }
}

sub test_init {
    $node = Fdftest::Node->new(
        ip     => "127.0.0.1",
        port   => "24422",
        nconn  => 10,
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
                
