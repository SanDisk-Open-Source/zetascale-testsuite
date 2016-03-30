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
# author: yiwen lu
# email: yiwenlu@hengtiansoft.com
# date: Jan 3, 2013
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
use Fdftest::Stress;
use Test::More tests => 13016;

my $node; 
my $nconn = 1;

sub test_run {
    my $cguid;
    my $ret;
    my $choice; 
    my @cguids;
    my @threads;    
    my %cguid_cname;
    my $nctr = 2*1;
    my @ctr_type = ("BTREE","HASH");   
    my @prop = (["yes","no","yes","ZS_DURABILITY_HW_CRASH_SAFE","no"],);
    #my @data = ([50, 64000, 6250], [100, 128000, 6250], [150, 512, 37500]);
    my @data = ([64, 16000, 3000], [74, 32000, 3000], [84, 64000, 3000], [94, 128000, 3000], [104, 48,60000]);

    $ret = $node->start(ZS_REFORMAT => 1);    
    like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=1");
   
    foreach(0..1)
    {
        $choice = $_;
        print "choice=$choice\n";  
        foreach(0..$nctr-1)
        {
            $ret = ZSOpen($node->conn(0),"ctr-$_",$choice,0,"ZS_CTNR_CREATE","yes","ZS_DURABILITY_HW_CRASH_SAFE",$ctr_type[$_%2]);
            like ($ret, qr/^OK.*/, $ret);
            $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
            $cguids[$_]=$cguid;
            $cguid_cname{$cguid}="ctr-$_";
        }

        my $loop = 1000;
        foreach(0..$loop-1)
        {
            my ($keyoff, $keylen, $datalen, $nops) = (int(rand(50)), int($data[$_%3][0]), int($data[$_%3][1]), $data[$_%3][2]);
            foreach(0..$nctr-1) 
            {
                my $cguid = $cguids[$_];
                $ret = ZSWriteObject(
                              $node->conn(0),
                              cguid           => $cguid,
                              key             => $keyoff,
                              key_len         => $keylen,
                              data_offset     => $keyoff,
                              data_len        => $datalen,
                              nops            => $nops,
                              flags           =>"ZS_WRITE_MUST_NOT_EXIST",
                              );
                like ($ret, qr/^OK.*/, "ZSWriteObject key=$keyoff,nops=$nops->$ret");

                $ret = ZSReadObject(
                              $node->conn(0),
                              cguid           => $cguid,
                              key             => $keyoff,
                              key_len         => $keylen,
                              data_offset     => $keyoff,
                              data_len        => $datalen,
                              nops            => $nops,
                              check           => "yes",
                              );
                like ($ret, qr/^OK.*/, "ZSReadObject->$ret");

                $ret = ZSFlushRandom($node->conn(0), $cguid, $keyoff);
                like ($ret, qr/^OK.*/, $ret);

				if($_%2 == 1){
					$ret = ZSEnumerate($node->conn(0), $cguid);
					my $num_count = $1 if ($ret =~ /.*enumerate (\d+) objects.*/);
					like ($ret, qr/^OK.*/, $ret);
					is ($num_count, $nops, "Enumerate:expect num is $nops;".$ret);
				}
				if($_%2 ==0){
					$ret = ZSGetRange (
									  $node->conn(0),
									  cguid         => $cguid,
									  );
					like($ret, qr/OK.*/,"ZSGetRange:->$ret");
					$ret = ZSGetNextRange (
									  $node->conn(0),
									  n_in          => $nops+10000000,
									  check         => "yes",
									  );
					my $n_out = $1 if($ret =~ /OK n_out=(\d+)/);
					like($ret, qr/OK n_out=$nops.*/, "ZSGetNextRange:Get $n_out objects ,$ret");

					$ret = ZSGetRangeFinish($node->conn(0));
					like($ret, qr/OK.*/, "ZSGetRangeFinish");
				}
                $ret = ZSDeleteObject(
                              $node->conn(0),
                              cguid            => $cguid,
                              key              => $keyoff,
                              key_len          => $keylen,
                              nops             => $nops
                              );
                like ($ret, qr/^OK.*/, "ZSDeleteObject,key=$keyoff,nop=$nops->$ret");

            }
        }
   
        foreach $cguid (@cguids)
        {
            $ret = ZSClose($node->conn(0), $cguid);
            like ($ret, qr/^OK.*/, $ret);
            $ret = ZSDelete($node->conn(0), $cguid);
            like ($ret, qr/^OK.*/, $ret);
        }
       
    }
    return;
}

sub test_init {
    $node = Fdftest::Node->new(
                ip     => "127.0.0.1", 
                port   => "24422",
                nconn  => $nconn,
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


