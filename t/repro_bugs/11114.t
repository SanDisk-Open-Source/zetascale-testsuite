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
# date:
# description: 

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Test::More tests => 4;

my $node; 

sub test_run {

    my $ret;
    my $cguid;
    #my $repeat = 5;
    my $ctr_size = 100000;
    my ($offset,$len)=(0,10);
    my %off_len;
    my $nops=150000;
   
	my $async="no";
	my $write="yes";
	my $keylen=25;
	my $datalen=2000;

            $ret = $node->start(
                    ZS_REFORMAT  => 1,
                    );
            like($ret, qr/OK.*/, 'Node start');

            %off_len = ();
            ($offset,$len)=(0,10);
            #$ctr_size = 2*1024*1024;
            $ctr_size =0;
            print "<<< test with async_write=$async,writethru=$write >>>\n";
            $ret = ZSOpenContainer(
                    $node->conn(0), 
                    cname            => "demo0",
                    fifo_mode        => "no",
                    persistent       => "yes",
                    evicting         => "no",
                    writethru        => "yes",
                    async_writes     => "no", 
                    size             => "$ctr_size",
                    durability_level => "ZS_DURABILITY_HW_CRASH_SAFE",
                    num_shards       => 1,
                    flags            => "ZS_CTNR_CREATE",
                    );
            $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
            like($ret, qr/OK.*/, "ZSOpenContainer canme=demo0,cguid=$cguid,size=$ctr_size,fifo=no,persis=yes,evict=no,writethru=$write,async=$async,flags=CREATE");

                $offset = 500+$offset;
                $len = 40+$len;
                $ret = ZSWriteObject(
                        $node->conn(0),
                        cguid         => "$cguid",     
			#key	      => $offset,
                        key_offset    => $offset, 
                        key_len       => $keylen, 
                        data_offset   => $offset, 
                        data_len      => $datalen, 
                        nops          => $nops,
                        flags         => "ZS_WRITE_MUST_NOT_EXIST",
                        );
                like($ret, qr/OK.*/, "ZSWriteObject-->cguid=$cguid offset=$offset keylen=$keylen datalen=$datalen nops=$nops");

                sleep(15);

                $ret = ZSReadObject(
                        $node->conn(0),
                        cguid         => "$cguid",     
			#key	      => $offset,
                        key_offset    => $offset, 
                        key_len       => $keylen, 
                        data_offset   => $offset, 
                        data_len      => $datalen, 
                        nops          => $nops,
                        check         => "yes",
                        keep_read     => "yes",
                        );
                like($ret, qr/OK.*/, "ZSReadObject->cguid=$cguid offset=$offset keylen=$keylen datalen=$datalen nops=$nops");
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


