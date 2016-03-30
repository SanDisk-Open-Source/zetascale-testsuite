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

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::TestCase;
use Test::More tests => 28;

my $node; 
sub test_run {
    my $cguid;
    my $ret; 
    my $read_failed;
    my @prop = ([4, "ZS_DURABILITY_HW_CRASH_SAFE", "no"],);
    #my @data = ([50, 64000, 7000], [100, 128000, 7000], [150, 512, 42000]);
    my @data = ([64, 16000, 3000], [74, 32000, 3000], [84, 64000, 3000], [94, 128000, 3000], [104, 48, 60000]);
       
    foreach my $p(@prop){
        $ret = $node->start(ZS_REFORMAT => 1);    
	like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=1");
    
        #OpenContainer {$conn,$ctrname,$flags,$size,$choice,$durab,$async_writes);
        $ret = OpenContainer($node->conn(0),"ctr-1","ZS_CTNR_CREATE",0,$$p[0],$$p[1],$$p[2]);
	$cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
        foreach my $d(@data){
            WriteReadObjects($node->conn(0),$cguid,0,$$d[0],1000,$$d[1],$$d[2]);
        }

	$ret = $node->stop();
	like($ret,qr/OK.*/,"Node Stop");
	$ret = $node->start(ZS_REFORMAT => 0);    
	like($ret,qr/OK.*/,"Node Start: REFORMAT=0");
    
	OpenContainer($node->conn(0),"ctr-1","ZS_CTNR_RW_MODE",0,$$p[0],$$p[1],$$p[2]);
   #    ReadObjects($node->conn(0),$cguid,1000,50,1000,50,50000);
 
        foreach my $d(@data){
            $ret = ZSReadObject(
                    $node->conn(0),
                    cguid         => "$cguid",
		    key_offset    => 0,
		    key_len       => $$d[0],
		    data_offset   => 1000,
		    data_len      => $$d[1],
		    nops          => $$d[2],
		    check         => "yes",
		    keep_read     => "yes",
		    );
            if($ret =~ /OK/){
                like('0',qr/0/,"ZSReadObject->Read all items succced");
                like('0',qr/0/,"0 items check failed");
            }
            elsif($ret =~ /SERVER_ERROR (\d+)(\D+)(\d+)(\D+)/){
                $read_failed = $1;
                if($read_failed > 512){
                    like('0',qr/1/,"ZSReadObject->read failed more than 512");
                    like($3,qr/0/,"0 items check failed");
                }
                else{
                    like('0',qr/0/,"ZSReadObject->$1 items read failed");
                    like($3,qr/0/,"0 items check failed");
                }
            }
        }

        CloseContainer($node->conn(0),$cguid);
        DeleteContainer($node->conn(0),$cguid);

        $ret = $node->stop();
        like($ret,qr/OK.*/,"Node Stop");
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


