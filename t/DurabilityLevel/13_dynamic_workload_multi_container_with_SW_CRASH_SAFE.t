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
use Test::More tests => 15;
use threads;

my $node;
my $ctr_num = 3;

sub write_obj {
    my ($conn,$cguid) = @_;
    ZSWriteObject (
            $node->conn ($conn),
            cguid       => "$cguid",
            key_offset  => 0,
            key_len     => 25,
            data_offset => 1000,
            data_len    => 50,
            nops        => 100000,
            flags       => "ZS_WRITE_MUST_NOT_EXIST",
            );
}

sub test_run {

    my $ret;
    my $cguid;
    my @cguids;
    my %cguid_objnum;
    my @tmp;
    my $read_failed;
    my $diff;
    
    print "<<test with async_writes=no>>\n";
    $ret = $node->start (ZS_REFORMAT => 1,);
    like ($ret, qr/OK.*/, 'Node start');

    foreach (0 .. $ctr_num - 1) {
        $ret = ZSOpenContainer (
            $node->conn (0),
            cname            => "ctr_$_",
            fifo_mode        => "no",
            persistent       => "yes",
            evicting         => "no",
            writethru        => "yes",
            async_writes     => "no",
            size             => 1048576,
            durability_level => "ZS_DURABILITY_SW_CRASH_SAFE",
            num_shards       => 1,
            flags            => "ZS_CTNR_CREATE",
        );
        $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
        like ($ret, qr/OK.*/, "ZSOpenContainer canme=demo0,cguid=$cguid,async_writes=no,flags=CREATE");
        push @cguids,$cguid;
        $ret = ZSGetContainerProps ($node->conn (0), cguid => "$cguid",);
        like ($ret, qr/.*durability_level=1.*/, "durability_level=ZS_DURABILITY_SW_CRASH_SAFE");
    }


    foreach (1..$ctr_num) {
        threads->new(\&write_obj,$_,$cguids[$_-1]);
    }
    sleep(3);
    $ret = $node->kill_and_dump();

    $ret = $node->start (ZS_REFORMAT => 0,);
    like ($ret, qr/OK.*/, 'Node restart');

    foreach (0 .. $ctr_num - 1) {
        $ret = ZSOpenContainer (
            $node->conn (0),
            cname            => "ctr_$_",
            fifo_mode        => "no",
            persistent       => "yes",
            evicting         => "no",
            writethru        => "yes",
            async_writes     => "no",
            size             => 1048576,
            durability_level => "ZS_DURABILITY_SW_CRASH_SAFE",
            flags            => "ZS_CTNR_RW_MODE",
        );
        $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
        like ($ret, qr/OK.*/, "ZSOpenContainer cguid=$cguid flags=RW_MODE");
    }

    
    $ret = $node->dump_ctnr_success_set();
    @tmp = split(/\n/,$ret);

    foreach(@tmp){
        $cguid_objnum{$1} = $2 if ($_ =~ /(\d+) = (\d+)/);
    }
  
    foreach $cguid (sort keys %cguid_objnum)
    {
        $ret = ZSReadObject (
            $node->conn (0),
            cguid       => "$cguid",
            key_offset  => 0,
            key_len     => 25,
            data_offset => 1000,
            data_len    => 50,
            nops        => 100000,
            check       => "yes",
            keep_read   => "yes",
        );
        $read_failed = $1 if ($ret =~ /SERVER_ERROR (\d+)(\D+)(\d+)(\D+)/);
        $diff = (100000-$read_failed)-$cguid_objnum{$cguid};
        if ($diff <=5 )
        {
            like(0,qr/0/,"ZSReadObject:diff is $diff,set obj number is $cguid_objnum{$cguid},read succeed num is ".(100000-$read_failed));
        }
        else
        {
            like(0,qr/1/,"ZSReadObject:diff is $diff,set obj number is $cguid_objnum{$cguid},read succeed num is ".(100000-$read_failed));
        }
    }


    $ret = $node->stop ();
    like ($ret, qr/OK.*/, 'Node stop');
return;
}

sub test_init {
    $node = Fdftest::Node->new (
        ip    => "127.0.0.1",
        port  => "24422",
        nconn => $ctr_num+1,
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

