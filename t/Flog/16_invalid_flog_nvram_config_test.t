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
# date: Apr 7, 2015
# description:

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Fdftest::TestCase;
use Test::More tests => 18;

my $node; 
sub test_run {
    my $cguid;
    my $ret; 
    my $loops = 1;
    my @prop = ([4, "ZS_DURABILITY_HW_CRASH_SAFE", "no"],);
   

    print "\n# Test non-exist flog file#\n ";
    $node->set_ZS_prop(ZS_FLOG_MODE  => "ZS_FLOG_NVRAM_MODE");
    $node->set_ZS_prop(ZS_FLOG_NVRAM_FILE  => "/tmp/nvram_file_test_wrong");
    $ret = $node->start(ZS_REFORMAT => 1);    
    like($ret,qr/failed to start node, so exit program/,"Node Start failed for invalid flog file");
    print "run here\n";

    print "\n# Test invalid offset\n";
    $node->set_ZS_prop(ZS_FLOG_MODE  => "ZS_FLOG_NVRAM_MODE");
    $node->set_ZS_prop(ZS_FLOG_NVRAM_FILE  => "/tmp/nvram_file");
    $node->set_ZS_prop(ZS_FLOG_NVRAM_FILE_OFFSET  => "abc");
    $ret = $node->start(ZS_REFORMAT => 1);    
    like($ret,qr/failed to start node, so exit program/,"Node Start failed for invalid flog file offset");
    print "run here\n";


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
    $node->set_ZS_prop(ZS_FLOG_MODE  => "ZS_FLOG_FILE_MODE");
    $node->set_ZS_prop(ZS_FLOG_NVRAM_FILE  => "");
    $node->set_ZS_prop(ZS_FLOG_NVRAM_FILE_OFFSET  => "");
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


