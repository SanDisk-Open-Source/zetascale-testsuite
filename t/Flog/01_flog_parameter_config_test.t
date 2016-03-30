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
   

    print "\n#Test default ZS_FLOG_MODE#\n";
    $node->set_ZS_prop(ZS_FLOG_MODE  => "");
    $ret = $node->start(ZS_REFORMAT => 1);    
    like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=1");
    $ret = readpipe("grep -r ZS_FLOG_MODE r/Flog/01*/server.log");
    is($ret,"","ZS_FLOG_MODE is not set,so grep ZS_FLOG_MODE return null");
    $ret = readpipe("grep -r Flog r/Flog/01*/server.log");
    is($ret,"","grep 'Flog' result is  null");
    $ret = $node->stop();
    like($ret,qr/OK.*/,"Node Stop");
    system("rm $Bin/../../r/Flog/01*/server.log");

    print "\n#Test set ZS_FLOG_MODE = ZS_FLOG_FILE_MODE#\n";
    $node->set_ZS_prop(ZS_FLOG_MODE  => "ZS_FLOG_FILE_MODE");
    $ret = $node->start(ZS_REFORMAT => 1);    
    like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=1");
    $ret = readpipe("grep -r ZS_FLOG_MODE r/Flog/01*/server.log");
    like($ret,qr/.*ZS_FLOG_MODE = ZS_FLOG_FILE_MODE/,"Get ZS_FLOG_MODE = ZS_FLOG_FILE_MODE");
    $ret = readpipe("grep -r Flog r/Flog/01*/server.log");
    is($ret,"","grep 'Flog' result is  null");
    $ret = $node->stop();
    like($ret,qr/OK.*/,"Node Stop");
    system("rm $Bin/../../r/Flog/01*/server.log");

    print "\n#Test set ZS_FLOG_MODE = ZS_FLOG_NVRAM_MODE and default ZS_FLOG_NVRAM_FILE&ZS_FLOG_NVRAM_FILE_OFFSET#\n";
    $node->set_ZS_prop(ZS_FLOG_MODE  => "ZS_FLOG_NVRAM_MODE");
    $ret = $node->start(ZS_REFORMAT => 1);    
    like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=1");
    $ret = readpipe("grep -r ZS_FLOG_MODE r/Flog/01*/server.log");
    like($ret,qr/.*ZS_FLOG_MODE = ZS_FLOG_NVRAM_MODE/,"Get ZS_FLOG_MODE = ZS_FLOG_NVRAM_MODE");
    $ret = readpipe("grep -r Flog r/Flog/01*/server.log");
    like($ret,qr/.*flog_init_nv Flog mode = 2, ZS_FLOG_NVRAM_MODE.*/,"Grep 'Flog' return Flog mode = 2");
    $ret = readpipe("grep -r 'file = /tmp/nvram_file, Start offset = 0' r/Flog/01*/server.log");
    like($ret,qr/.*Start offset = 0*/,"check default nvram file is /tmp/nvram_file, ZS_FLOG_NVRAM_FILE_OFFSET = 0");
    $ret = $node->stop();
    like($ret,qr/OK.*/,"Node Stop");
    system("rm $Bin/../../r/Flog/01*/server.log");


    print "\n#Test set ZS_FLOG_NVRAM_FILE&ZS_FLOG_NVRAM_FILE_OFFSET#\n";
    $node->set_ZS_prop(ZS_FLOG_MODE  => "ZS_FLOG_NVRAM_MODE");
    $node->set_ZS_prop(ZS_FLOG_NVRAM_FILE  => "/tmp/nvram_file_test");
    $node->set_ZS_prop(ZS_FLOG_NVRAM_FILE_OFFSET  => "1024");
    $ret = $node->start(ZS_REFORMAT => 1);    
    like($ret,qr/OK.*/,"Node Start: ZS_REFORMAT=1");
    $ret = readpipe("grep -r ZS_FLOG_MODE r/Flog/01*/server.log");
    like($ret,qr/.*ZS_FLOG_MODE = ZS_FLOG_NVRAM_MODE/,"Get ZS_FLOG_MODE = ZS_FLOG_NVRAM_MODE");
    $ret = readpipe("grep -r Flog r/Flog/01*/server.log");
    like($ret,qr/.*flog_init_nv Flog mode = 2, ZS_FLOG_NVRAM_MODE.*/,"Grep 'Flog' return Flog mode = 2");
    $ret = readpipe("grep -r 'file = /tmp/nvram_file_test, Start offset = 1024' r/Flog/01*/server.log");
    like($ret,qr/.*Start offset = 0*/,"check vvram file is set to /tmp/nvram_file_test, ZS_FLOG_NVRAM_FILE_OFFSET = 1024");
    $ret = $node->stop();
    like($ret,qr/OK.*/,"Node Stop");
 #   system("rm $Bin/../../r/Flog/01*/server.log");


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


