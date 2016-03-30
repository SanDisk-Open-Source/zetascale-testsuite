# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
# file: 94_enumerate_object_after_close_cntr_when_cntr_is_write_through_fifo_no_persistent_yes_evicting_no_mode_test.t
# author: 
# email: ssshen@hengtiansoft.com
# date: Nov 13, 2012
# description: 

#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../../wrapper/lib";
use Fdftest::Fdfapi;
use Fdftest::Node;
use Test::More tests => 6;

my $node; 

sub test_run {
    
    my $ret;
    my $cguid;
    
    $ret = $node->start(
               ZS_REFORMAT  => 1,
           );
    like($ret, qr/OK.*/, 'Node start');
        
    $ret = ZSOpenContainer(
               $node->conn(0), 
               cname            => "cntr1",
               fifo_mode        => "no",
               persistent       => "yes",
               evicting         => "no",
               writethru        => "yes",
               size             => 1048576,
               durability_level => "ZS_DURABILITY_HW_CRASH_SAFE",
               num_shards       => 1,
               flags            => "ZS_CTNR_CREATE",
           );
    $cguid = $1 if ($ret =~ /OK cguid=(\d+)/);
    like($ret, qr/OK.*/, "ZSOpenContainer cname=cntr1,cguid=$cguid,fifo_mode=yes,persistent=yes,evicting=no,writethru=yes,flags=CREATE");
    
    $ret = ZSWriteObject(
               $node->conn(0),
               cguid         => "$cguid",     
               key_offset    => 0, 
               key_len       => 25, 
               data_offset   => 1000, 
               data_len      => 50, 
               nops          => 5000,
               flags         => "ZS_WRITE_MUST_NOT_EXIST",
           );
    like($ret, qr/OK.*/, "ZSWriteObject->cguid=$cguid nops=5000");
        
    $ret = ZSReadObject(
               $node->conn(0),
               cguid         => "$cguid",     
               key_offset    => 0, 
               key_len       => 25, 
               data_offset   => 1000, 
               data_len      => 50, 
               nops          => 5000,
               check         => "yes",
               keep_read     => "yes",
           );
    like($ret, qr/OK.*/, "ZSReadObject->cguid=$cguid nops=5000");
      
    
    $ret = ZSCloseContainer(
               $node->conn(0),
               cguid        => "$cguid",
           );
    like($ret, qr/OK.*/, "ZSCloseContainer->cguid=$cguid");
   
    $ret = ZSEnumerateContainerObjects(
               $node->conn(0),
               cguid         => "$cguid",     
          ); 
    
    like($ret, qr/SERVER_ERROR.*/, "ZSEnumerateContainerObjects cguid=$cguid ");
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


