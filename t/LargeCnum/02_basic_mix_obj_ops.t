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
use Test::More 'no_plan';

my $node; 

sub test_run {
    my ($ret, $cguid, @cguids, $ncntr);
    my $size = 0;
    my $keyoff = 1000;
    my @cnums = (2000, 4000, 8000, 16000);
    #my @data  = ( [50,64000,75], [100,128000,75], [150,512,450] );
    my @data = ([64, 16000, 35], [74, 32000, 35], [84, 64000, 35], [94, 128000, 35], [104, 48, 700]);
    my @prop  = ( [3,"ZS_DURABILITY_HW_CRASH_SAFE","no"]);

    $ret = $node->set_ZS_prop (ZS_MAX_NUM_CONTAINERS => 64000);
    like ($ret, qr//, 'set ZS_MAX_NUM_CONTAINERS to 64K');

    $ret = $node->start(ZS_REFORMAT  => 1,);
    like($ret, qr/OK.*/, 'Node started');

    foreach my $p(@prop){
        @cguids = ();
        $ncntr = $cnums[ rand(@cnums) ];
        print "Cntr num=$ncntr\n";

        #write_obj_after_write_with_must_exist_flag
        print "=== write_obj_after_write_with_must_exist_flag ===\n";
        for(1 .. $ncntr){
            $ret = ZSOpen($node->conn(0),"ctr-$_",$$p[0],$size,"ZS_CTNR_CREATE",$$p[2],$$p[1]);
            like ($ret, qr/^OK.*/, $ret);
            $cguid = $1 if($ret =~ /OK cguid=(\d+)/);
	    push(@cguids, $cguid);
        }

        foreach(@cguids){
            foreach my $d(@data){
                $ret = ZSSet($node->conn(0), $_, $keyoff, $$d[0], $$d[1], $$d[2], "ZS_WRITE_MUST_NOT_EXIST");
                like ($ret, qr/^OK.*/, $ret);
                $ret = ZSGet($node->conn(0), $_, $keyoff, $$d[0], $$d[1], $$d[2]);
                like ($ret, qr/^OK.*/, $ret);
            }
        }

        foreach(@cguids){
            foreach my $d(@data){
                $ret = ZSSet($node->conn(0), $_, $keyoff, $$d[0], $$d[1], $$d[2], "ZS_WRITE_MUST_EXIST");
                like ($ret, qr/^OK.*/, $ret);
                $ret = ZSGet($node->conn(0), $_, $keyoff, $$d[0], $$d[1], $$d[2]);
                like ($ret, qr/^OK.*/, $ret);
            }
        }

        foreach(@cguids){
            $ret = ZSEnumerate($node->conn(0), $_);
            like ($ret, qr/^OK.*/, $ret);
        }

        #write_obj_after_write_with_must_not_exist_flag
        print "=== write_obj_after_write_with_must_not_exist_flag ===\n";
        foreach(@cguids){
            foreach my $d(@data){
                $ret = ZSSet($node->conn(0), $_, $keyoff, $$d[0], $$d[1], $$d[2], "ZS_WRITE_MUST_NOT_EXIST");
                like ($ret, qr/SERVER_ERROR ZS_OBJECT_EXISTS.*/, $ret);
            }
        }

        foreach(@cguids){
            $ret = ZSEnumerate($node->conn(0), $_);
            like ($ret, qr/^OK.*/, $ret);
        }

        #write_obj_after_del_with_must_not_exist_flag
        print "=== write_obj_after_del_with_must_not_exist_flag ===\n";
        foreach(@cguids){
            foreach my $d(@data){
                $ret = ZSDel($node->conn(0), $_, $keyoff, $$d[0], $$d[2]);
                like ($ret, qr/^OK.*/, $ret);
            }
        }

        foreach(@cguids){
            foreach my $d(@data){
                $ret = ZSSet($node->conn(0), $_, $keyoff, $$d[0], $$d[1], $$d[2], "ZS_WRITE_MUST_NOT_EXIST");
                like ($ret, qr/^OK.*/, $ret);
            }
        }

        foreach(@cguids){
            $ret = ZSEnumerate($node->conn(0), $_);
            like ($ret, qr/^OK.*/, $ret);
        }

        #write_obj_after_del_with_must_exist_flag
        print "=== write_obj_after_del_with_must_exist_flag ===\n";
        foreach(@cguids){
            foreach my $d(@data){
                $ret = ZSDel($node->conn(0), $_, $keyoff, $$d[0], $$d[2]);
                like ($ret, qr/^OK.*/, $ret);
            }
        }

        foreach(@cguids){
            foreach my $d(@data){
                $ret = ZSSet($node->conn(0), $_, $keyoff, $$d[0], $$d[1], $$d[2], "ZS_WRITE_MUST_EXIST");
                like ($ret, qr/SERVER_ERROR ZS_OBJECT_UNKNOWN.*/, $ret);
            }
        }

        foreach(@cguids){
            $ret = ZSEnumerate($node->conn(0), $_);
            like ($ret, qr/^OK.*/, $ret);
        }

        #write_obj_after_close_cntr
        print "=== write_obj_after_close_cntr ===\n";
        foreach(@cguids){
            $ret = ZSClose($node->conn(0), $_);
            like ($ret, qr/^OK.*/, $ret);
        }

        foreach(@cguids){
            foreach my $d(@data){
                $ret = ZSSet($node->conn(0), $_, $keyoff, $$d[0], $$d[1], $$d[2], "ZS_WRITE_MUST_NOT_EXIST");
                like ($ret, qr/SERVER_ERROR ZS_FAILURE_CONTAINER_NOT_OPEN.*/, $ret);
            }
        }

        #write_obj_after_reopen_cntr
        print "=== write_obj_after_reopen_cntr ===\n";
        for(1 .. $ncntr){
            $ret = ZSOpen($node->conn(0),"ctr-$_",$$p[0],$size,"ZS_CTNR_RW_MODE",$$p[2],$$p[1]);
            like ($ret, qr/^OK.*/, $ret);
        }

        foreach(@cguids){
            foreach my $d(@data){
                $ret = ZSSet($node->conn(0), $_, $keyoff, $$d[0], $$d[1], $$d[2], "ZS_WRITE_MUST_NOT_EXIST");
                like ($ret, qr/^OK.*/, $ret);
            }
        }

        foreach(@cguids){
            $ret = ZSEnumerate($node->conn(0), $_);
            like ($ret, qr/^OK.*/, $ret);
        }

        #write_obj_after_reopen_cntr_with_ro_mode
=pod
        print "=== write_obj_after_reopen_cntr_with_ro_mode ===\n";
        foreach(@cguids){
            $ret = ZSClose($node->conn(0), $_);
            like ($ret, qr/^OK.*/, $ret);
        }

        for(1 .. $ncntr){
            $ret = ZSOpen($node->conn(0),"ctr-$_",$$p[0],$size,"ZS_CTNR_RO_MODE",$$p[2],$$p[1]);
            like ($ret, qr/^OK.*/, $ret);
        }

        foreach(@cguids){
            foreach my $d(@data){
                $ret = ZSSet($node->conn(0), $_, $keyoff, $$d[0], $$d[1], $$d[2], "ZS_WRITE_MUST_EXIST");
                like($ret, qr/ERROR.*/, $ret);
            }
        }

        foreach(@cguids){
            $ret = ZSEnumerate($node->conn(0), $_);
            like ($ret, qr/^OK.*/, $ret);
        }
=cut

        #write_obj_after_del_cntr
        print "=== write_obj_after_del_cntr ===\n";
        foreach(@cguids){
            $ret = ZSClose($node->conn(0), $_);
            like ($ret, qr/^OK.*/, $ret);
            $ret = ZSDelete($node->conn(0), $_);
            like ($ret, qr/^OK.*/, $ret);
        }

        foreach(@cguids){
            foreach my $d(@data){
                $ret = ZSSet($node->conn(0), $_, $keyoff, $$d[0], $$d[1], $$d[2], "ZS_WRITE_MUST_NOT_EXIST");
                like ($ret, qr/SERVER_ERROR ZS_FAILURE_CONTAINER_NOT_FOUND.*/, $ret);
            }
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
    $node->set_ZS_prop(ZS_REFORMAT  => 1, ZS_MAX_NUM_CONTAINERS => 6000);

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
                