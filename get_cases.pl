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

#!/usr/bin/perl 

use Getopt::Long qw(GetOptions);
use FindBin;
use lib "$FindBin::Bin/wrapper/lib";
use Global::CaseMap;
use Global::Config qw($config);
use Testlink;

#get_cases.pl --project=FDF --plan=FDF_Recovery_Dev_1.1 --build='dev_test'

my %options = (
    help        => 0,
    plan        => "",
    build       => "",
    project     => "Admin",
    priority    => 0,
);

sub getTestCasesFromTestlink {
    my $tl = Testlink->new (
        devKey  => $config->{TESTLINK}{DEVKEY},
        url     => $config->{TESTLINK}{URL},
        project => $options{project},
        plan    => $options{plan},
        build   => $options{build},
    );
    die "Cannot connect to Testlink, $ENV{'TESTLINK_URL'}" unless ($tl->isConnected);
    my $result = $tl->getTestCases;
    if (ref $result eq "ARRAY") {
        print $result->[0]{message}, "\n";
        exit 1;
    }
    filterCaseByPriority ($result) if ($options{priority});
    my @caseArray = sortCaseByTestSuite ($result);
    return @caseArray;
}

sub filterCaseByPriority {
    my $result = shift;
    for my $key (keys %{$result}) {
        my $tc_pr = int($result->{$key}->{priority});
        if ($tc_pr >= 6) {
            $tc_pr = "high";
        } elsif ($tc_pr < 3) {
            $tc_pr = "low";
        } else {
            $tc_pr = "medium";
        }
        delete $result->{$key} if ($tc_pr !~ qr/$options{pr}/);
    }
}

sub sortCaseByTestSuite {
    my $result   = shift;
    my %cases_h  = %$result;
    my @cases_id = sort { $cases_h{$a}->{testsuite_id} <=> $cases_h{$b}->{testsuite_id} } keys %cases_h;
    my @cases_a;
    foreach (@cases_id) {
        push @cases_a, $cases_h{$_};
    }
    return @cases_a;
}

sub usage {
    print <<EOF

    Usage:  ./get_cases.pl [ options ... ]

    Options:

        --project=      project name
        --plan=         plan name
        --build=        build name
        --PR=           priority (h for high, m for medium, l for low)
        --help          print this list
EOF
}

if (
    !GetOptions (
        "project=s" => \$options{project},
        "plan=s"    => \$options{plan},
        "build=s"   => \$options{build},
        "help"      => \$options{help},
        "PR=s"      => \$options{priority},
    )
    )
{
    usage ();
    exit 1;
}

if ($options{help}) {
    usage ();
    exit;
}

@cases = getTestCasesFromTestlink();
foreach my $case (@cases) {
        my $case_id = "fdf-$case->{external_id}";
        print "$getCaseScriptByID{$case_id};"
}

