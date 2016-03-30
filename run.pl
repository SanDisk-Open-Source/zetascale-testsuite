# Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with SanDisk Corp.  Please
# refer to the included End User License Agreement (EULA), "License" or "License.txt" file
# for terms and conditions regarding the use and redistribution of this software.
#
#!/usr/bin/perl 

use strict;
use warnings;
use FindBin;
use lib "$FindBin::Bin/wrapper/lib";
use Log::Log4perl;
use Getopt::Long qw(GetOptions);
use Global::Config qw($config);
use Global::CaseMap;
use Global::Utils;
use IO::File;
use File::Basename qw(fileparse);
use File::Path qw(mkpath rmtree);
use File::Spec::Functions;
use TAP::Harness;
use TAP::Formatter::Console;
use TAP::Parser;
use TAP::Parser::Aggregator;

$| = 1;

### version ###
my $VERSION = 0.01;

chomp(my $root_dir = `echo \$(hostname | cut -d'.' -f 1-1):\$(pwd)`);
my $config_file = "conf/log4perl.conf";
my $status      = 0;

my %options = (
    single_case => "",
    help        => 0,
    plan        => "",
    cname       => "",
    build       => "",
    project     => "Admin",
    verbosity   => 0,
    exit_imm    => 0,
    priority    => 0,
);

our $shared = 0;

if (
    !GetOptions (
        "verbose"   => \$options{verbosity},
        "case=s"    => \$options{single_case},
        "project=s" => \$options{project},
        "plan=s"    => \$options{plan},
        "cname=s"   => \$options{cname},
        "build=s"   => \$options{build},
        "help"      => \$options{help},
        "exit_imm"  => \$options{exit_imm},
        "PR=s"      => \$options{priority},
        "shared"      => \$shared,
    )
    )
{
    usage ();
    exit 1;
}

if ($options{help}) {
    usage ();
    exit 1;
}

unless ( -d "r" ) {
    mkdir "r", 0770 or die $!;
}

if($shared) {
    $ENV{FDF_TEST_FRAMEWORK_SHARED} = 1;
}

Log::Log4perl->init_and_watch ($config_file);

if (    (not $options{single_case})
    and (not $options{cname})
    and ((not $options{plan}) or (not $options{build})))
{
    print "No test case/suite specified.\n";
    usage ();
    exit 1;
}

if ($options{cname}) {
    listCaseIDByName ();
    exit 1;
}

if ($options{priority}) {
    die "Wrong priority specified" unless ($options{priority} =~ m/^([hml])/);
    $options{pr} = $1;
}

my $last_suit  = "";
my $harness    = TAP::Harness->new;
my $aggregator = TAP::Parser::Aggregator->new;
$aggregator->start;

if ($options{single_case}) {
    chomp $options{single_case};
    my @cases = getTestCasesFromLocal ();
    foreach my $case (@cases) {
        my $parser = runSingleCase ($case);
        $aggregator->add (
            "# Test Scripts: $case\n" .
            "# Log Location: " . catfile($root_dir, get_log_dir ($case)) . "\n ",
            $parser);
        my $failed = is_test_failed ($parser);
        print "Log location " . catfile($root_dir, get_log_dir ($case)) . "\n" if ($failed);
        if ($options{plan} and $options{build}) {
            my ($case_id) =
                grep { $getCaseScriptByID{$_} eq $case }
                keys %getCaseScriptByID;
            updateResult ($case_id, $failed) if (defined $case_id);
        }
        $status |= $failed;
        last if ($status and $options{exit_imm});
    }
}
$aggregator->stop;
$harness->summary ($aggregator);

exit $status;

# Subroutines
sub is_test_failed {
    my $parser = shift;
    return 1 unless (defined $parser->tests_planned && defined $parser->passed);
    return 1 unless ($parser->tests_planned == $parser->passed and $parser->failed == 0);
    return 0;
}

sub runSingleCase {
    my $case   = shift;
    my $logger = Log::Log4perl->get_logger;
    print "\n=== [" . localtime() . "] Running: $case\n";
    $logger->info ("Running: $case");

    # Identify log dir
    my $log_dir = get_log_dir ($case);
    my $log_file = catfile ($log_dir, "tap.log");
    mkpath ("$log_dir", 0, 0777) unless (-e "$log_dir");

    # Run case
    my $hole   = IO::File->new ("> /dev/null");
    my $cmd    = "perl $case 2>&1 | tee $log_file";
    my $parser = TAP::Parser->new (
        {
            exec  => [$cmd],
            spool => $options{verbosity} ? \*STDOUT : $hole,
            merge => 1,
        }
    );
    $parser->run;
    $hole->flush ();
    close($hole);
    return $parser;
}

sub getTestCasesFromLocal {
    my @cases;
    if (-d $options{single_case}) {
        my @files;
        my $dh;
        push(@files, $options{single_case});
        while (@files) {
            if (-d $files[0]) {
                opendir $dh, $files[0] or die $!;
                @_ = grep { /^[^\.]/ } readdir $dh;
                foreach (@_) {
                    push(@files, File::Spec->catfile ($files[0], $_));
                }
                closedir $dh;
            }
            elsif ($files[0] =~ /\.t$/) {
                push(@cases, $files[0]);
            }
            shift @files;
        }
    }
    else {
        @cases = ($options{single_case});
    }
    return @cases;
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

    Usage:  ./run.pl [ options ... ]

    Options:

        --verbose       verbose to see each case
        --case=         run single case
        --PR=           priority (h for high, m for medium, l for low)
        --help          print this list
        --shared        allow multiple instances

    Examples:
    
        ./run.pl --verbose --case=t/sample/basic.t
        
EOF
}

