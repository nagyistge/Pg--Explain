#!perl

use Test::More;
use Test::Deep;
use Data::Dumper;
use autodie;

use Pg::Explain;

my @tests = @ARGV;
if (0 == scalar @tests) {
    opendir( my $dir, 't/16-ctes/' );

    my %uniq = ();
    @tests = sort {
            $a =~ /(\d+)/; my $i1 = $1;
            $b =~ /(\d+)/; my $i2 = $1;
            $i1 <=> $i2
        }
        grep { !$uniq{ $_ }++ }
        map { s/\..*//; $_ }
        grep { /^\d+-[a-z]+\.(?:expect|plan)$/ } readdir $dir;

    closedir $dir;
}

plan 'tests' => 2 * scalar @tests;

for my $test ( @tests ) {

    print STDERR 'Working on test ' . $test . "\n" if  $ENV{'DEBUG_TESTS'};

    my $plan_file = 't/16-ctes/' . $test . '.plan';

    my $explain = Pg::Explain->new( 'source_file' => $plan_file );
    isa_ok( $explain, 'Pg::Explain' );

    my $expected = get_expected_from_file( $test );

    my $got = $explain->top_node->get_struct();
    print STDERR Dumper($got) if  $ENV{'DEBUG_TESTS'};

    cmp_deeply( $got, $expected, 'Plan no. ' . $test . ' passed as file.', );
}

exit;

sub get_expected_from_file {
    my $test_no = shift;

    my $filename = 't/16-ctes/' . $test_no . '.expect';

    open my $fh, '<', $filename;
    local $/ = undef;
    my $expected_str = <$fh>;
    close $fh;

    my $expected = eval $expected_str;
    die $@ if $@;

    return $expected;
}
