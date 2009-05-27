#!perl

use Test::More;
use Test::Deep;
use Data::Dumper;
use autodie;

use Pg::Explain;

plan 'tests' => 1;

my $explain = Pg::Explain->new( 'source_file' => 't/plans/18-plan', );

my $top = $explain->top_node;
ok(
    abs( $top->total_exclusive_time - 58.497 ) < 0.001,    # it's float so i have to use < instead of = 0
    'total exclusive time (58.497) - calculated exclusive times of node'
  );
