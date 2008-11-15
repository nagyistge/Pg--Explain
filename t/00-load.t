#!perl -T

use Test::More tests => 2;

BEGIN {
	use_ok( 'PostgreSQL::Explain' );
	use_ok( 'PostgreSQL::Explain::Node' );
}

diag( "Testing PostgreSQL::Explain $PostgreSQL::Explain::VERSION, Perl $], $^X" );
