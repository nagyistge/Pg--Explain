#!perl -T

use Test::More tests => 2;

BEGIN {
	use_ok( 'Pg::Explain' );
	use_ok( 'Pg::Explain::Node' );
}

diag( "Testing Pg::Explain $Pg::Explain::VERSION, Perl $], $^X" );
