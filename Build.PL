use strict;
use warnings;
use Module::Build;

my $builder = Module::Build->new(
    module_name       => 'Pg::Explain',
    license           => 'perl',
    dist_author       => 'hubert depesz lubaczewski <depesz@depesz.com>',
    dist_version_from => 'lib/Pg/Explain.pm',
    build_requires    => {
        'autodie'         => 0,
        'Test::More'      => 0,
        'Test::Deep'      => 0,
        'Test::Exception' => 0,
    },
    add_to_cleanup => [ 'Pg-Explain-*' ],
    requires       => { 'Moose' => 0, },
);

$builder->create_build_script();
