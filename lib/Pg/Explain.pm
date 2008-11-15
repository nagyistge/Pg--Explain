package PostgreSQL::Explain;

use Moose;
use Data::Dumper;
use PostgreSQL::Explain::Node;
use autodie;

has 'source_file' => ( 'is' => 'rw', 'isa' => 'Str', 'clearer' => 'clear_source_file', );
has 'source' => (
    'is'      => 'rw',
    'isa'     => 'Str',
    'lazy'    => 1,
    'default' => \&_read_source_from_file,
);
has 'top_node' => (
    'is'      => 'rw',
    'isa'     => 'PostgreSQL::Explain::Node',
    'lazy'    => 1,
    'builder' => 'parse_source',
);

=head1 NAME

PostgreSQL::Explain - Object approach at reading explain analyze output

=head1 VERSION

Version 0.05

=cut

our $VERSION = '0.05';

=head1 SYNOPSIS

Quick summary of what the module does.

Perhaps a little code snippet.

    use PostgreSQL::Explain;

    my $explain = PostgreSQL::Explain->new('source_file' => 'some_file.out');
    ...

    my $explain = PostgreSQL::Explain->new(
        'source' => 'Seq Scan on tenk1  (cost=0.00..333.00 rows=10000 width=148)'
    );
    ...

=head1 FUNCTIONS

=cut

=head2 BUILD

Moose-called function which handles:

=over

=item * checking if only one of (source, source_file) parameters to constructor has been given

=back

=cut

sub BUILD {
    my $self = shift;
    if ( ( defined $self->source ) && ( defined $self->source_file ) ) {
        Moose->throw_error( "Only one of (source, source_file) parameters has to be provided" );
    }
    return;
}

=head2 parse_source

Internally (from ->BUILD()) called function which parses provided source, and generated appropriate PostgreSQL::Explain::Node objects.

Top level node is stored as $self->top_node.

=cut

sub parse_source {
    my $self = shift;

    my $top_node         = undef;
    my %element_at_depth = ();      # element is hashref, contains 2 keys: node (PostgreSQL::Explain::Node) and subelement-type, which can be: subnode, initplan or subplan.

    my @lines = split /\r?\n/, $self->source;

    LINE:
    for my $line ( @lines ) {
        if (
            my @catch =
            $line =~ m{
                \A
                (\s* (?:->)? \s*)
                (\S.*?)
                \s+
                \( cost=(\d+\.\d+)\.\.(\d+\.\d+) \s+ rows=(\d+) \s+ width=(\d+) \)
                (?:
                    \s+
                    \( actual \s time=(\d+\.\d+)\.\.(\d+\.\d+) \s rows=(\d+) \s loops=(\d+) \)
                )?
                \s*
                \z
            }xms
           )
        {
            my $new_node = PostgreSQL::Explain::Node->new(
                'type'                   => $catch[ 1 ],
                'estimated_startup_cost' => $catch[ 2 ],
                'estimated_total_cost'   => $catch[ 3 ],
                'estimated_rows'         => $catch[ 4 ],
                'estimated_row_width'    => $catch[ 5 ],
                'actual_time_first'      => $catch[ 6 ],
                'actual_time_last'       => $catch[ 7 ],
                'actual_rows'            => $catch[ 8 ],
                'actual_loops'           => $catch[ 9 ],
            );
            my $element = { 'node' => $new_node, 'subelement-type' => 'subnode', };

            if ( 0 == scalar keys %element_at_depth ) {
                $element_at_depth{ length $catch[ 0 ] } = $element;
                $top_node = $new_node;
                next LINE;
            }
            my @existing_depths = sort { $a <=> $b } keys %element_at_depth;
            for my $key ( grep { $_ >= length($catch[ 0 ]) } @existing_depths ) {
                delete $element_at_depth{ $key };
            }

            my $maximal_depth = ( sort { $b <=> $a } keys %element_at_depth )[ 0 ];
            my $previous_element = $element_at_depth{ $maximal_depth };

            $element_at_depth{ length $catch[ 0 ] } = $element;

            if ( $previous_element->{ 'subelement-type' } eq 'subnode' ) {
                $previous_element->{ 'node' }->add_sub_node( $new_node );
            }
            elsif ( $previous_element->{ 'subelement-type' } eq 'initplan' ) {
                $previous_element->{ 'node' }->add_initplan( $new_node );
            }
            elsif ( $previous_element->{ 'subelement-type' } eq 'subplan' ) {
                $previous_element->{ 'node' }->add_subplan( $new_node );
            }
            else {
                my $msg = "Bad subelement-type in previous_element - this shouldn't happen - please contact author.\n";
                Moose->throw_error( $msg );
            }

        }
        elsif ( $line =~ m{ \A (\s*) ((?:Sub|Init)Plan) \s* \z }xms ) {
            my ( $prefix, $type ) = ($1, $2);

            my @remove_elements = grep { $_ >= length $prefix } keys %element_at_depth;
            delete @element_at_depth{@remove_elements} unless 0 == scalar @remove_elements;

            my $maximal_depth = ( sort { $b <=> $a } keys %element_at_depth )[ 0 ];
            my $previous_element = $element_at_depth{ $maximal_depth };

            $element_at_depth{ length $prefix } = {
                'node'            => $previous_element->{ 'node' },
                'subelement-type' => lc $type,
            };
            next LINE;
        }
        elsif ( $line =~ m{ \A (\s*) ( \S .* \S ) \s* \z }xms ) {
            my ( $infoprefix, $info ) = ( $1, $2 );
            my $maximal_depth = ( sort { $b <=> $a } grep { $_ < length $infoprefix } keys %element_at_depth )[ 0 ];
            next LINE unless defined $maximal_depth;
            my $previous_element = $element_at_depth{ $maximal_depth };
            next LINE unless $previous_element;
            $previous_element->{'node'}->add_extra_info( $info );
        }
    }

    return $top_node;
}

sub _read_source_from_file {
    my $self = shift;

    Moose->throw_error( 'One of (source, source_file) parameters has to be provided' ) unless defined $self->source_file;

    open my $fh, '<', $self->source_file;
    local $/ = undef;
    my $content = <$fh>;
    close $fh;

    $self->clear_source_file;

    return $content;
}

=head1 AUTHOR

hubert depesz lubaczewski, C<< <depesz at depesz.com> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-postgresql-explain at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=PostgreSQL-Explain>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.




=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc PostgreSQL::Explain


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=PostgreSQL-Explain>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/PostgreSQL-Explain>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/PostgreSQL-Explain>

=item * Search CPAN

L<http://search.cpan.org/dist/PostgreSQL-Explain>

=back


=head1 ACKNOWLEDGEMENTS


=head1 COPYRIGHT & LICENSE

Copyright 2008 hubert depesz lubaczewski, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.


=cut

1;    # End of PostgreSQL::Explain
