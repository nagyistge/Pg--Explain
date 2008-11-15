package Pg::Explain;

use Moose;
use Data::Dumper;
use Pg::Explain::Node;
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
    'isa'     => 'Pg::Explain::Node',
    'lazy'    => 1,
    'builder' => 'parse_source',
);

=head1 NAME

Pg::Explain - Object approach at reading explain analyze output

=head1 VERSION

Version 0.06

=cut

our $VERSION = '0.06';

=head1 SYNOPSIS

Quick summary of what the module does.

Perhaps a little code snippet.

    use Pg::Explain;

    my $explain = Pg::Explain->new('source_file' => 'some_file.out');
    ...

    my $explain = Pg::Explain->new(
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

=head2 top_node

This method returns the top node of parsed plan.

For example - in this plan:

                           QUERY PLAN
 --------------------------------------------------------------
  Limit  (cost=0.00..0.01 rows=1 width=4)
    ->  Seq Scan on test  (cost=0.00..14.00 rows=1000 width=4)

top_node is Pg::Explain::Node element with type set to 'Limit'.

Generally every output of plans should start with ->top_node(), and descend recursively in it, using subplans(), initplans() and sub_nodes() methods.

=head2 parse_source

Internally (from ->BUILD()) called function which parses provided source, and generated appropriate Pg::Explain::Node objects.

Top level node is stored as $self->top_node.

=cut

sub parse_source {
    my $self = shift;

    my $top_node         = undef;
    my %element_at_depth = ();      # element is hashref, contains 2 keys: node (Pg::Explain::Node) and subelement-type, which can be: subnode, initplan or subplan.

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
            my $new_node = Pg::Explain::Node->new(
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

Please report any bugs or feature requests to C<depesz at depesz.com>.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Pg::Explain

=head1 COPYRIGHT & LICENSE

Copyright 2008 hubert depesz lubaczewski, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.


=cut

1;    # End of Pg::Explain
