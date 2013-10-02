package Pg::Explain::FromText;
use strict;
use Carp;
use Pg::Explain::Node;

=head1 NAME

Pg::Explain::FromText - Parser for text based explains

=head1 VERSION

Version 0.68

=cut

our $VERSION = '0.68';

=head1 SYNOPSIS

It's internal class to wrap some work. It should be used by Pg::Explain, and not directly.

=head1 FUNCTIONS

=head2 new

Object constructor.

This is not really useful in this particular class, but it's to have the same API for all Pg::Explain::From* classes.

=cut

sub new {
    my $class = shift;
    my $self = bless {}, $class;
    return $self;
}

=head2 parse_source

Function which parses actual plan, and constructs Pg::Explain::Node objects
which represent it.

Returns Top node of query plan.

=cut

sub parse_source {
    my $self   = shift;
    my $source = shift;

    my $top_node         = undef;
    my %element_at_depth = ();      # element is hashref, contains 2 keys: node (Pg::Explain::Node) and subelement-type, which can be: subnode, initplan or subplan.

    my @lines = split /\r?\n/, $source;

    LINE:
    for my $line ( @lines ) {

        # There could be stray " at the end. No idea why, but some people paste such explains on explain.depesz.com
        $line =~ s/"\z//;

        if (
            my @catch =
            $line =~ m{
                \A
                (\s* -> \s* | \s* )
                (\S.*?)
                \s+
                \( cost=(\d+\.\d+)\.\.(\d+\.\d+) \s+ rows=(\d+) \s+ width=(\d+) \)
                (?:
                    \s+
                    \(
                        (?:
                            actual \s time=(\d+\.\d+)\.\.(\d+\.\d+) \s rows=(\d+) \s loops=(\d+)
                            |
                            ( never \s+ executed )
                        )
                    \)
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
            if ( defined $catch[ 10 ] && $catch[ 10 ] =~ m{never \s+ executed }xms ) {
                $new_node->actual_loops( 0 );
                $new_node->never_executed( 1 );
            }
            my $element = { 'node' => $new_node, 'subelement-type' => 'subnode', };

            if ( 0 == scalar keys %element_at_depth ) {
                $element_at_depth{ length $catch[ 0 ] } = $element;
                $top_node = $new_node;
                next LINE;
            }
            my @existing_depths = sort { $a <=> $b } keys %element_at_depth;
            for my $key ( grep { $_ >= length( $catch[ 0 ] ) } @existing_depths ) {
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
            elsif ( $previous_element->{ 'subelement-type' } =~ /^cte:(.+)$/ ) {
                $previous_element->{ 'node' }->add_cte( $1, $new_node );
                delete $element_at_depth{ $maximal_depth };
            }
            else {
                my $msg = "Bad subelement-type in previous_element - this shouldn't happen - please contact author.\n";
                croak( $msg );
            }

        }
        elsif ( $line =~ m{ \A (\s*) ((?:Sub|Init)Plan) \s* (?: \d+ \s* )? \s* (?: \( returns .* \) \s* )? \z }xms ) {
            my ( $prefix, $type ) = ( $1, $2 );

            my @remove_elements = grep { $_ >= length $prefix } keys %element_at_depth;
            delete @element_at_depth{ @remove_elements } unless 0 == scalar @remove_elements;

            my $maximal_depth = ( sort { $b <=> $a } keys %element_at_depth )[ 0 ];
            my $previous_element = $element_at_depth{ $maximal_depth };

            $element_at_depth{ length $prefix } = {
                'node'            => $previous_element->{ 'node' },
                'subelement-type' => lc $type,
            };
            next LINE;
        }
        elsif ( $line =~ m{ \A (\s*) CTE \s+ (\S+) \s* \z }xms ) {
            my ( $prefix, $cte_name ) = ( $1, $2 );

            my @remove_elements = grep { $_ >= length $prefix } keys %element_at_depth;
            delete @element_at_depth{ @remove_elements } unless 0 == scalar @remove_elements;

            my $maximal_depth = ( sort { $b <=> $a } keys %element_at_depth )[ 0 ];
            my $previous_element = $element_at_depth{ $maximal_depth };

            $element_at_depth{ length $prefix } = {
                'node'            => $previous_element->{ 'node' },
                'subelement-type' => 'cte:' . $cte_name,
            };

            next LINE;
        }
        elsif ( $line =~ m{ \A (\s*) ( \S .* \S ) \s* \z }xms ) {
            my ( $infoprefix, $info ) = ( $1, $2 );
            my $maximal_depth = ( sort { $b <=> $a } grep { $_ < length $infoprefix } keys %element_at_depth )[ 0 ];
            next LINE unless defined $maximal_depth;
            my $previous_element = $element_at_depth{ $maximal_depth };
            next LINE unless $previous_element;
            $previous_element->{ 'node' }->add_extra_info( $info );
        }
    }
    return $top_node;
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

1;    # End of Pg::Explain::FromText
