package Pg::Explain;
use strict;
use Pg::Explain::Node;
use autodie;
use Carp;

=head1 NAME

Pg::Explain - Object approach at reading explain analyze output

=head1 VERSION

Version 0.20

=cut

our $VERSION = '0.20';

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

=head2 _read_source_from_file

Helper function to read source from file.

=head2 source

Returns source (text version) of explain.

=cut

sub source {
    return shift->{ 'source' };
}

=head2 new

Object constructor.

Takes one of (only one!) (source, source_file) parameters, and either parses it from given source, or first reads given file.

=cut

sub new {
    my $class = shift;
    my $self = bless {}, $class;
    my %args;
    if ( 0 == scalar @_ ) {
        croak( 'One of (source, source_file) parameters has to be provided)' );
    }
    if ( 1 == scalar @_ ) {
        if ( 'HASH' eq ref $_[ 0 ] ) {
            %args = @{ $_[ 0 ] };
        }
        else {
            croak( 'One of (source, source_file) parameters has to be provided)' );
        }
    }
    elsif ( 1 == ( scalar( @_ ) % 2 ) ) {
        croak( 'One of (source, source_file) parameters has to be provided)' );
    }
    else {
        %args = @_;
    }

    if ( $args{ 'source_file' } ) {
        croak( 'Only one of (source, source_file) parameters has to be provided)' ) if $args{ 'source' };
        $self->{ 'source_file' } = $args{ 'source_file' };
        $self->_read_source_from_file();
    }
    elsif ( $args{ 'source' } ) {
        $self->{ 'source' } = $args{ 'source' };
    }
    else {
        croak( 'One of (source, source_file) parameters has to be provided)' );
    }
    return $self;
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

=cut

sub top_node {
    my $self = shift;
    $self->parse_source() unless $self->{ 'top_node' };
    return $self->{ 'top_node' };
}

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
            else {
                my $msg = "Bad subelement-type in previous_element - this shouldn't happen - please contact author.\n";
                croak( $msg );
            }

        }
        elsif ( $line =~ m{ \A (\s*) ((?:Sub|Init)Plan) \s* (?: \d+ \s* )? \z }xms ) {
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
        elsif ( $line =~ m{ \A (\s*) ( \S .* \S ) \s* \z }xms ) {
            my ( $infoprefix, $info ) = ( $1, $2 );
            my $maximal_depth = ( sort { $b <=> $a } grep { $_ < length $infoprefix } keys %element_at_depth )[ 0 ];
            next LINE unless defined $maximal_depth;
            my $previous_element = $element_at_depth{ $maximal_depth };
            next LINE unless $previous_element;
            $previous_element->{ 'node' }->add_extra_info( $info );
        }
    }
    $self->{ 'top_node' } = $top_node;
    return;
}

sub _read_source_from_file {
    my $self = shift;

    open my $fh, '<', $self->{ 'source_file' };
    local $/ = undef;
    my $content = <$fh>;
    close $fh;

    delete $self->{ 'source_file' };
    $self->{ 'source' } = $content;

    return;
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
