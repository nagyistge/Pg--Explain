package PostgreSQL::Explain::Node;

use Moose;
use Data::Dumper;
use Clone qw( clone );
use warnings;
use strict;

has 'actual_loops'           => ( 'is' => 'rw', 'isa' => 'Maybe[Int]', 'required' => 1, );
has 'actual_rows'            => ( 'is' => 'rw', 'isa' => 'Maybe[Int]', 'required' => 1, );
has 'actual_time_first'      => ( 'is' => 'rw', 'isa' => 'Maybe[Num]', 'required' => 1, );
has 'actual_time_last'       => ( 'is' => 'rw', 'isa' => 'Maybe[Num]', 'required' => 1, );
has 'estimated_rows'         => ( 'is' => 'rw', 'isa' => 'Int',        'required' => 1, );
has 'estimated_row_width'    => ( 'is' => 'rw', 'isa' => 'Int',        'required' => 1, );
has 'estimated_startup_cost' => ( 'is' => 'rw', 'isa' => 'Num',        'required' => 1, );
has 'estimated_total_cost'   => ( 'is' => 'rw', 'isa' => 'Num',        'required' => 1, );
has 'type'                   => ( 'is' => 'rw', 'isa' => 'Str',        'required' => 1, );
has 'scan_on'                => ( 'is' => 'rw', 'isa' => 'HashRef' );
has 'extra_info'             => ( 'is' => 'rw', 'isa' => 'ArrayRef' );
has 'sub_nodes'              => ( 'is' => 'rw', 'isa' => 'ArrayRef' );
has 'initplans'              => ( 'is' => 'rw', 'isa' => 'ArrayRef' );
has 'subplans'               => ( 'is' => 'rw', 'isa' => 'ArrayRef' );

=head1 NAME

PostgreSQL::Explain::Node - The great new PostgreSQL::Explain::Node!

=head1 VERSION

Version 0.05

=cut

our $VERSION = '0.05';

=head1 SYNOPSIS

Quick summary of what the module does.

Perhaps a little code snippet.

    use PostgreSQL::Explain::Node;

    my $foo = PostgreSQL::Explain::Node->new();
    ...

=head1 FUNCTIONS

=cut

=head2 BUILD

Moose-called function (from within constructor) which checks if provided node type is one of table scan types, and if so, extracts information from it to $self->scan_on structure.

=cut

sub BUILD {
    my $self = shift;
    if ( $self->type =~ m{ \A (Seq \s Scan ) \s on \s (\S+) (?: \s+ (\S+) ) ? \z }xms ) {
        $self->type( $1 );
        $self->scan_on( { 'table_name' => $2, } );
        $self->scan_on->{ 'table_alias' } = $3 if defined $3;
    } elsif ( $self->type =~ m{ \A (Index \s Scan (?: \s Backward )? ) \s using \s (\S+) \s on \s (\S+) (?: \s+ (\S+) ) ? \z }xms ) {
        $self->type( $1 );
        $self->scan_on(
            {
                'index_name' => $2, 
                'table_name' => $3,
            }
        );
        $self->scan_on->{ 'table_alias' } = $4 if defined $4;
    }
    return;
}

=head2 add_extra_info

Adds new line of extra information to explain node.

It will be available at $node->extra_info (returns arrayref)

=cut

sub add_extra_info {
    my $self = shift;
    if ($self->extra_info) {
        push @{ $self->extra_info }, @_;
    } else {
        $self->extra_info( [ @_ ] );
    }
    return;
}

=head2 add_subplan

Adds new subplan node (for example - where x = (subselect))

It will be available at $node->subplans (returns arrayref)

=cut

sub add_subplan {
    my $self = shift;
    if ($self->subplans) {
        push @{ $self->subplans }, @_;
    } else {
        $self->subplans( [ @_ ] );
    }
    return;
}

=head2 add_initplan

Adds new initplan node (for example - where x = (subselect))

It will be available at $node->initplans (returns arrayref)

=cut

sub add_initplan {
    my $self = shift;
    if ($self->initplans) {
        push @{ $self->initplans }, @_;
    } else {
        $self->initplans( [ @_ ] );
    }
    return;
}

=head2 add_sub_node

Adds new sub node (for example - join sources).

It will be available at $node->sub_nodes (returns arrayref)

=cut

sub add_sub_node {
    my $self = shift;
    if ($self->sub_nodes) {
        push @{ $self->sub_nodes }, @_;
    } else {
        $self->sub_nodes( [ @_ ] );
    }
    return;
}

=head2 get_struct

Function which returns simple, not blessed, hashref with all information about given explain node and it's children.

=cut

sub get_struct {
    my $self  = shift;
    my $reply = {};

    $reply->{ 'estimated_row_width' }    = $self->estimated_row_width        if defined $self->estimated_row_width;
    $reply->{ 'estimated_rows' }         = $self->estimated_rows             if defined $self->estimated_rows;
    $reply->{ 'estimated_startup_cost' } = 0 + $self->estimated_startup_cost if defined $self->estimated_startup_cost;    # "0+" to remove .00 in case of integers
    $reply->{ 'estimated_total_cost' }   = 0 + $self->estimated_total_cost   if defined $self->estimated_total_cost;      # "0+" to remove .00 in case of integers
    $reply->{ 'actual_loops' }           = $self->actual_loops               if defined $self->actual_loops;
    $reply->{ 'actual_rows' }            = $self->actual_rows                if defined $self->actual_rows;
    $reply->{ 'actual_time_first' }      = 0 + $self->actual_time_first      if defined $self->actual_time_first;         # "0+" to remove .00 in case of integers
    $reply->{ 'actual_time_last' }       = 0 + $self->actual_time_last       if defined $self->actual_time_last;          # "0+" to remove .00 in case of integers
    $reply->{ 'type' }                   = $self->type                       if defined $self->type;
    $reply->{ 'scan_on' }                = clone( $self->scan_on )           if defined $self->scan_on;
    $reply->{ 'extra_info' }             = clone( $self->extra_info )        if defined $self->extra_info;

    $reply->{ 'sub_nodes' } = [ map { $_->get_struct } @{ $self->sub_nodes } ] if defined $self->sub_nodes;
    $reply->{ 'initplans' } = [ map { $_->get_struct } @{ $self->initplans } ] if defined $self->initplans;
    $reply->{ 'subplans'  } = [ map { $_->get_struct } @{ $self->subplans  } ] if defined $self->subplans ;

    return $reply;
}

=head1 AUTHOR

hubert depesz lubaczewski, C<< <depesz at depesz.com> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-postgresql-explain-node at rt.cpan.org>, or through
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

1;    # End of PostgreSQL::Explain::Node
