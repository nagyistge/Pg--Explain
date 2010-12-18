package Pg::Explain::Node;
use strict;
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
has 'never_executed'         => ( 'is' => 'rw', 'isa' => 'Bool' );

=head1 NAME

Pg::Explain::Node - Class representing single node from query plan

=head1 VERSION

Version 0.11

=cut

our $VERSION = '0.11';

=head1 SYNOPSIS

Quick summary of what the module does.

Perhaps a little code snippet.

    use Pg::Explain::Node;

    my $foo = Pg::Explain::Node->new();
    ...

=head1 FUNCTIONS

=head2 actual_loops

Returns number how many times current node has been executed.

This information is available only when parsing EXPLAIN ANALYZE output - not in EXPLAIN output.

=head2 actual_rows

Returns amount of rows current node returnes in single execution (i.e. if given node was executed 10 times, you have to multiply actual_rows by 10, to get full number of returned rows.

This information is available only when parsing EXPLAIN ANALYZE output - not in EXPLAIN output.

=head2 actual_time_first

Returns time (in miliseconds) how long it took PostgreSQL to return 1st row from given node.

This information is available only when parsing EXPLAIN ANALYZE output - not in EXPLAIN output.

=head2 actual_time_last

Returns time (in miliseconds) how long it took PostgreSQL to return all rows from given node. This number represents single execution of the node, so if given node was executed 10 times, you have to multiply actual_time_last by 10 to get total time of running of this node.

This information is available only when parsing EXPLAIN ANALYZE output - not in EXPLAIN output.

=head2 estimated_rows

Returns estimated number of rows to be returned from this node.

=head2 estimated_row_width

Returns estimated width (in bytes) of single row returned from this node.

=head2 estimated_startup_cost

Returns estimated cost of starting execution of given node. Some node types do not have startup cost (i.e., it is 0), but some do. For example - Seq Scan has startup cost = 0, but Sort node has
startup cost depending on number of rows.

This cost is measured in units of "single-page seq scan".

=head2 estimated_total_cost

Returns estimated full cost of given node. 

This cost is measured in units of "single-page seq scan".

=head2 type

Textual representation of type of current node. Some types for example:

=over

=item * Index Scan

=item * Index Scan Backward

=item * Limit

=item * Nested Loop

=item * Nested Loop Left Join

=item * Result

=item * Seq Scan

=item * Sort

=back

=head2 scan_on

Hashref with extra information in case of table scans.

For Seq Scan it contains always 'table_name' key, and optionally 'table_alias' key.

For Index Scan and Backward Index Scan, it also contains (always) 'index_name' key.

=head2 extra_info

ArrayRef of strings, each contains textual information (leading and tailing spaces removed) for given node.

This is not always filled, as it depends heavily on node type and PostgreSQL version.

=head2 sub_nodes

ArrayRef of Pg::Explain::Node objects, which represent sub nodes.

For more details, check ->add_sub_node method description.

=head2 initplans

ArrayRef of Pg::Explain::Node objects, which represent init plan.

For more details, check ->add_initplan method description.

=head2 subplans

ArrayRef of Pg::Explain::Node objects, which represent sub plan.

For more details, check ->add_subplan method description.

=head2 meta

Method provided by Moose. From it's perldoc:

 This is a method which provides access to the current class's metaclass.

=head2 BUILD

Moose-called function (from within constructor) which checks if provided node type is one of table scan types, and if so, extracts information from it to $self->scan_on structure.

=cut

sub BUILD {
    my $self = shift;
    if ( $self->type =~ m{ \A ( Seq \s Scan | Bitmap \s+ Heap \s+ Scan) \s on \s (\S+) (?: \s+ (\S+) ) ? \z }xms ) {
        $self->type( $1 );
        $self->scan_on( { 'table_name' => $2, } );
        $self->scan_on->{ 'table_alias' } = $3 if defined $3;
    }
    elsif ( $self->type =~ m{ \A ( Bitmap \s+ Index \s+ Scan) \s on \s (\S+) \z }xms ) {
        $self->type( $1 );
        $self->scan_on( { 'index_name' => $2, } );
    }
    elsif ( $self->type =~ m{ \A (Index \s Scan (?: \s Backward )? ) \s using \s (\S+) \s on \s (\S+) (?: \s+ (\S+) ) ? \z }xms ) {
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

Extra_info is used by some nodes to provide additional information. For example
- for Sort nodes, they usually contain informtion about used memory, used sort
method and keys.

=cut

sub add_extra_info {
    my $self = shift;
    if ( $self->extra_info ) {
        push @{ $self->extra_info }, @_;
    }
    else {
        $self->extra_info( [ @_ ] );
    }
    return;
}

=head2 add_subplan

Adds new subplan node.

It will be available at $node->subplans (returns arrayref)

Example of plan with subplan:

 # explain select *, (select oid::int4 from pg_class c2 where c2.relname = c.relname) - oid::int4 from pg_class c;
                                               QUERY PLAN
 ------------------------------------------------------------------------------------------------------
  Seq Scan on pg_class c  (cost=0.00..1885.60 rows=227 width=200)
    SubPlan
      ->  Index Scan using pg_class_relname_nsp_index on pg_class c2  (cost=0.00..8.27 rows=1 width=4)
            Index Cond: (relname = $0)
 (4 rows)


=cut

sub add_subplan {
    my $self = shift;
    if ( $self->subplans ) {
        push @{ $self->subplans }, @_;
    }
    else {
        $self->subplans( [ @_ ] );
    }
    return;
}

=head2 add_initplan

Adds new initplan node.

It will be available at $node->initplans (returns arrayref)

Example of plan with initplan:

 # explain analyze select 1 = (select 1);
                                          QUERY PLAN
 --------------------------------------------------------------------------------------------
  Result  (cost=0.01..0.02 rows=1 width=0) (actual time=0.033..0.035 rows=1 loops=1)
    InitPlan
      ->  Result  (cost=0.00..0.01 rows=1 width=0) (actual time=0.003..0.005 rows=1 loops=1)
  Total runtime: 0.234 ms
 (4 rows)

=cut

sub add_initplan {
    my $self = shift;
    if ( $self->initplans ) {
        push @{ $self->initplans }, @_;
    }
    else {
        $self->initplans( [ @_ ] );
    }
    return;
}

=head2 add_sub_node

Adds new sub node.

It will be available at $node->sub_nodes (returns arrayref)

Sub nodes are nodes that are used by given node as data sources.

For example - "Join" node, has 2 sources (sub_nodes), which are table scans (Seq Scan, Index Scan or Backward Index Scan) over some tables.

Example plan which contains subnode:

 # explain select * from test limit 1;
                           QUERY PLAN
 --------------------------------------------------------------
  Limit  (cost=0.00..0.01 rows=1 width=4)
    ->  Seq Scan on test  (cost=0.00..14.00 rows=1000 width=4)
 (2 rows)

Node 'Limit' has 1 sub_plan, which is "Seq Scan"

=cut

sub add_sub_node {
    my $self = shift;
    if ( $self->sub_nodes ) {
        push @{ $self->sub_nodes }, @_;
    }
    else {
        $self->sub_nodes( [ @_ ] );
    }
    return;
}

=head2 get_struct

Function which returns simple, not blessed, hashref with all information about given explain node and it's children.

This can be used for debug purposes, or as a base to print information to user.

Output looks like this:

 {
     'estimated_rows'         => '10000',
     'estimated_row_width'    => '148',
     'estimated_startup_cost' => '0',
     'estimated_total_cost'   => '333',
     'scan_on'                => { 'table_name' => 'tenk1', },
     'type'                   => 'Seq Scan',
 }

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
    $reply->{ 'is_analyzed' }            = $self->is_analyzed;

    $reply->{ 'sub_nodes' } = [ map { $_->get_struct } @{ $self->sub_nodes } ] if defined $self->sub_nodes;
    $reply->{ 'initplans' } = [ map { $_->get_struct } @{ $self->initplans } ] if defined $self->initplans;
    $reply->{ 'subplans' }  = [ map { $_->get_struct } @{ $self->subplans } ]  if defined $self->subplans;

    return $reply;
}

=head2 total_inclusive_time

Method for getting total node time, summarized with times of all subnodes, subplans and initplans - which is basically ->actual_loops * ->actual_time_last.

=cut

sub total_inclusive_time {
    my $self = shift;
    return unless $self->actual_loops;
    return unless defined $self->actual_time_last;
    return $self->actual_loops * $self->actual_time_last;
}

=head2 total_exclusive_time

Method for getting total node time, without times of subnodes - which amounts to time PostgreSQL spent running this paricular node.

=cut

sub total_exclusive_time {
    my $self = shift;

    my $time = $self->total_inclusive_time;
    return unless defined $time;

    for my $node ( map { @{ $_ } } grep { defined $_ } ( $self->sub_nodes ) ) {
        $time -= ( $node->total_inclusive_time || 0 );
    }

    for my $init ( map { @{ $_ } } grep { defined $_ } ( $self->initplans ) ) {
        $time -= ( $init->total_inclusive_time || 0 );
    }

    for my $plan ( map { @{ $_ } } grep { defined $_ } ( $self->subplans ) ) {
        $time -= ( $plan->total_inclusive_time || 0 );
    }

    # ignore negative times - these come from rounding errors on nodes with loops > 1.
    return 0 if $time < 0;

    return $time;
}

=head2 is_analyzed

Returns 1 if the explain node it represents was generated by EXPLAIN ANALYZE. 0 otherwise.

=cut

sub is_analyzed {
    my $self = shift;

    return defined $self->actual_loops || $self->never_executed ? 1 : 0;
}

=head1 AUTHOR

hubert depesz lubaczewski, C<< <depesz at depesz.com> >>

=head1 BUGS

Please report any bugs or feature requests to C<depesz at depesz.com>.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Pg::Explain::Node

=head1 COPYRIGHT & LICENSE

Copyright 2008 hubert depesz lubaczewski, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.


=cut

1;    # End of Pg::Explain::Node
