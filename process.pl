#!/usr/bin/perl


use common::sense;
use utf8;
use XML::Twig;
use Data::Dumper;
use DBI;
use DBD::Pg qw(:pg_types);


my $db = DBI->connect("dbi:Pg:dbname=cz_osm", "", "");


sub get_relation {
	my ($id) = @_;
	my $sth = $db->prepare("SELECT * FROM relations WHERE osm_id = $id"); 
	$sth->execute() or die $db->errstr;
	my $relation = $sth->fetchrow_hashref();

	my @ways;
	my $sth = $db->prepare("SELECT way_id FROM ways_relations WHERE relation_id = $id"); 
	$sth->execute() or die $db->errstr;
	while (my @row = $sth->fetchrow_array()){
		my ($way_id) = @row;

		my $sth = $db->prepare("SELECT osm_id, tags, first FROM ways WHERE osm_id = $way_id"); 
		$sth->execute() or die $db->errstr;
		my $way = $sth->fetchrow_hashref();
		if ($way){
			push @ways, $way;
		} else {
			print "Way $way_id not found\n";
		}
	}

	foreach my $way (@ways) {
		my $way_id = $way->{osm_id};
			
		my $sth = $db->prepare("SELECT node_id FROM nodes_ways WHERE way_id = $way_id"); 
		$sth->execute() or die $db->errstr;
		$way->{nodes} = $sth->fetchall_arrayref();
	}

	$relation->{ways} = \@ways;
	
}



my $sth = $db->prepare("SELECT osm_id FROM relations;"); 
$sth->execute() or die $db->errstr;
while (my @row = $sth->fetchrow_array()){
	my ($rel_id) = @row;
	print "Relation:".$rel_id."\n";
	my $relation = get_relation($rel_id);
	#print Dumper($relation);
}
