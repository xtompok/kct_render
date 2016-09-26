#!/usr/bin/perl


use common::sense;
use utf8;
use XML::Twig;
use Data::Dumper;
use DBI;
use DBD::Pg qw(:pg_types);
use Pg::hstore;


my $db = DBI->connect("dbi:Pg:dbname=cz_osm", "", "");

sub double_quote {
  my ($v) = @_;
  $v =~ s/\\/\\\\/g;
  $v =~ s/"/\\"/g;
  $v =~ s/'/''/g;
  "\"$v\"";
}


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
	return $relation;	
}

sub update_way_tags {
	my ($way_id,$key,$value) = @_;
	my $sth = $db->prepare("UPDATE ways SET tags = tags || '".double_quote($key)."=>".double_quote($value)."'::hstore WHERE osm_id = $way_id;");
	$sth->execute() or die $db->errstr;
}


sub mark_ways {
	my $sth = $db->prepare("SELECT osm_id FROM relations;"); 
	$sth->execute() or die $db->errstr;
	while (my @row = $sth->fetchrow_array()){
		my ($rel_id) = @row;
		print "Relation:".$rel_id;
		my $relation = get_relation($rel_id);
		my $tags = Pg::hstore::decode($relation->{tags});
		my $symbol = $tags->{"osmc:symbol"};
		print " symbol:".$symbol."\n";
		foreach my $way (@{$relation->{ways}}){
			update_way_tags($way->{osm_id},"symbol_$symbol","yes");	
		}
		#print Dumper($relation);
	}
}


sub merge_symbols {
	my $sth = $db->prepare("SELECT osm_id, tags FROM ways;");
	$sth->execute() or die $db->errstr;
	while (my @row = $sth->fetchrow_array()){
		my ($way_id,$tags) = @row;
		my $tags = Pg::hstore::decode($tags);
		my $kct = "";
		$kct .="r" if $tags->{"symbol_red:white:red_bar"};
		$kct .="g" if $tags->{"symbol_green:white:green_bar"};
		$kct .="b" if $tags->{"symbol_blue:white:blue_bar"};
		$kct .="y" if $tags->{"symbol_yellow:white:yellow_bar"};

		update_way_tags($way_id,"kct_symbols",$kct);
	}
}

mark_ways();
merge_symbols();
