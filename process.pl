#!/usr/bin/perl


use common::sense;
use utf8;
use XML::Twig;
use Data::Dumper;
use DBI;
use DBD::Pg qw(:pg_types);
use Pg::hstore;


my $db = DBI->connect("dbi:Pg:dbname=cz_osm", "", "");
$db->{AutoCommit}=0;

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

	my %ways;
	my $sth = $db->prepare("SELECT way_id FROM ways_relations WHERE relation_id = $id"); 
	$sth->execute() or die $db->errstr;
	while (my @row = $sth->fetchrow_array()){
		my ($way_id) = @row;

		my $sth = $db->prepare("SELECT osm_id, tags, first FROM ways WHERE osm_id = $way_id"); 
		$sth->execute() or die $db->errstr;
		my $way = $sth->fetchrow_hashref();
		if ($way){
			$ways{$way->{osm_id}}=$way;
		} else {
			print "Way $way_id not found\n";
		}
	}

	foreach my $way (values(%ways)) {
		my $way_id = $way->{osm_id};
			
		my $sth = $db->prepare("SELECT node_id FROM nodes_ways WHERE way_id = $way_id ORDER BY index"); 
		$sth->execute() or die $db->errstr;
		$way->{nodes} = [];
		while (my @row =  $sth->fetchrow_array()){
			push @{$way->{nodes}}, $row[0];
		}
	}

	$relation->{ways} = \%ways;
	return $relation;	
}

#FIXME: SQL injection prevention
sub update_way_tags {
	my ($way_id,$key,$value) = @_;
	die "Not given way id" if not $way_id;
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
		foreach my $way (values(%{$relation->{ways}})){
			update_way_tags($way->{osm_id},"symbol_$symbol","yes");	
		}
		#print Dumper($relation);
	}
}


sub count_nodes {
	my ($ways) = @_;
	my %nodes;
	foreach my $way (@{$ways}){
		foreach my $node (@{$way->{nodes}}){
			if ($nodes{$node}){
				$nodes{$node}+=2;	
			} else {
				$nodes{$node} = 2;
			}
		}
		$nodes{$way->{nodes}->[0]}--;
		$nodes{$way->{nodes}->[-1]}--;
	}
	return \%nodes;	
}

sub ways_neigh {
	my ($waysref) = @_;
	my @ways = @{$waysref};
	my %nodeways;
	foreach my $way (@ways){
		foreach my $node (@{$way->{nodes}}){
			if ($nodeways{$node}){
				push @{$nodeways{$node}},$way;
			}else{
				$nodeways{$node} = [$way];
			}
		}	
	}	

	my %neighbors;
	foreach my $way (@ways){
		$neighbors{$way->{osm_id}}=[];
	}
	foreach my $way (@ways){
		foreach my $node (@{$way->{nodes}}){
			foreach my $nextway (@{$nodeways{$node}}){
				push $neighbors{$way->{osm_id}}, $nextway if $nextway->{osm_id} != $way->{osm_id};	
			}
		}	
	}
#	while (my ($way,$nexts) = each %neighbors){
#		print "Neighs:".$way.":";
#		foreach my $next (@{$nexts}){
#			print $next->{osm_id}.",";	
#		}
#		print "\n";
#	}
	return \%neighbors;
}

sub orient_way {
	my ($way,$from_way) = @_;
	return if $way->{orientation};
	#print "Orient way ".$way->{osm_id}.":".$way->{nodes}->[0]." ".$way->{nodes}->[-1]."\n";
	#print "From way ".$from_way->{osm_id}.":".$from_way->{nodes}->[0]." ".$from_way->{nodes}->[-1]." ".$from_way->{orientation}."\n";
	# First way in search
	if (not $from_way){
		update_way_tags($way->{osm_id},"orientation","forward");
		$way->{orientation} = "forward";
		update_way_tags($way->{osm_id},"first","yes");
		return;
	}

	if (($from_way->{nodes}->[0] == $way->{nodes}->[0]) or ($from_way->{nodes}->[-1] == $way->{nodes}->[-1])){
			if ($from_way->{orientation} eq "forward"){
				update_way_tags($way->{osm_id},"orientation","backward");
				$way->{orientation} = "backward";
			} else {
				update_way_tags($way->{osm_id},"orientation","forward");
				$way->{orientation} = "forward";	
			}
	
	} elsif (($from_way->{nodes}->[0] == $way->{nodes}->[-1]) or ($from_way->{nodes}->[-1] == $way->{nodes}->[0])){
	
			if ($from_way->{orientation} eq "forward"){
				update_way_tags($way->{osm_id},"orientation","forward");
				$way->{orientation} = "forward";
			} else {
				update_way_tags($way->{osm_id},"orientation","backward");
				$way->{orientation} = "backward";	
			}
	}else{
		update_way_tags($way->{osm_id},"orientation","forward");
		$way->{orientation} = "forward";
		
	}
	#print "Oriented: ".$way->{orientation}."\n"
	#print "\n";

}

sub get_neighbors {
	# {vrchol (id) = [[way(id),vrchol(id)],...],...	}
	my %neighbors;
	my @ways = @{$_[0]};
	foreach my $way (@ways){
		foreach my $node (@{$way->{nodes}}){
			$neighbors{$node} = [];	
		}
	}
	foreach my $way (@ways){
		my @nodes = @{$way->{nodes}};
		for (my $idx=0;$idx < @nodes-1;$idx++){
			push $neighbors{$nodes[$idx]}, [$way->{osm_id},$nodes[$idx+1]];
			push $neighbors{$nodes[$idx+1]}, [$way->{osm_id},$nodes[$idx]];
		}
	}
	return \%neighbors;
	
}

sub orient_relation {
	my ($relation) = @_;
	my $tags = Pg::hstore::decode($relation->{tags});
	my @ways = values $relation->{ways};
	#my $nneighbors = get_neighbors(\@ways);
	my $wneighbors = ways_neigh(\@ways);

	my %visited;
	my %unoriented;

	foreach my $way (@ways){
		$unoriented{$way->{osm_id}}=1;	
	}
	while (keys %unoriented){
		my $cur;
		$cur = $relation->{ways}->{(keys %unoriented)[0]};
		#print $cur;
		
		my @buffer;

		push @buffer, [$cur,undef];



		while (@buffer) {
			my $item = pop @buffer;
			#print Dumper($item);
			my $way = $item->[0];
			my $from_way = $item->[1];

		#	print("At:".$way->{osm_id}." ".$node." ".$from_way->{osm_id}."\n");
			orient_way($way,$from_way);
			delete $unoriented{$way->{osm_id}};
			$visited{$way->{osm_id}} = 1;


			foreach my $neigh (@{$wneighbors->{$way->{osm_id}}}){
				push @buffer, [$neigh,$way] if not $visited{$neigh->{osm_id}};	
			}
		}
	}
	$db->commit;



#	print Dumper($neighbors);
		
}

sub orient_ways {
	my $sth = $db->prepare("SELECT osm_id FROM relations;"); 
	$sth->execute() or die $db->errstr;
	while (my @row = $sth->fetchrow_array()){
		my ($rel_id) = @row;
		print "Relation:".$rel_id."\n";
		my $relation = get_relation($rel_id);
		orient_relation($relation);
	}
#	my $rel_id = 229808;
#	print "Relation:".$rel_id."\n";
#	my $relation = get_relation($rel_id);
#	orient_relation($relation);
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
orient_ways();
