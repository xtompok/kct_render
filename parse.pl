#!/usr/bin/perl
#

use common::sense;
use utf8;
use XML::Twig;
use Data::Dumper;
use DBI;
use DBD::Pg qw(:pg_types);
#use Pg::hstore;

sub create_tables {
	my ($db) = @_;
	my $cmd = "DROP TABLE IF EXISTS nodes,ways,relations,nodes_ways,ways_relations,nodes_relations;";
	$db->do($cmd) or die $db->errstr;
	my $cmd = "CREATE TABLE nodes ( 
		osm_id BIGINT NOT NULL PRIMARY KEY, 
		lat DOUBLE PRECISION,
		lon DOUBLE PRECISION,
		tags hstore);";
	$db->do($cmd) or die $db->errstr;
	my $cmd = "CREATE TABLE ways ( osm_id BIGINT NOT NULL PRIMARY KEY, tags hstore, first BIGINT);";
	$db->do($cmd) or die $db->errstr;
	my $cmd = "CREATE TABLE relations ( osm_id BIGINT NOT NULL PRIMARY KEY, tags hstore);";
	$db->do($cmd) or die $db->errstr;
	my $cmd = "CREATE TABLE nodes_ways (
		node_id BIGINT /*REFERENCES nodes(osm_id)*/, 
		way_id BIGINT /*REFERENCES ways(osm_id)*/,
		index INT);";
	$db->do($cmd) or die $db->errstr;
	my $cmd = "CREATE TABLE nodes_relations ( 
		node_id BIGINT /*REFERENCES nodes(osm_id)*/, 
		relation_id BIGINT /*REFERENCES relations(osm_id)*/, 
		role VARCHAR(32));";
	$db->do($cmd) or die $db->errstr;
	my $cmd = "CREATE TABLE ways_relations ( 
		way_id BIGINT /*REFERENCES ways(osm_id)*/, 
		relation_id BIGINT /*REFERENCES ways(osm_id)*/, 
		role VARCHAR(32));";
	$db->do($cmd) or die $db->errstr;
	1;
}

my $db = DBI->connect("dbi:Pg:dbname=cz_osm_ogr", "", "");
my $insert_node = $db->prepare_cached('INSERT INTO nodes VALUES(?,?,?,?)');
my $insert_way = $db->prepare_cached('INSERT INTO ways VALUES(?,?,?)');
my $insert_relation = $db->prepare_cached('INSERT INTO relations VALUES(?,?)');
my $insert_node_way = $db->prepare_cached('INSERT INTO nodes_ways VALUES(?,?,?)');
my $insert_node_relation = $db->prepare_cached('INSERT INTO nodes_relations VALUES(?,?,?)');
my $insert_way_relation = $db->prepare_cached('INSERT INTO ways_relations VALUES(?,?,?)');

my $nodes = 0;
my $ways = 0;
my $relations = 0;

sub double_quote {
  my ($v) = @_;
  $v =~ s/"/\\"/g;
  "\"$v\"";
}

# Takes a hash ptr or hash, and returns an hstore string of the data
sub hash_to_hstore {
  my $h = $_[0]; # get hash ref or hash
  my $s = '';
  foreach my $k (sort keys %$h) {
    next unless $k; # Empty keys?
    $s .= double_quote($k)  . '=>';
    $s .= double_quote($h->{$k}) . ',';
  }
  $s =~ s/,$//;
  $s;
}

sub get_tags{
	my ($ele) = @_;
	my $tags={};
	my $child = $ele->first_child("tag");
	while (defined $child) {
                my ($key, $value) = ($child->att("k"), $child->att("v"));
				$tags->{$key} = $value;
#                printf("  %s=%s\n", $key, $value);
                $child = $child->next_sibling("tag");
    }
	$tags;
}

sub proc_node {
	my ($twig, $ele) = @_;
#	print "Node!\n";
	my $tags = get_tags($ele);
	$insert_node->execute($ele->att('id'),$ele->att('lat'),$ele->att('lon'),hash_to_hstore($tags));
	$ele->purge();
	$nodes++;
	print "$nodes nodes processed\n" if $nodes % 1000 == 0;
	1;
}

sub proc_way {
	my ($twig, $ele) = @_;
#	print "Way!\n";
	my $tags = get_tags($ele);
	my $index = 0;
	my $id = $ele->att('id');
	my $child = $ele->first_child("nd");
	my $first = $child->att("ref");
	while (defined $child) {
                my $nd = $child->att("ref");
				$insert_node_way->execute($nd,$id,$index);
				$index+=1;
                $child = $child->next_sibling("nd");
    }
	$insert_way->execute($id,hash_to_hstore($tags),$first);
	$ele->purge();
	$ways++;
	print "$ways ways processed\n" if $ways % 1000 == 0;
	1;
}
sub proc_relation {
	my ($twig, $ele) = @_;
#	print "Relation!\n";
	my $tags = get_tags($ele);
	my $id = $ele->att('id');
	my $child = $ele->first_child("member");
	while (defined $child) {
                my $mid = $child->att("ref");
				my $type = $child->att("type");
				my $role = $child->att("role");
				if ($type eq "node"){
					$insert_node_relation->execute($mid,$id,$role);
				}elsif ($type eq "way"){
					$insert_way_relation->execute($mid,$id,$role);
				}
                $child = $child->next_sibling("member");
    }
	$insert_relation->execute($id,hash_to_hstore($tags));

	$ele->purge();
	$relations++;
	print "$relations relations processed\n" if $relations % 1000 == 0;
	1;
}


my $roots = {node => \&proc_node, way => \&proc_way, relation => \&proc_relation};
my $twig = XML::Twig->new( twig_roots => $roots);

create_tables($db);

print "Parsing $ARGV[0]...\n";
$twig->parsefile($ARGV[0]);

$db->disconnect;

