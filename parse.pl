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
$db->{AutoCommit}=0;
#my $insert_node = $db->prepare_cached('INSERT INTO nodes VALUES(?,?,?,?)');
#my $insert_way = $db->prepare_cached('INSERT INTO ways VALUES(?,?,?)');
#my $insert_relation = $db->prepare_cached('INSERT INTO relations VALUES(?,?)');
#my $insert_node_way = $db->prepare_cached('INSERT INTO nodes_ways VALUES(?,?,?)');
#my $insert_node_relation = $db->prepare_cached('INSERT INTO nodes_relations VALUES(?,?,?)');
#my $insert_way_relation = $db->prepare_cached('INSERT INTO ways_relations VALUES(?,?,?)');

sub batch_insert {
	my ($table,$nodes) = @_;
	return if (@{$nodes} == 0);
	my $query = "INSERT INTO $table VALUES";
	foreach (@{$nodes}){
		$query .= "(" . join(',',@$_). "),";
	}
	chop($query);
#	print ">>>>";
#	print $query;
#	print "<<<<<";
	if ($db->do($query) == 0){
		print $query;
		die $db->errstr;
	}
}

my $sumnodes = 0;
my $sumways = 0;
my $sumrelations = 0;

sub double_quote {
  my ($v) = @_;
  $v =~ s/\\/\\\\/g;
  $v =~ s/"/\\"/g;
  $v =~ s/'/''/g;
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
  chomp($s);
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

my @nodes;
sub proc_node {
	my ($twig, $ele) = @_;
	my $tags = get_tags($ele);
	my $hstore = hash_to_hstore($tags);
	push(@nodes, [$ele->att('id'),$ele->att('lat'),$ele->att('lon'),"'".hash_to_hstore($tags)."'"]);
	$ele->purge();
	$sumnodes ++;
	if (@nodes % 1000 == 0){
		batch_insert("nodes",\@nodes);
		print $sumnodes." nodes processed\n";
		@nodes = ();
	
	}
	$db->commit	if ( $sumnodes % 100000 == 0);
	1;
}

my @ways;
my @nodes_ways;
sub proc_way {
	my ($twig, $ele) = @_;
	my $tags = get_tags($ele);
	my $index = 0;
	my $id = $ele->att('id');
	my $child = $ele->first_child("nd");
	my $first = $child->att("ref");
	for ($ele->children("nd")){
				push @nodes_ways, [$_->att("ref"),$id,$index];
				$index+=1;
    }
	push(@ways, [$id,"'".hash_to_hstore($tags)."'",$first]);
	$sumways++;
	$ele->purge();
	if ( $sumways % 1000 == 0){
		batch_insert("ways",\@ways);
		batch_insert("nodes_ways",\@nodes_ways);
		print "$sumways ways processed\n";
		@ways = ();
		@nodes_ways = ();
	}
	$db->commit	if ( $sumways % 100000 == 0);
	1;
}

my @relations;
my @nodes_relations;
my @ways_relations;
sub proc_relation {
	my ($twig, $ele) = @_;
	my $tags = get_tags($ele);
	my $id = $ele->att('id');
	for ($ele->children("member")) {
                my $mid = $_->att("ref");
				my $type = $_->att("type");
				my $role = $_->att("role");
				if ($role eq ""){ 
					$role = "NULL";
				} else {
					$role = "'".$role."'";
				}
				if ($type eq "node"){
					push(@nodes_relations,[$mid,$id,$role]);
				}elsif ($type eq "way"){
					push(@ways_relations,[$mid,$id,$role]);
				}
    }
	push(@relations,[$id,"'".hash_to_hstore($tags)."'"]);

	$ele->purge();
	$sumrelations++;
	if ($sumrelations % 1000 == 0){
		batch_insert("relations",\@relations);
		batch_insert("nodes_relations",\@nodes_relations);
		batch_insert("ways_relations",\@ways_relations);
		print "$sumrelations relations processed\n";
		@relations = ();
		@nodes_relations = ();
		@ways_relations = ();
	}
	$db->commit	if ( $sumrelations % 100000 == 0);
	1;
}


my $roots = {node => \&proc_node, way => \&proc_way, relation => \&proc_relation};
my $twig = XML::Twig->new( twig_roots => $roots);

create_tables($db);

print "Parsing $ARGV[0]...\n";
$twig->parsefile($ARGV[0]);

batch_insert("nodes",\@nodes);
batch_insert("ways",\@ways);
batch_insert("nodes_ways",\@nodes_ways);
batch_insert("relations",\@relations);
batch_insert("nodes_relations",\@nodes_relations);
batch_insert("ways_relations",\@ways_relations);

$db->commit;

$db->disconnect;

