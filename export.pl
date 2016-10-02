#!/usr/bin/perl


use common::sense;
use utf8;
use XML::Twig;
use Data::Dumper;
use DBI;
use DBD::Pg qw(:pg_types);
use Pg::hstore;
use Text::CSV;


my $db = DBI->connect("dbi:Pg:dbname=cz_osm", "", "");

my $sth = $db->prepare("SELECT osm_id,tags->'kct_symbols', tags->'orientation' FROM ways WHERE tags->'kct_symbols' <>  '';");
$sth->execute() or die $db->errstr;
my $ways = $sth->fetchall_arrayref();

$\="\n";
my $csv = Text::CSV->new({binary=>1});
open(my $fh, ">:encoding(utf8)", "ways.csv");

$csv->print($fh,["osm_id","kct_symbols","orientation"]);
$csv->print($fh,$_) for @{$ways};
close $fh;



