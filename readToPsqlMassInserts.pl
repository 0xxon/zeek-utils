#!/usr/bin/env perl

# Questions -> johanna@icir.org

use 5.10.1;
use strict;
use warnings;

use DBI;
use DBD::Pg qw/:pg_types/;

use Carp::Assert;
use Data::Dumper;

my $connectString = "dbi:Pg:dbname=intnet";

my $dbh = DBI->connect($connectString, "", "", {
	RaiseError => 1,
	AutoCommit => 0,
});

my %typemap = (
	bool => "boolean",
	int => "bigint",
	count => "bigint",
	counter => "bigint",
	port => "integer",
	subnet => "inet",
	addr => "inet",
	time => "double precision",
	interval => "double precision",
	double => "double precision",
	enum => "text",
	string => "text",
	file => "text",
	func => "text",
);

# black magic from the internet... make all .gz and .bz2 arguments go through gzcat.
s{ 
    ^            # make sure to get whole filename
    ( 
      [^'] +     # at least one non-quote
      \.         # extension dot
      (?:        # now either suffix
	  gz
	| Z 
       )
    )
    \z           # through the end
}{zcat '$1' |}xs for @ARGV;

s{ 
    ^            # make sure to get whole filename
    ( 
      [^'] +     # at least one non-quote
      \.         # extension dot
      (?:        # now either suffix
	  bz2 
       )
    )
    \z           # through the end
}{bzcat '$1' |}xs for @ARGV;


# -------------------------------- Step 1 - parse header lines

for ( 1..4 ) {
	# throw away unneeded header lines
	<>;
}

my $path = <>;
chomp($path);
assert($path =~ s/#path\s//);

my $fields_string = <>;
chomp($fields_string);
assert($fields_string =~ s/#fields\s//);

my @fields = split(/\s/, $fields_string);

my $types_string = <>;
chomp($types_string);
assert($types_string =~ s/#types\s//);

my @types = split(/\s/, $types_string);

my %f;
@f{@fields} = @types;


# -------------------------------- Step 2 - create table

my $create = "CREATE TABLE IF NOT EXISTS $path (\n";
$create .= "id SERIAL UNIQUE NOT NULL PRIMARY KEY";

for my $field ( @fields ) {
	my $type = $f{$field};

	$field =~ tr/./_/; # manipulation is SAVED TO ARRAY! which we want.

	$create .= ",\n";
	$create .= "$field ";

	if ( $type =~ s#^(table|vector)\[(.*)\]#$2# ) {
		assert(defined($typemap{$type}));
		$create .= $typemap{$type}."[]";
	} else {
		assert(defined($typemap{$type}));
		$create .= $typemap{$type};
	}
}
$create .= "\n);";

say $create;

$dbh->do($create);

$dbh->do("BEGIN;");


# -------------------------------- Step 2 - build insert statement that will be reused for eternity

my $insert = "INSERT INTO $path (";
$insert .= join(',', @fields).") VALUES (";
for ( 1..scalar(@fields)) {
	$insert .= "?,";
}
chop($insert); # last ,
$insert .= ");";

my $sth = $dbh->prepare($insert) or die ("Prepare $insert failed");

while ( my $line = <> ) {
	chomp($line);


	my @values = split('\t', $line);
	#say Dumper(\@values);

	my $pos = 1;
	for my $val ( @values ) {
		if ( $val eq "-" ) {
			$sth->bind_param($pos, undef);
		} elsif ( $val eq "(empty)" ) {
			$sth->bind_param($pos, "{}");
		} else {
			if ( $types[$pos-1] =~ m#\[# ) {
				my @parts = split(',', $val);
				my @parts2 = map { '{'.$_.'}' } @parts;
				$sth->bind_param($pos, '{'.join(',', @parts2).'}');
			} else {
				$sth->bind_param($pos, $val);
			}
		} 

		$pos++;

	}

	$sth->execute();

}
$dbh->do("COMMIT;");

say "Done";
