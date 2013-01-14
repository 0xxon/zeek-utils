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
#	Autocommit => 1,
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

say Dumper(\@ARGV);

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
$fields_string = <> if ($fields_string =~ m/^#open\s.*/);
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

#say $create;

$dbh->do($create);

my $neednum = scalar @fields;


# -------------------------------- Step 2 - build copy statement 

my $insert = "copy $path (".join(',', @fields).") FROM STDIN;";
$dbh->do($insert);

while ( my $line = <> ) {
	chomp($line);
	next if ( $line =~ m/^#close\s.*/ );


	my @values = split('\t', $line);
	if ( scalar @values != $neednum ) {
		say "Column with wrong number of entries";
		last;
	}
	#say Dumper(\@values);
	my $str;

	my $pos=0;
	my @out;
       	for my $val (@values) {
		$val = $dbh->quote($val);
		$val = substr($val, 1, -1);
		if ( $val eq "-" ) { 
			push(@out, '\N'); 
		} elsif ( $val eq "(empty)" ) {
			push(@out, '{}');
		} else {
			if ( $types[$pos] =~ m#\[# ) {
				$val =~ s/"/\\\\\"/g;
				#die ($val) if $val =~ m#"#;
				my @parts = split(',', $val);
				my @parts2 = map { '{"'.$_.'"}' } @parts;
				push(@out, '{'.join(',', @parts2).'}');
			} else {
				push(@out, $val);
			}

		} 
		$pos++;
	}

	#say Dumper(\@out);

	$dbh->pg_putcopydata(join("\t", @out)."\n");


}

$dbh->pg_putcopyend();
say "Done";
