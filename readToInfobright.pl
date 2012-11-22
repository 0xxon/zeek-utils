#!/usr/bin/env perl

# Questions -> johanna@icir.org

use 5.10.1;
use strict;
use warnings;

use DBI;

use Carp::Assert;
use Data::Dumper;

my $connectString = "DBI:mysql:database=intnet;mysql_socket=/tmp/mysql-ib.sock";

my $dbh = DBI->connect($connectString, "root", "", {
}) or die("Could not connect");

my %typemap = (
	bool => "boolean",
	int => "bigint",
	count => "bigint",
	counter => "bigint",
	port => "integer",
	subnet => "varchar(50)",
	addr => "varchar(50)",
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


my $first = 1;
for my $field ( @fields ) {
	my $type = $f{$field};

	$field =~ tr/./_/; # manipulation is SAVED TO ARRAY! which we want.

	unless ( $first == 1 ) {
		$create .= ",\n";
	}
	$first = 0;
	$create .= "$field ";

	if ( $type =~ s#^(table|vector)\[(.*)\]#$2# ) {
		assert(defined($typemap{$type}));
		$create .= "text";
	} else {
		assert(defined($typemap{$type}));
		$create .= $typemap{$type};
	}
}
$create .= "\n);";

say $create;


$dbh->do($create);
$dbh->do("set \@bh_dataformat = 'txt_variable';");

# copy all remaining data to temporary file...

open(my $fh, '>', "/xc/ssl/tmpimport");
while ( my $line = <> ) {
	chomp($line);
	my @fields = split(/\t/, $line);
	for my $field ( @fields ) {
		if ( length($field) > 65500 ) {
			$field = "Field too long";
		}

		if ( $field eq "-" ) {
			$field = '';
		}
	}

	say $fh join("\t", @fields);
}
close($fh);


# -------------------------------- Step 2 - build copy statement 

my $insert = "LOAD DATA INFILE '/xc/ssl/tmpimport' INTO TABLE $path FIELDS TERMINATED BY '\\t' ENCLOSED BY 'NULL' ESCAPED BY '\\\\';";
$dbh->do($insert) or die("?");

unlink( '/xc/ssl/tmpimport' );

say "Done";
