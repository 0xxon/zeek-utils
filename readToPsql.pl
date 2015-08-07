#!/usr/bin/env perl

# Questions, comments -> johanna@icir.org

use 5.10.1;
use strict;
use warnings;
use autodie;

use DBI;
use DBD::Pg qw/:pg_types/;
use Getopt::Long;
use Carp;

use Data::Dumper;

my $dbname;
my $port;
my $username;
my $password;
my $host;
my $tablename;
my $createstatement = 0;
my $nocreate = 0;
my $copystatement;
my $headerfile;
my $outfile;

GetOptions(
	"db=s" => \$dbname,
	"port=i" => \$port,
	"username=s" => \$username,
	"password=s" => \$password,
	"host=s" => \$host,
	"tablename=s" => \$tablename,
	"createstatement" => \$createstatement,
	"nocreate" => \$nocreate,
	"copystatement=s" => \$copystatement, # expert option - provide own copy statement
	"headerfile=s" => \$headerfile, # expert option - read header from different file
	"outfile=s" => \$outfile
);

unless (defined($dbname)) {
	say STDERR "Please specify db name with --db=[name]";
	say STDERR "Options:";
	say STDERR "--db=[database name] (mandantory)";
	say STDERR "--host=[hostname]";
	say STDERR "--port=[database port]";
	say STDERR "--username=[username]";
	say STDERR "--password=[password]";
	say STDERR "--tablename=[table name]";
	say STDERR "--createstatement (only shows create table statement)";
	say STDERR "--nocreate (do not emit create table statement)";
	say STDERR "--outfile=[filename] (dump to file instead of db, implies nocreate. Still needs db connection.)";
	exit(-1);
}

my $outfh;
if ( defined($outfile) ) {
	$nocreate = 1;
	open($outfh, ">", $outfile);
}

my $connectString = "dbi:Pg:dbname=$dbname";
$connectString .= ";port=$port" if defined($port);
$connectString .= ";username=$port" if defined($username);
$connectString .= ";password=$port" if defined($password);
$connectString .= ";host=$port" if defined($host);

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

for my $file ( @ARGV ) {
	say "Reading $file";
	croak("File $file does not exist") unless ( -f $file );
	next if ( -s $file == 0 );

	# black magic from the internet... make all .gz and .bz2 arguments go through gzcat.
	$file =~ s{
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
	}{zcat '$1' |}xs;

	$file =~ s{
	    ^            # make sure to get whole filename
	    (
	      [^'] +     # at least one non-quote
	      \.         # extension dot
	      (?:        # now either suffix
		  bz2
	       )
	    )
	    \z           # through the end
	}{bzcat '$1' |}xs;

	open(my $fh, $file);
	my $headerfh;
	if ( defined($headerfile) ) {
		open($headerfh, "<", $headerfile);
	}
	$headerfh //= $fh;

	# -------------------------------- Step 1 - parse header lines


	my $fields_string;
	my $types_string;
	my $path;
	# this only works if the last header line is #types...
	while ( my $headerline = <$headerfh> ) {
		chomp($headerline);
		croak("Unexpected non-headerline: $headerline") unless ($headerline =~ m/^#/);
		if ($headerline =~ s/^#path\s//) {
			$path = $headerline;
			next;
		}
		if ($headerline =~ s/^#fields\s//) {
			$fields_string = $headerline;
			next;
		}
		if ($headerline =~ s/^#types\s//) {
			$types_string = $headerline;
			last;
		}
	}

	croak("No #types line") unless defined($types_string);
	croak("No #fields line") unless defined($fields_string);
	croak("No #path line") unless defined($path);

	$path = $tablename if ( defined($tablename) );

	my @fields = split(/\s/, $fields_string);
	my @types = split(/\s/, $types_string);

	my %f;
	@f{@fields} = @types;


	# -------------------------------- Step 2 - create table

	my $create = "CREATE TABLE IF NOT EXISTS $path (\n";
	$create .= "id SERIAL UNIQUE NOT NULL PRIMARY KEY";

	for my $field ( @fields ) {
		my $type = $f{$field};

		$field =~ tr/./_/; # manipulation is SAVED TO ARRAY! which we want.
		$field = 'from_addr' if $field eq 'from';
		$field = 'to_addr' if $field eq 'to';


		$create .= ",\n";
		$create .= "$field ";

		if ( $type =~ s#^(table|vector|set)\[(.*)\]#$2# ) {
			carp("internal error") unless(defined($typemap{$type}));
			$create .= $typemap{$type}."[]";
		} else {
			carp("internal error") unless(defined($typemap{$type}));
			$create .= $typemap{$type};
		}
	}
	$create .= "\n);";

	if ( $createstatement ) {
		say $create;
		exit(0);
	}

	$dbh->do($create) unless ( $nocreate );

	my $neednum = scalar @fields;


	# -------------------------------- Step 2 - build copy statement

	my $insert = "copy $path (".join(',', @fields).") FROM STDIN;";
	$insert = $copystatement if ( defined($copystatement) );
	$dbh->do($insert) unless(defined($outfile));

	while ( my $line = <$fh> ) {
		chomp($line);
		next if ( $line =~ m/^#close\s.*/ );
		next if ( $line =~ m/^#.*\s.*/ );

		my @values = split('\t', $line);
		if ( scalar @values != $neednum ) {
			say "Column with wrong number of entries";
			say $line;
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

		$dbh->pg_putcopydata(join("\t", @out)."\n") unless(defined($outfile));
		say $outfh join("\t", @out) if (defined($outfile));
	}

	close($fh);
	close($outfh) if (defined($outfile));

	$dbh->pg_putcopyend() unless(defined($outfile));

}
