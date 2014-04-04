#!/usr/bin/env perl

# Questions -> johanna@icir.org

use 5.10.1;
use strict;
use warnings;

use DBI;
use DBD::Pg qw/:pg_types/;
use Getopt::Long;

use Data::Dumper;

my $dbname;
my $port;
my $username;
my $password;
my $host;

GetOptions(
	"db=s" => \$dbname,
	"port=i" => \$port,
	"username=s" => \$username,
	"password=s" => \$password,
	"host=s" => \$host,
);

unless (defined($dbname)) {
	say STDERR "Please specify db name with --db=[name]";
	say STDERR "Options:";
	say STDERR "--db [database name] (mandantory)";
	say STDERR "--host [hostname]";
	say STDERR "--port [database port]";
	say STDERR "--username [username]";
	say STDERR "--password [password]";
	exit(-1);
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

	# -------------------------------- Step 1 - parse header lines

	for ( 1..4 ) {
		# throw away unneeded header lines
		<$fh>;
	}

	my $path = <$fh>;
	chomp($path);
	croak("Problem parsing header - #path") unless($path =~ s/#path\s//);

	my $fields_string = <$fh>;
	$fields_string = <$fh> if ($fields_string =~ m/^#open\s.*/);
	chomp($fields_string);
	croak("Problem parsing header - #fields") unless ($fields_string =~ s/#fields\s//);

	my @fields = split(/\s/, $fields_string);

	my $types_string = <$fh>;
	chomp($types_string);
	croak("Problem parsing header - #types") unless ($types_string =~ s/#types\s//);

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

	#say $create;

	$dbh->do($create);

	my $neednum = scalar @fields;


	# -------------------------------- Step 2 - build copy statement

	my $insert = "copy $path (".join(',', @fields).") FROM STDIN;";
	$dbh->do($insert);

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

		$dbh->pg_putcopydata(join("\t", @out)."\n");
	}

	close($fh);

	$dbh->pg_putcopyend();

}
