#!/usr/bin/env perl
# Convert abuse.ch SSL Fingerprint Blacklist in CVS format to format ingestible by
# the Zeek intelligence framework.
#
# Get the blacklist at https://sslbl.abuse.ch/blacklist/, get Zeek at https://www.zeek.org
#
# Usage:
# ./convert-blacklist.pl infile > outfile

use 5.12.0;
use strict;
#use warnings; no warnings - they trigger incorrectly here

use Carp;

say join("\t", qw/#fields indicator indicator_type meta.source meta.desc meta.url/);
while ( my $line = <> ) {
  chomp($line);
  next if ( $line =~ m@^#@ );
  my ( $ts, $hash, $reason) = split(/,/, $line);
  croak("Invalid line: $line") unless ( defined($ts) && defined($hash) && defined($reason) );
  say join("\t", ($hash, "Intel::FILE_HASH", "abuse.ch SSLBL", $reason, "https://sslbl.abuse.ch/blacklist/"));
}
