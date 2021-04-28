#!/usr/bin/env perl
#
# This script outputs information about running Zeek processes in influxdb line format. If the Zeek processes have been started
# using Zeekctl, it will also add information like the listening interface and the type of node.
#
# Example output:
# zeek,host=allison,interface='unknown',nodetype='logger-1' pmem=0.0,pcpu=87.4,vsize=3271596,rss=197424,cputimes=153,etimes=175
# zeek,host=allison,interface='unknown',nodetype='manager' pmem=0.0,pcpu=0.5,vsize=686600,rss=89964,cputimes=0,etimes=174
# zeek,host=allison,interface='unknown',nodetype='proxy-1' pmem=0.0,pcpu=0.4,vsize=684792,rss=85476,cputimes=0,etimes=173
# zeek,host=allison,interface='af_packet::em2',nodetype='worker-1-1' pmem=0.0,pcpu=17.3,vsize=875652,rss=278452,cputimes=29,etimes=171
# zeek,host=allison,interface='af_packet::em2',nodetype='worker-1-3' pmem=0.0,pcpu=23.2,vsize=875360,rss=278308,cputimes=39,etimes=171
# zeek,host=allison,interface='af_packet::em2',nodetype='worker-1-11' pmem=0.0,pcpu=16.8,vsize=871848,rss=276724,cputimes=28,etimes=171

use strict;
use warnings;
use 5.26.0;
use Carp;
use autodie;
use Data::Dumper;

open my $cmd, '-|', 'ps -Czeek -o pmem,pcpu,vsize,rss,pid,cputimes,etimes,args --no-headers';
while (my $line = <$cmd>) {
	chomp($line);
	$line =~ s/^\s+//;
	my ($pmem, $pcpu, $vsize, $rss, $pid, $cputimes, $etimes) = split(/\s+/, $line);
	my $worker = "unknown";
	if ( $line =~ m#-p ([^ ]*?)(?= local\.zeek)# ) {
		$worker = $1;
	}
	my $interface="unknown";
	if ( $line =~ m#-i ([^ ]*?) # ) {
		$interface = $1;
	}
	my $out="zeek,host=allison,interface='$interface',nodetype='$worker' pmem=$pmem,pcpu=$pcpu,vsize=$vsize,rss=$rss,cputimes=$cputimes,etimes=$etimes";
	say $out;
}
