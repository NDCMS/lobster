#!/bin/bash

echo arguments: $*

optional=
output=

while getopts a:o: opt; do
	case $opt in
		a)
			optional=$OPTARG
			;;
		o)
			output=$OPTARG
			;;
		?)
			exit 1
			;;
	esac
done

for i in $(seq $(($OPTIND-1))); do
	shift
done

echo "output file:   $output"
echo "optional file: $optional"
echo "input files:   $*"

if [ -n "$output" -a -n "$*" ]; then
	echo ">>> producing output"
	for i in $(seq 5); do
		cat $* >> $output
	done
fi

if [ -n "$optional" ]; then
	echo ">>> content of $optional:"
	cat $optional
fi
