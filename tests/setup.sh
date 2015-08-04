#!/bin/bash

for i in {0..4}; do
	var=$((8888+$i*2))
	echo $var
	screen -dmS "tests$i" tests -num 2 -start $var
	sleep 2
done
