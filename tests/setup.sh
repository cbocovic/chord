#!/bin/bash

for i in {0..2}; do
	var=$((8888+$i*15))
	echo $var
	screen -dmS "tests$i" tests -num 15 -start $var
done
