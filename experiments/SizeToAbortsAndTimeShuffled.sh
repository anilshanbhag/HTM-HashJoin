#!/bin/bash

HTMHOME=/home/anil/htm
MCHOME=/home/anil/htm/mc
HTM=$HTMHOME/main
MC=$MCHOME/src/mchashjoins

# Run HTM
for i in `seq 0 9`
do
  pl $HTM --algo htm --rSize $((2**27)) --transactionSize $((2**i)) --probeLength 4 --dataDistr shuffle
done


