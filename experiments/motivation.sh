#!/bin/bash

HTMHOME=/home/anil/htm
MCHOME=/home/anil/htm/mc
HTM=$HTMHOME/main
MC=$MCHOME/src/mchashjoins

# Run MC
for i in `seq 18 29`
do
  pl $MC --nthreads=8 --r-size=$((2**i)) --s-size=2 --algo=PRO -local-shuffle-range=1024
done

# Run JustHashBuild 
for i in `seq 18 29`
do
  pl $HTM --algo nocc --rSize $((2**i)) --probeLength 4 --dataDistr local_shuffle --shuffleRange 16
done

# Run AtomicHashBuild
for i in `seq 18 29`
do
  pl $HTM --algo atomic --rSize $((2**i)) --probeLength 4 --dataDistr local_shuffle --shuffleRange 16
done

