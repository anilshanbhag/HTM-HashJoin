#!/bin/bash

HTMHOME=/home/anil/htm
MCHOME=/home/anil/htm/mc
HTM=$HTMHOME/main
MC=$MCHOME/src/mchashjoins

# Run MC
for i in `seq 0 27`
do
  pl $MC --nthreads=8 --r-size=$((2**27)) --s-size=2 --algo=PRO -local-shuffle-range=$((2**i))
done

# Run JustHashBuild 
for i in `seq 0 27`
do
  pl $HTM --algo nocc --rSize $((2**27)) --probeLength 4 --dataDistr local_shuffle --shuffleRange $((2**i))
done

# Run AtomicHashBuild
for i in `seq 0 27`
do
  pl $HTM --algo atomic --rSize $((2**27)) --probeLength 4 --dataDistr local_shuffle --shuffleRange $((2**i))
done

