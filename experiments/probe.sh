#!/bin/bash

HTMHOME=/home/anil/htm
MCHOME=/home/anil/htm/mc
HTM=$HTMHOME/adaptiveWithProbe

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

# Run HTMHashBuild
for i in `seq 0 27`
do
  pl $HTM --algo htm --transactionSize 16 --rSize $((2**27)) --probeLength 4 --dataDistr local_shuffle --shuffleRange $((2**i))
done


