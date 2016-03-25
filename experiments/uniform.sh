#!/bin/bash

HTMHOME=/home/anil/htm
MCHOME=/home/anil/htm/mc
HTM=$HTMHOME/main
MC=$MCHOME/src/mchashjoins

# Run MC
for i in `seq 18 2 28`
do
  pl $MC --nthreads=8 --r-size=$((2**i)) --s-size=2 --algo=PRO --non-unique
done

# Run JustHashBuild 
for i in `seq 18 2 28`
do
  pl $HTM --algo nocc --rSize $((2**i)) --probeLength 4 --dataDistr uniform
done

# Run AtomicHashBuild
for i in `seq 18 2 28`
do
  pl $HTM --algo atomic --rSize $((2**i)) --probeLength 4 --dataDistr uniform 
done

# Run HTMHashBuild 
for i in `seq 18 2 28`
do
  pl $HTM --algo htm --rSize $((2**i)) --transactionSize 16 --probeLength 4 --dataDistr uniform 
done