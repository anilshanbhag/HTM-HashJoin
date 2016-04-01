#!/bin/bash

HTMHOME=/home/anil/htm
MCHOME=/home/anil/htm/mc
HTM=$HTMHOME/noretry
MC=$MCHOME/src/mchashjoins

# Run HTM
for i in `seq 0 27`
do
  pl $HTM --algo htm --rSize $((2**27)) --transactionSize 1 --probeLength 4 --dataDistr local_shuffle --shuffleRange $((2**i))
done

# Run HTM
for i in `seq 0 27`
do
  pl $HTM --algo htm --rSize $((2**27)) --transactionSize 4 --probeLength 4 --dataDistr local_shuffle --shuffleRange $((2**i))
done

# Run HTM
for i in `seq 0 27`
do
  pl $HTM --algo htm --rSize $((2**27)) --transactionSize 8 --probeLength 4 --dataDistr local_shuffle --shuffleRange $((2**i))
done

# Run HTM
for i in `seq 0 27`
do
  pl $HTM --algo htm --rSize $((2**27)) --transactionSize 16 --probeLength 4 --dataDistr local_shuffle --shuffleRange $((2**i))
done


# Run HTM
for i in `seq 0 27`
do
  pl $HTM --algo htm --rSize $((2**27)) --transactionSize 32 --probeLength 4 --dataDistr local_shuffle --shuffleRange $((2**i))
done

# Run HTM
for i in `seq 0 27`
do
  pl $HTM --algo htm --rSize $((2**27)) --transactionSize 64 --probeLength 4 --dataDistr local_shuffle --shuffleRange $((2**i))
done





