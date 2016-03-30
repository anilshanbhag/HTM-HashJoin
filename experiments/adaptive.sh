#!/bin/bash
HTMHOME=/home/anil/htm
HTM=$HTMHOME/main

# Run HTM
for i in `seq 0 27`
do
  pl $HTM --algo htm --rSize $((2**27)) --probeLength 4 --transactionSize 1 --dataDistr local_shuffle --shuffleRange $((2**i))
done

# Run HTM
for i in `seq 0 27`
do
  pl $HTM --algo htm --rSize $((2**27)) --probeLength 4 --transactionSize 4 --dataDistr local_shuffle --shuffleRange $((2**i))
done

# Run HTM
for i in `seq 0 27`
do
  pl $HTM --algo htm --rSize $((2**27)) --probeLength 4 --transactionSize 8 --dataDistr local_shuffle --shuffleRange $((2**i))
done

# Run HTM
for i in `seq 0 27`
do
  pl $HTM --algo htm --rSize $((2**27)) --probeLength 4 --transactionSize 16 --dataDistr local_shuffle --shuffleRange $((2**i))
done

# Run HTM
for i in `seq 0 27`
do
  pl $HTM --algo htm --rSize $((2**27)) --probeLength 4 --transactionSize 32 --dataDistr local_shuffle --shuffleRange $((2**i))
done

# Run HTM
for i in `seq 0 27`
do
  pl $HTM --algo htm --rSize $((2**27)) --probeLength 4 --transactionSize 64 --dataDistr local_shuffle --shuffleRange $((2**i))
done

# Run HTM
for i in `seq 0 27`
do
  pl $HTM --algo htm --rSize $((2**27)) --probeLength 4 --transactionSize 128 --dataDistr local_shuffle --shuffleRange $((2**i))
done
