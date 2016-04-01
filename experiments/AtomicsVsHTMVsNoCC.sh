#!/bin/bash

HTMHOME=/home/anil/htm
HTM=$HTMHOME/noretry

pl $HTM --algo nocc --rSize $((2**27)) --probeLength 4 --dataDistr sorted
pl $HTM --algo nocc --rSize $((2**27)) --probeLength 4 --dataDistr shuffle
pl $HTM --algo atomic --rSize $((2**27)) --probeLength 4 --dataDistr sorted
pl $HTM --algo atomic --rSize $((2**27)) --probeLength 4 --dataDistr shuffle
pl $HTM --algo htm --rSize $((2**27)) --transactionSize 1 --probeLength 4 --dataDistr sorted
pl $HTM --algo htm --rSize $((2**27)) --transactionSize 1 --probeLength 4 --dataDistr shuffle
