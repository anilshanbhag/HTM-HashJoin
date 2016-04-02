#!/bin/bash

export N=5

for i in `seq 1 $N`
do
  ./motivation.sh > motivation_log$i
done

for i in `seq 1 $N`
do
  ./SizeToAbortsAndTimeSorted.sh > SizeToAbortsAndTimeSorted_log$i
done

for i in `seq 1 $N`
do
  ./SizeToAbortsAndTimeShuffled.sh > SizeToAbortsAndTimeShuffled_log$i
done

for i in `seq 1 $N`
do
  ./TSizeAndShuffleWindowstoTime.sh > TSizeAndShuffleWindowstoTime_log$i
done

for i in `seq 1 $N`
do
  ./AtomicsVsHTMVsNoCC.sh > AtomicsVsHTMVsNoCC_log$i
done

for i in `seq 1 $N`
do
  ./adaptive.sh > adaptive_log$i
done

for i in `seq 1 $N`
do
  ./adaptive2.sh > adaptive2_log$i
done

for i in `seq 1 $N`
do
  ./probe.sh > probe_log$i
done

