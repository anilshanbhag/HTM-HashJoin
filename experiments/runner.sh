#!/bin/bash

for i in `seq 1 5`
do
  ./motivation.sh > motivation_log$i
done

#for i in `seq 1 5`
#do
  #./SizeToAbortsAndTimeSorted.sh > SizeToAbortsAndTimeSorted_log$i
#done

#for i in `seq 1 5`
#do
  #./SizeToAbortsAndTimeShuffled.sh > SizeToAbortsAndTimeShuffled_log$i
#done

for i in `seq 1 5`
do
  ./TSizeAndShuffleWindowstoTime.sh > TSizeAndShuffleWindowstoTime_log$i
done

#for i in `seq 1 5`
#do
  #./AtomicsVsHTMVsNoCC.sh > AtomicsVsHTMVsNoCC_log$i
#done

for i in `seq 1 5`
do
  ./adaptive.sh > adaptive_log$i
done

for i in `seq 1 5`
do
  ./adaptive2.sh > adaptive2_log$i
done

for i in `seq 1 5`
do
  ./probe.sh > probe_log$i
done

