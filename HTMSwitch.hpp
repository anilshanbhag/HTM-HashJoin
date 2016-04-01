#pragma once

#include <iostream>
#ifdef __APPLE__
#include <rtm.h>
#else
#include <immintrin.h>
#endif
#include <tbb/tbb.h>
#include <memory>
#include <cmath>
#include <sys/time.h>

#include "config.h"

#define K 10

using namespace std;
using namespace tbb;

void
HTMHashBuild(uint64_t* relR, uint32_t rSize,
#if ENABLE_PROBE
    uint64_t* relS, uint32_t sSize,
#endif
    uint32_t transactionSize, uint32_t scaleOutput, uint32_t numPartitions,
    uint32_t probeLength) {
  uint32_t tableSize = rSize*2;
  uint32_t inputPartitionSize = rSize / numPartitions;
  uint32_t outputPartitionSize = tableSize / numPartitions;

  uint64_t* output = new uint64_t[tableSize]{};

  uint64_t* conflicts = new uint64_t[rSize]{};
  uint32_t* conflictCounts = new uint32_t[numPartitions]{};

  uint64_t* conflictRanges = new uint64_t[rSize]{};
  uint32_t* conflictRangeCounts = new uint32_t[numPartitions]{};

#if ENABLE_PROBE
  uint32_t* matchCounter = new uint32_t[numPartitions];
#endif // ENABLE_PROBE

#if TM_TRACK
  tbb::atomic<int> b1=0,b2=0,b3=0,b4=0,b5=0,b6=0,b7=0, b8=0;
#endif // TM_TRACK

  struct timeval before, after, afterFirstRound;
  gettimeofday(&before, NULL);

  tbb::atomic<int> firstRoundConflicts = 0;

  uint32_t tableMask = tableSize - 1;
  parallel_for(blocked_range<size_t>(0, rSize, inputPartitionSize),
               [output, tableMask, transactionSize, inputPartitionSize, probeLength,
                conflicts, conflictCounts, relR, conflictRanges, &firstRoundConflicts,
#if TM_TRACK
                &b1, &b2, &b3, &b4, &b5, &b6, &b7, &b8,
#endif
                conflictRangeCounts, outputPartitionSize](const auto range) {
                 uint32_t localConflictCount = 0;
                 uint32_t localConflictRangeCount = 0;
                 uint32_t localPartitionId = range.begin() / inputPartitionSize;
                 uint32_t conflictPartitionStart = inputPartitionSize * localPartitionId;
                 uint32_t localFailsCount = 0;
                 // Initialize tSize to transactionSize.
                 uint32_t tSize = 16;
                 for(size_t k = range.begin(); k < range.begin() + 16384*K; k += 16384) {
                   uint32_t total = 16384 / tSize;
                   uint32_t prevConflictRangeCount = localConflictRangeCount;
                   for(size_t j = k; j < k + 16384; j += tSize) {
                     auto status = _xbegin();
                     if(status == _XBEGIN_STARTED) {
                       for(size_t i = j; i < j + tSize; i++) {
                         uint32_t curSlot = relR[i] & tableMask;
                         uint32_t probeBudget = probeLength;
                         while (probeBudget != 0) {
                            if (output[curSlot] == 0) {
                              output[curSlot] = relR[i];
                              break;
                            } else {
                              curSlot += 1; // we could use quadratic probing by doing <<1
                              curSlot &= tableMask;
                              probeBudget--;
                            }
                         }

                         if (probeBudget == 0)
                           conflicts[conflictPartitionStart + localConflictCount++] = relR[i];
                       }
                       _xend();
                     } else {
                       uint64_t entry = j;
                       entry = (entry << 32) + tSize;
                       conflictRanges[conflictPartitionStart + localConflictRangeCount++] = entry;
                       localFailsCount += tSize;
#if TM_TRACK
                      if (status == _XABORT_EXPLICIT) b1 += 1;
                       else if (status == _XABORT_RETRY) b2 += 1;
                       else if (status == _XABORT_CONFLICT) b3 += 1;
                       else if (status == _XABORT_CAPACITY) b4 += 1;
                       else if (status == _XABORT_DEBUG) b5 += 1;
                       else if (status == _XABORT_NESTED) b6 += 1;
                       else if (status == _XBEGIN_STARTED) b7 += 1;
                       else b8 += 1;
#endif // TRACK_CONFLICT
                     }
                   }

                   double failureFraction = (localConflictRangeCount - prevConflictRangeCount);
                   failureFraction /= total;

                   // if (failureFraction < 0.004) tSize = tSize > 16 ? 32: tSize * 2;
                   // else if (failureFraction > 0.020) tSize = tSize > 2? tSize / 2: 1;
                 }
                 conflictCounts[localPartitionId] = localConflictCount;
                 conflictRangeCounts[localPartitionId] = localConflictRangeCount;
                 firstRoundConflicts += localFailsCount;
                 // cout<<"TSize "<<tSize<<endl;
               });

  uint32_t totalRun = 16384 * K * numPartitions;
  double firstRoundFailureFraction = (firstRoundConflicts / (1.0 * totalRun));

  gettimeofday(&afterFirstRound, NULL);

  parallel_for(blocked_range<size_t>(0, rSize, inputPartitionSize),
               [output, tableMask, transactionSize, inputPartitionSize, probeLength,
                conflicts, conflictCounts, relR, conflictRanges,
#if TM_TRACK
                &b1, &b2, &b3, &b4, &b5, &b6, &b7, &b8,
#endif
                conflictRangeCounts, outputPartitionSize](const auto range) {
                 uint32_t localConflictCount = 0;
                 uint32_t localConflictRangeCount = 0;
                 uint32_t localPartitionId = range.begin() / inputPartitionSize;
                 uint32_t conflictPartitionStart = inputPartitionSize * localPartitionId;
                 // Initialize tSize to transactionSize.
                 uint32_t tSize = transactionSize;
                 for(size_t k = range.begin() + 16384*K; k < range.end(); k += 16384) {
                   uint32_t total = 16384 / tSize;
                   uint32_t prevConflictRangeCount = localConflictRangeCount;
                   for(size_t j = k; j < k + 16384; j += tSize) {
                     auto status = _xbegin();
                     if(status == _XBEGIN_STARTED) {
                       for(size_t i = j; i < j + tSize; i++) {
                         uint32_t curSlot = relR[i] & tableMask;
                         uint32_t probeBudget = probeLength;
                         while (probeBudget != 0) {
                            if (output[curSlot] == 0) {
                              output[curSlot] = relR[i];
                              break;
                            } else {
                              curSlot += 1; // we could use quadratic probing by doing <<1
                              curSlot &= tableMask;
                              probeBudget--;
                            }
                         }

                         if (probeBudget == 0)
                           conflicts[conflictPartitionStart + localConflictCount++] = relR[i];
                       }
                       _xend();
                     } else {
                       uint64_t entry = j;
                       entry = (entry << 32) + tSize;
                       conflictRanges[conflictPartitionStart + localConflictRangeCount++] = entry;
#if TM_TRACK
                      if (status == _XABORT_EXPLICIT) b1 += 1;
                       else if (status == _XABORT_RETRY) b2 += 1;
                       else if (status == _XABORT_CONFLICT) b3 += 1;
                       else if (status == _XABORT_CAPACITY) b4 += 1;
                       else if (status == _XABORT_DEBUG) b5 += 1;
                       else if (status == _XABORT_NESTED) b6 += 1;
                       else if (status == _XBEGIN_STARTED) b7 += 1;
                       else b8 += 1;
#endif // TRACK_CONFLICT
                     }
                   }

                   double failureFraction = (localConflictRangeCount - prevConflictRangeCount);
                   failureFraction /= total;

                   if (failureFraction < 0.004) tSize = tSize > 16 ? 32: tSize * 2;
                   else if (failureFraction > 0.020) tSize = tSize > 2? tSize / 2: 1;
                 }
                 conflictCounts[localPartitionId] = localConflictCount;
                 conflictRangeCounts[localPartitionId] = localConflictRangeCount;

                 // cout<<"TSize "<<tSize<<endl;
               });

#if TM_RETRY
  for (int i=0; i<numPartitions; i++) {
    uint32_t conflictPartitionStart = inputPartitionSize * i;
    uint32_t localConflictCount = conflictCounts[i];
    for (int j=inputPartitionSize*i; j<inputPartitionSize*i + conflictRangeCounts[i]; j++) {
      uint64_t entry = conflictRanges[j];
      uint64_t start = entry >> 32;
      uint64_t tSize = ((entry << 32) >> 32);
      for(size_t k = start; k < start + tSize; k++) {
        uint32_t curSlot = relR[k] & tableMask;
        uint32_t probeBudget = probeLength;
        while (probeBudget != 0) {
          if (output[curSlot] == 0) {
            output[curSlot] = relR[k];
            break;
          } else {
            curSlot += 1; // we could use quadratic probing by doing <<1
            curSlot &= tableMask;
            probeBudget--;
          }
        }

        if (probeBudget == 0)
         conflicts[conflictPartitionStart + localConflictCount++] = relR[i];
      }
    }
  }
#endif // TM_RETRY

#if ENABLE_PROBE
  uint32_t sPartitionSize = sSize/numPartitions;
  parallel_for(blocked_range<size_t>(0, sSize, sPartitionSize),
               [relS, matchCounter, sPartitionSize, tableMask, probeLength, output](auto range) {
                 uint32_t pId = range.begin() / sPartitionSize;
                 uint32_t matches = 0;
                 for(size_t i = range.begin(); i< range.end(); i++) {
                   uint32_t curSlot = relS[i] & tableMask;
                   uint32_t probeBudget = probeLength;
                   while(probeBudget--) {
                     if (output[curSlot] == relS[i]) {matches++; curSlot++;}
                     else if (output[curSlot] != 0) curSlot++;
                     else break;
                     // matches += (output[curSlot] == relS[i]);
                     // curSlot++;
                   }
                 }
                 matchCounter[pId] = matches;
               });
#endif // ENABLE_PROBE
  gettimeofday(&after, NULL);

  auto inputSum =
      parallel_deterministic_reduce(blocked_range<size_t>(0, rSize, 1024), 0ul,
                                    [&relR](auto range, auto init) {
                                      for(size_t i = range.begin(); i < range.end(); i++) {
                                        init += relR[i];
                                      }
                                      return init;
                                    },
                                    [](auto a, auto b) { return a + b; });

  auto sum = parallel_deterministic_reduce(blocked_range<size_t>(0, tableSize, 1024), 0ul,
                                           [&output](auto range, auto init) {
                                             for(size_t i = range.begin(); i < range.end(); i++) {
                                               init += output[i];
                                             }
                                             return init;
                                           },
                                           [](auto a, auto b) { return a + b; });


#if TM_RETRY == 0
  auto failedTransactionSum = parallel_deterministic_reduce(
      blocked_range<size_t>(0, numPartitions, 1), 0ul,
      [relR, &conflictRanges, &conflictRangeCounts, inputPartitionSize](auto range, auto init) {
        for(size_t i = range.begin(); i < range.end(); i++) {
          for(int j = inputPartitionSize * i; j < inputPartitionSize * i + conflictRangeCounts[i]; j++) {
            uint64_t entry = conflictRanges[j];
            uint64_t start = entry >> 32;
            uint64_t tSize = ((entry << 32) >> 32);
            for(size_t k = start; k < start + tSize; k++) {
              init += relR[k];
            }
          }
        }
        return init;
      },
      [](auto a, auto b) { return a + b; });
#else
  uint64_t failedTransactionSum = 0;
#endif

  auto conflictSum = parallel_deterministic_reduce(
      blocked_range<size_t>(0, numPartitions, 1), 0ul,
      [&conflicts, &conflictCounts, inputPartitionSize](auto range, auto init) {
        for(size_t i = range.begin(); i < range.end(); i++) {
          for(int j = inputPartitionSize * i; j < inputPartitionSize * i + conflictCounts[i]; j++) {
            init += conflicts[j];
          }
        }
        return init;
      },
      [](auto a, auto b) { return a + b; });

  int conflictCount = 0;
  for(int i = 0; i < numPartitions; i++) {
    conflictCount += conflictCounts[i];
  }

  uint32_t failedTransactions = parallel_deterministic_reduce(
      blocked_range<size_t>(0, numPartitions, 1), 0ul,
      [relR, &conflictRanges, &conflictRangeCounts, inputPartitionSize](auto range, auto init) {
        for(size_t i = range.begin(); i < range.end(); i++) {
          for(int j = inputPartitionSize * i; j < inputPartitionSize * i + conflictRangeCounts[i]; j++) {
            uint64_t entry = conflictRanges[j];
            uint64_t tSize = ((entry << 32) >> 32);
            init += tSize;
          }
        }
        return init;
      },
      [](auto a, auto b) { return a + b; });

#if ENABLE_PROBE
  int totalMatches = 0;
  for (int i=0; i<numPartitions; i++) {
    totalMatches += matchCounter[i];
  }
#endif //ENABLE_PROBE

  double failedTransactionPercentage = (failedTransactions) / (1.0 * rSize);
  double failedPercentage = (failedTransactions + conflictCount) / (1.0 * rSize);

  cout << "{"
       << "\"algo\": \"htm\"",
  cout << ","
       << "\"rSize\": " << rSize;
  cout << ", "
       << "\"transactionSize\": " << transactionSize;
  cout << ", "
       << "\"probeLength\": " << probeLength;
  cout << ", \"hashBuildTimeInMicroseconds\": "
       << (after.tv_sec * 1000000 + after.tv_usec) -
               (before.tv_sec * 1000000 + before.tv_usec);
  cout << ", \"firstRoundTime\": "
       << (afterFirstRound.tv_sec * 1000000 + afterFirstRound.tv_usec) -
               (before.tv_sec * 1000000 + before.tv_usec);
  cout << ", \"firstRoundFailureFraction\": "
       <<  firstRoundFailureFraction;
  cout << ", "
       << "\"conflictCount\": " << conflictCount;
  cout << ", "
       << "\"failedTransactions\": " << failedTransactions;
  cout << ", "
       << "\"failedTransactionPercentage\": " << failedTransactionPercentage;
  cout << ", "
       << "\"totalFailedPercentage\": " << failedPercentage;
#if ENABLE_PROBE
  cout << ", "
       << "\"totalMatches\": " << totalMatches;
#endif
  cout << ", "
       << "\"inputSum\": " << inputSum;
  cout << ", "
       << "\"outputSum\": " << sum + conflictSum + failedTransactionSum;
  cout << "}" << endl;

#if TM_TRACK
  printf("Conflict Reason: %d %d %d %d %d %d %d %d\n", b1, b2, b3, b4, b5, b6, b7, b8);
#endif //TM_TRACK

  delete[] output;
  delete[] conflicts;
  delete[] conflictCounts;
  delete[] conflictRanges;
  delete[] conflictRangeCounts;
}
