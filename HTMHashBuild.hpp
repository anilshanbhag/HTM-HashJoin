#pragma once

#include <cstdlib>
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
#include <cstring>

#include "config.h"

using namespace std;
using namespace tbb;

#define CACHE_LINE_SIZE 32

#ifndef NEXT_POW_2
/**
 *  compute the next number, greater than or equal to 32-bit unsigned v.
 *  taken from "bit twiddling hacks":
 *  http://graphics.stanford.edu/~seander/bithacks.html
 */
#define NEXT_POW_2(V)                           \
    do {                                        \
        V--;                                    \
        V |= V >> 1;                            \
        V |= V >> 2;                            \
        V |= V >> 4;                            \
        V |= V >> 8;                            \
        V |= V >> 16;                           \
        V++;                                    \
    } while(0)
#endif

struct Bucket {
  uint64_t tuples[3];
  uint32_t count;
  uint32_t nextIndex;
};

// Number of rounds of 16k tuples to process.
#if HTM_SWITCH
#define K 5
#else
#define K 0
#endif

void
HTMHashBuild(uint64_t* relR, uint32_t rSize,
#if ENABLE_PROBE
    uint64_t* relS, uint32_t sSize,
#endif
    uint32_t transactionSize, uint32_t scaleOutput, uint32_t numPartitions,
    uint32_t probeLength) {
  uint32_t numBuckets = rSize / 3 + 1;
  NEXT_POW_2(numBuckets);
  uint32_t inputPartitionSize = rSize / numPartitions;

  Bucket* buckets;
  if (posix_memalign((void**)&buckets, CACHE_LINE_SIZE,
                     numBuckets * sizeof(Bucket))){
      perror("Aligned allocation failed!\n");
      exit(EXIT_FAILURE);
  }

  memset(buckets, 0, numBuckets * sizeof(Bucket));

  int tCount = 0;
  for (int i=0; i<numBuckets; i++) {
    tCount += buckets[i].count;
  }

  uint64_t* conflicts = new uint64_t[rSize]{};
  uint32_t* conflictCounts = new uint32_t[numPartitions]{};

  uint64_t* conflictRanges = new uint64_t[rSize]{};
  uint32_t* conflictRangeCounts = new uint32_t[numPartitions]{};

#if ENABLE_PROBE
  uint32_t* matchCounter = new uint32_t[numPartitions];
#endif // ENABLE_PROBE

#if TM_TRACK
  tbb::atomic<int> b1=0,b2=0,b3=0,b4=0,b5=0,b6=0,b7=0;
#endif // TM_TRACK

  struct timeval before, after, afterFirstRound;
  gettimeofday(&before, NULL);

  uint32_t tableMask = numBuckets - 1;

  // Run a first round to determine if there is sufficient locality in the data.
  // 16k * K tuples run per partition with tSize = 16.
  parallel_for(blocked_range<size_t>(0, rSize, inputPartitionSize),
               [buckets, tableMask, transactionSize, inputPartitionSize,
#if TM_TRACK
                &b1, &b2, &b3, &b4, &b5, &b6, &b7,
#endif
                relR, conflicts, conflictCounts, conflictRanges, conflictRangeCounts](const auto range) {
                 uint32_t localConflictCount = 0;
                 uint32_t localConflictRangeCount = 0;
                 uint32_t localPartitionId = range.begin() / inputPartitionSize;
                 uint32_t conflictPartitionStart = inputPartitionSize * localPartitionId;
                 uint32_t localFailsCount = 0;

                 uint32_t tSize = 16;

                 for(size_t k = range.begin(); k < range.begin() + K*16384; k += 16384) {
                   for(size_t j = k; j < k + 16384; j += tSize) {
                       auto status = _xbegin();
                       if(status == _XBEGIN_STARTED) {
                         for(size_t i = j; i < j + tSize; i++) {

                           // Note that the hash function is (key / 3) & tableMask
                           // This ensures multiple nearby keys hit the same hash bucket.
                           uint32_t slot = (relR[i]/3) & tableMask;
                           if (buckets[slot].count != 3) {
                             buckets[slot].tuples[buckets[slot].count++] = relR[i];
                           } else {
                             conflicts[conflictPartitionStart + localConflictCount++] = relR[i];
                           }
                         }
                         _xend();
                       } else {
                         uint64_t entry = j;
                         entry = (entry << 32) + tSize;
                         conflictRanges[conflictPartitionStart + localConflictRangeCount++] = entry;
#if TM_TRACK
                        if (status & _XABORT_EXPLICIT) b1 += 1;
                        if (status & _XABORT_RETRY) b2 += 1;
                        if (status & _XABORT_CONFLICT) b3 += 1;
                        if (status & _XABORT_CAPACITY) b4 += 1;
                        if (status & _XABORT_DEBUG) b5 += 1;
                        if (status & _XABORT_NESTED) b6 += 1;
                        if (! (status & 0x3f)) b7 += 1;
#endif // TRACK_CONFLICT
                       }
                   }
                 }
                 conflictCounts[localPartitionId] = localConflictCount;
                 conflictRangeCounts[localPartitionId] = localConflictRangeCount;
               });

  uint32_t totalRun = 16384 * K * numPartitions;
  uint32_t firstRoundConflicts = 0;
  for (int i=0; i<numPartitions; i++) firstRoundConflicts += conflictRangeCounts[i];
  firstRoundConflicts *= 16;
  double firstRoundFailureFraction = totalRun > 0 ? (firstRoundConflicts / (1.0 * totalRun)) : 0;

  gettimeofday(&afterFirstRound, NULL);
  parallel_for(blocked_range<size_t>(0, rSize, inputPartitionSize),
               [buckets, tableMask, transactionSize, inputPartitionSize,
#if TM_TRACK
                &b1, &b2, &b3, &b4, &b5, &b6, &b7,
#endif
                relR, conflicts, conflictCounts, conflictRanges, conflictRangeCounts](const auto range) {
                 uint32_t localConflictCount = 0;
                 uint32_t localConflictRangeCount = 0;
                 uint32_t localPartitionId = range.begin() / inputPartitionSize;
                 uint32_t conflictPartitionStart = inputPartitionSize * localPartitionId;

                 uint32_t tSize = transactionSize;

                 for(size_t k = range.begin() + K*16384; k < range.end(); k += 16384) {
                   uint32_t total = 16384 / tSize;
                   uint32_t prevConflictRangeCount = localConflictRangeCount;
                   for(size_t j = k; j < k + 16384; j += tSize) {
                       auto status = _xbegin();
                       if(status == _XBEGIN_STARTED) {
                         for(size_t i = j; i < j + tSize; i++) {

                           // Note that the hash function is (key / 3) & tableMask
                           // This ensures multiple nearby keys hit the same hash bucket.
                           uint32_t slot = (relR[i]/3) & tableMask;
                           if (buckets[slot].count != 3) {
                             buckets[slot].tuples[buckets[slot].count++] = relR[i];
                           } else {
                             conflicts[conflictPartitionStart + localConflictCount++] = relR[i];
                           }
                         }
                         _xend();
                       } else {
                         uint64_t entry = j;
                         entry = (entry << 32) + tSize;
                         conflictRanges[conflictPartitionStart + localConflictRangeCount++] = entry;
#if TM_TRACK
                        if (status & _XABORT_EXPLICIT) b1 += 1;
                        if (status & _XABORT_RETRY) b2 += 1;
                        if (status & _XABORT_CONFLICT) b3 += 1;
                        if (status & _XABORT_CAPACITY) b4 += 1;
                        if (status & _XABORT_DEBUG) b5 += 1;
                        if (status & _XABORT_NESTED) b6 += 1;
                        if (! (status & 0x3f)) b7 += 1;
#endif // TRACK_CONFLICT
                       }
                   }

                   double failureFraction = (localConflictRangeCount - prevConflictRangeCount);
                   failureFraction /= total;

// Adapt the transaction size.
#if HTM_ADAPT
                   if (failureFraction < 0.004) tSize = tSize > 16 ? 32: tSize * 2;
                   else if (failureFraction > 0.020) tSize = tSize > 2? tSize / 2: 1;
#endif
                 }
                 conflictCounts[localPartitionId] = localConflictCount;
                 conflictRangeCounts[localPartitionId] = localConflictRangeCount;
               });

  int conflictCount = 0;

#if TM_RETRY
  for (int i=0; i<numPartitions; i++) {
    uint32_t conflictPartitionStart = inputPartitionSize * i;
    uint32_t localConflictCount = conflictCounts[i];
    for (int j=inputPartitionSize*i; j<inputPartitionSize*i + conflictRangeCounts[i]; j++) {
      uint64_t entry = conflictRanges[j];
      uint64_t start = entry >> 32;
      uint64_t tSize = ((entry << 32) >> 32);
      for(size_t k = start; k < start + tSize; k++) {
        uint32_t slot = (relR[k]/3) & tableMask;
        if (buckets[slot].count != 3) {
          buckets[slot].tuples[buckets[slot].count++] = relR[k];
        } else {
          // TODO: We can insert directly these entries with chaining.
          conflicts[conflictPartitionStart + localConflictCount++] = relR[i];
        }
      }
    }
    conflictCounts[i] = localConflictCount;
  }

  for(int i = 0; i < numPartitions; i++) {
    conflictCount += conflictCounts[i];
  }

  // Create a list of overflow buckets.
  Bucket* overflows = new Bucket[conflictCount + 1]{};
  int curCounter = 1;

  for (int i=0; i<numPartitions; i++) {
    uint32_t conflictPartitionStart = inputPartitionSize * i;
    for (int j=conflictPartitionStart; j<conflictPartitionStart + conflictCounts[i]; j++) {
      uint32_t slot = (relR[i]/3) & tableMask;
      if (buckets[slot].count == 3) {
        int nextIndex = buckets[slot].nextIndex;
        if (nextIndex == 0) {
          buckets[slot].nextIndex = curCounter;
          overflows[curCounter].count = 1;
          overflows[curCounter].tuples[0] = conflicts[j];
          curCounter += 1;
        } else {
          Bucket& curBucket = overflows[nextIndex];
          if (curBucket.count == 3) {
            curBucket = overflows[curCounter];
            curBucket.nextIndex = buckets[slot].nextIndex;
            buckets[slot].nextIndex = curCounter;
            curCounter += 1;
            curBucket.count = 1;
            curBucket.tuples[0] = conflicts[j];
          } else {
            curBucket.tuples[curBucket.count] = conflicts[j];
            curBucket.count += 1;
          }
        }
      } else {
        buckets[slot].tuples[buckets[slot].count] = conflicts[j];
        buckets[slot].count++;
      }
    }
  }

/*  for (int i=0; i<numBuckets; i++) {*/
      //Bucket& b = buckets[i];
    //cout<<"B " << b.count << " " << b.nextIndex << " ";
    //for (int j=0; j<buckets[i].count; j++) cout<< b.tuples[j]<< " ";
    //cout<<endl;
  /*}*/
#endif // TM_RETRY

#if ENABLE_PROBE
  uint32_t sPartitionSize = sSize/numPartitions;
  parallel_for(blocked_range<size_t>(0, sSize, sPartitionSize),
               [relS, matchCounter, sPartitionSize, tableMask, output, buckets, tMask, tbSize, overflows](auto range) {
                 uint32_t pId = range.begin() / sPartitionSize;
                 uint32_t matches = 0;
                 for(size_t i = range.begin(); i< range.end(); i++) {
                   uint32_t curSlot = (relS[i] / 3) & tableMask;
#if BUILD_OVERFLOW_TABLE
                   Bucket& curBucket = buckets[slot];
                   while (true) {
                     for (int j=0; j<curBucket.count; j++)
                         if (curBucket.tuples[j] == relS[i]) matches++;
                     if (curBucket.nextIndex == 0) break;
                     curBucket = overflows[curBucket.nextIndex];
                   }
#endif
                 }
                 matchCounter[pId] = matches;
               });
#endif // ENABLE_PROBE

  gettimeofday(&after, NULL);

  auto inputSum =
      parallel_deterministic_reduce(blocked_range<size_t>(0, rSize, 1024), 0ul,
                                    [relR](auto range, auto init) {
                                      for(size_t i = range.begin(); i < range.end(); i++) {
                                        init += relR[i];
                                      }
                                      return init;
                                    },
                                    [](auto a, auto b) { return a + b; });

  auto sum = parallel_deterministic_reduce(blocked_range<size_t>(0, numBuckets, 1024), 0ul,
                                           [buckets
#if TM_RETRY
                                           ,overflows
#endif
                                           ](auto range, auto init) {
                                             for(size_t i = range.begin(); i < range.end(); i++) {
                                               Bucket& curBucket = buckets[i];
                                               while (true) {
                                                 for (uint32_t j = 0; j < curBucket.count; j++) {
                                                   init += curBucket.tuples[j];
                                                 }
                                                 if (curBucket.nextIndex == 0) break;
#if TM_RETRY
                                                 curBucket = overflows[curBucket.nextIndex];
#endif
                                               }
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

  int conflictRangeCount = 0;
  for(int i = 0; i < numPartitions; i++) {
    conflictRangeCount += conflictRangeCounts[i];
  }

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

#if TM_RETRY
#else
  for(int i = 0; i < numPartitions; i++) {
    conflictCount += conflictCounts[i];
  }
#endif

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
#if TM_RETRY
  double failedPercentage = (conflictCount) / (1.0 * rSize);
#else
  double failedPercentage = (failedTransactions + conflictCount) / (1.0 * rSize);
#endif

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
       << "\"outputSum\": " << sum + failedTransactionSum + conflictSum;
  cout << "}" << endl;

#if TM_TRACK
  printf("Conflict Reason: %d %d %d %d %d %d %d %d\n", b1, b2, b3, b4, b5, b6, b7);
#endif //TM_TRACK

  free(buckets);
  delete[] conflicts;
  delete[] conflictCounts;
  delete[] conflictRanges;
  delete[] conflictRangeCounts;

#if TM_RETRY
  delete[] overflows;
#endif
}
