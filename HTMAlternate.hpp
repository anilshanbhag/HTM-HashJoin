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

struct Bucket {
  uint64_t tuples[3];
  uint32_t count;
  uint32_t nextIndex;
};

void
HTMHashBuild(uint64_t* relR, uint32_t rSize,
#if ENABLE_PROBE
    uint64_t* relS, uint32_t sSize,
#endif
    uint32_t transactionSize, uint32_t scaleOutput, uint32_t numPartitions,
    uint32_t probeLength) {
  uint32_t numBuckets = rSize / 2;
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

  uint32_t* conflictRanges = new uint32_t[rSize]{};
  uint32_t* conflictRangeCounts = new uint32_t[numPartitions]{};

#if ENABLE_PROBE
  uint32_t* matchCounter = new uint32_t[numPartitions];
#endif // ENABLE_PROBE

#if TM_TRACK
  tbb::atomic<int> b1=0,b2=0,b3=0,b4=0,b5=0,b6=0,b7=0;
#endif // TM_TRACK

  struct timeval before, after;
  gettimeofday(&before, NULL);

  uint32_t tableMask = numBuckets - 1;
  parallel_for(blocked_range<size_t>(0, rSize, inputPartitionSize),
               [buckets, tableMask, transactionSize, inputPartitionSize,
#if TM_TRACK
                &b1, &b2, &b3, &b4, &b5, &b6, &b7,
#endif
                relR, conflictRanges, conflictRangeCounts](const auto range) {
                 uint32_t localConflictCount = 0;
                 uint32_t localConflictRangeCount = 0;
                 uint32_t localPartitionId = range.begin() / inputPartitionSize;
                 uint32_t conflictPartitionStart = inputPartitionSize * localPartitionId;
                 for(size_t j = range.begin(); j < range.end(); j += transactionSize) {
                   auto status = _xbegin();
                   if(status == _XBEGIN_STARTED) {
                     for(size_t i = j; i < j + transactionSize; i++) {

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
                     conflictRanges[conflictPartitionStart + localConflictRangeCount++] = j;
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
                 conflictCounts[i] = localConflictCount;
                 conflictRangeCounts[localPartitionId] = localConflictRangeCount;
               });

#if TM_RETRY
  for (int i=0; i<numPartitions; i++) {
    uint32_t conflictPartitionStart = inputPartitionSize * i;
    uint32_t localConflictCount = conflictCounts[i];
    for (int j=inputPartitionSize*i; j<inputPartitionSize*i + conflictRangeCounts[i]; j++) {
      for(size_t k = conflictRanges[j]; k < conflictRanges[j] + transactionSize; k++) {
        uint32_t slot = (relR[i]/3) & tableMask;
        if (buckets[slot].count != 3) {
          buckets[slot].tuples[buckets[slot].count++] = relR[i];
        } else {
          // TODO: We can insert directly these entries with chaining.
          conflicts[conflictPartitionStart + localConflictCount++] = relR[i];
        }
      }
    }
    conflictCounts[i] = localConflictCount;
  }

  int conflictCount = 0;
  for(int i = 0; i < numPartitions; i++) {
    conflictCount += conflictCounts[i];
  }

  // Create a list of overflow buckets.
  Bucket* overflows = &buckets[conflictCount];
  int curCounter = 0;

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
            if (curBucket->count == 3) {
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
                                           [buckets,
#if TM_RETRY
                                           overflows
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
      [relR, transactionSize, &conflictRanges, &conflictRangeCounts, inputPartitionSize](auto range, auto init) {
        for(size_t i = range.begin(); i < range.end(); i++) {
          for(int j = inputPartitionSize * i; j < inputPartitionSize * i + conflictRangeCounts[i]; j++) {
            for(size_t k = conflictRanges[j]; k < conflictRanges[j] + transactionSize; k++) {
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

  int conflictCount = 0;
  for(int i = 0; i < numPartitions; i++) {
    conflictCount += conflictCounts[i];
  }

#if ENABLE_PROBE
  int totalMatches = 0;
  for (int i=0; i<numPartitions; i++) {
    totalMatches += matchCounter[i];
  }
#endif //ENABLE_PROBE

  int failedTransactions = conflictRangeCount * transactionSize;
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
