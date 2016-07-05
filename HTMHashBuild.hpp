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

using namespace std;
using namespace tbb;

#define CACHE_LINE_SIZE 32
#define G 2

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
  uint64_t tuples[2];
  Bucket* next;
  int count;
  int padding;
};

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

  uint32_t* conflictRanges = new uint32_t[rSize]{};
  uint32_t* conflictRangeCounts = new uint32_t[numPartitions]{};

  /* allocate hashtable buckets cache line aligned */
  Bucket* buckets;
  uint32_t numBuckets = rSize / 4;
  if (posix_memalign((void**)&buckets, CACHE_LINE_SIZE,
                     numBuckets * sizeof(Bucket))){
      perror("Aligned allocation failed!\n");
      exit(EXIT_FAILURE);
  }
  memset(buckets, 0, numBuckets * sizeof(Bucket));

  cout<<"IN HERE"<<endl;

#if ENABLE_PROBE
  uint32_t* matchCounter = new uint32_t[numPartitions];
#endif // ENABLE_PROBE

#if TM_TRACK
  tbb::atomic<int> b1=0,b2=0,b3=0,b4=0,b5=0,b6=0,b7=0, b8=0;
#endif // TM_TRACK

  struct timeval before, after;
  gettimeofday(&before, NULL);

  uint32_t tableMask = tableSize - 1;
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
                 for(size_t j = range.begin(); j < range.end(); j += transactionSize) {
                   auto status = _xbegin();
                   if(status == _XBEGIN_STARTED) {
                     for(size_t i = j; i < j + transactionSize; i++) {
                       uint32_t curSlot = (relR[i] * G) & tableMask;
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
                 conflictCounts[localPartitionId] = localConflictCount;
                 conflictRangeCounts[localPartitionId] = localConflictRangeCount;
               });


#if TM_RETRY
  for (int i=0; i<numPartitions; i++) {
    uint32_t conflictPartitionStart = inputPartitionSize * i;
    uint32_t localConflictCount = conflictCounts[i];
    for (int j=inputPartitionSize*i; j<inputPartitionSize*i + conflictRangeCounts[i]; j++) {
      for(size_t k = conflictRanges[j]; k < conflictRanges[j] + transactionSize; k++) {
        uint32_t curSlot = (relR[k] * G) & tableMask;
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
    conflictCounts[i] = localConflictCount;
  }
#endif // TM_RETRY

#if BUILD_OVERFLOW_TABLE
  int conflictCount = 0;
  for(int i = 0; i < numPartitions; i++) {
    conflictCount += conflictCounts[i];
  }

  // Each bucket has 2 tuples.
  int tbSize = conflictCount;
  NEXT_POW_2(tbSize);
  tbSize = tbSize >> 1;
  int tMask = tbSize - 1;
  Bucket* overflows = &buckets[tbSize];
  int curCounter = 0;

  for (int i=0; i<numPartitions; i++) {
    uint32_t conflictPartitionStart = inputPartitionSize * i;
    for (int j=conflictPartitionStart; j<conflictPartitionStart + conflictCounts[i]; j++) {
      int slot = conflicts[j] & tbSize;
      if (buckets[slot].count == 2) {
        Bucket* curBucket = buckets[slot].next;
        if (curBucket == NULL) {
          curBucket = &overflows[curCounter];
          curCounter += 1;
          buckets[slot].next = curBucket;
          curBucket->count = 1;
          curBucket->tuples[0] = conflicts[j];
        } else {
            if (curBucket->count == 2) {
                curBucket = &overflows[curCounter];
                curCounter += 1;
                curBucket->next = buckets[slot].next;
                buckets[slot].next = curBucket;
                curBucket->count = 1;
                curBucket->tuples[0] = conflicts[j];
            } else {
                curBucket->tuples[curBucket->count] = conflicts[j];
                curBucket->count += 1;
            }
        }
      } else {
        buckets[slot].tuples[buckets[slot].count] = conflicts[j];
        buckets[slot].count++;
      }
    }
  }
#endif

#if ENABLE_PROBE
  uint32_t sPartitionSize = sSize/numPartitions;
  tbb::atomic<uint32_t> totalOverflows = 0;
  parallel_for(blocked_range<size_t>(0, sSize, sPartitionSize),
               [relS, matchCounter, sPartitionSize, tableMask, probeLength, output, &totalOverflows, buckets, tMask, tbSize](auto range) {
                 uint32_t pId = range.begin() / sPartitionSize;
                 uint32_t matches = 0;
                 uint32_t overflow = 0;
                 for(size_t i = range.begin(); i< range.end(); i++) {
                   uint32_t curSlot = (relS[i] * G) & tableMask;
                   uint32_t probeBudget = probeLength;
                   while(probeBudget--) {
                     if (output[curSlot] == relS[i]) {matches++; curSlot++;}
                     else if (output[curSlot] != 0) curSlot++;
                     else break;
                     // matches += (output[curSlot] == relS[i]);
                     // curSlot++;
                   }

                   if (probeBudget == 0) {
#if BUILD_OVERFLOW_TABLE
                   if (tbSize > 0) {
                     uint32_t slot = relS[i] & tMask;
                     Bucket* curBucket = &buckets[slot];
                     while (curBucket != NULL) {
                       for (int j=0; j<curBucket->count; j++)
                           if (curBucket->tuples[j] == relS[i]) matches++;
                       curBucket = curBucket->next;
                     }
#endif
                    }
                   }
                 }
                 matchCounter[pId] = matches;
                 totalOverflows += overflow;
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

  int conflictRangeCount = 0;
  for(int i = 0; i < numPartitions; i++) {
    conflictRangeCount += conflictRangeCounts[i];
  }

#if ENABLE_PROBE
  int totalMatches = 0;
  for (int i=0; i<numPartitions; i++) {
    totalMatches += matchCounter[i];
  }
#endif //ENABLE_PROBE

  int failedTransactions = conflictRangeCount * transactionSize;
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
  cout << ", "
       << "\"totalOverflows\": " << totalOverflows;
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
