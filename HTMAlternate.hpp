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

#define TM_TRACK 1

using namespace std;
using namespace tbb;

#define CACHE_LINE_SIZE 32

struct Bucket {
  uint32_t count;
  uint32_t tuples[5];
  Bucket* next;
};

void
HTMHashBuild(uint32_t* relR, uint32_t rSize, uint32_t transactionSize, uint32_t scaleOutput, uint32_t numPartitions,
    uint32_t probeLength) {
  uint32_t numBuckets = rSize / 4;
  uint32_t inputPartitionSize = rSize / numPartitions;

  /* allocate hashtable buckets cache line aligned */
  Bucket* buckets;
  if (posix_memalign((void**)&buckets, CACHE_LINE_SIZE,
                     numBuckets * sizeof(Bucket))){
      perror("Aligned allocation failed!\n");
      exit(EXIT_FAILURE);
  }
  memset(buckets, 0, numBuckets * sizeof(Bucket));

  cout<<sizeof(Bucket)<<endl;

  uint32_t* conflictRanges = new uint32_t[rSize];
  uint32_t* conflictRangeCounts = new uint32_t[numPartitions];

#if TM_TRACK
  tbb::atomic<int> b1=0,b2=0,b3=0,b4=0,b5=0,b6=0,b7=0, b8=0;
#endif // TM_TRACK

  struct timeval before, after;
  gettimeofday(&before, NULL);

  uint32_t tableMask = numBuckets - 1;
  parallel_for(blocked_range<size_t>(0, rSize, inputPartitionSize),
               [buckets, tableMask, transactionSize, inputPartitionSize,
#if TM_TRACK
                &b1, &b2, &b3, &b4, &b5, &b6, &b7, &b8,
#endif
                relR, conflictRanges, conflictRangeCounts](const auto range) {
                 uint32_t localConflictRangeCount = 0;
                 uint32_t localPartitionId = range.begin() / inputPartitionSize;
                 uint32_t conflictPartitionStart = inputPartitionSize * localPartitionId;
                 for(size_t j = range.begin(); j < range.end(); j += transactionSize) {
                   auto status = _xbegin();
                   if(status == _XBEGIN_STARTED) {
                     for(size_t i = j; i < j + transactionSize; i++) {
                       uint32_t slot = relR[i] & tableMask;
                       if (buckets[slot].count != 5) {
                         buckets[slot].tuples[buckets[slot].count++] = relR[i];
                       }
                     }
                     _xend();
                   } else {
                     conflictRanges[conflictPartitionStart + localConflictRangeCount++] = j;
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
                 conflictRangeCounts[localPartitionId] = localConflictRangeCount;
               });

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
                                           [buckets](auto range, auto init) {
                                             for(size_t i = range.begin(); i < range.end(); i++) {
                                               for (uint32_t j = 0; j < buckets[i].count; j++) {
                                                 init += buckets[i].tuples[j];
                                               }
                                             }
                                             return init;
                                           },
                                           [](auto a, auto b) { return a + b; });

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

  int conflictRangeCount = 0;
  for(int i = 0; i < numPartitions; i++) {
    conflictRangeCount += conflictRangeCounts[i];
  }

  int failedTransactions = conflictRangeCount * transactionSize;
  double failedTransactionPercentage = (failedTransactions) / (1.0 * rSize);

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
       << "\"failedTransactions\": " << failedTransactions;
  cout << ", "
       << "\"failedTransactionPercentage\": " << failedTransactionPercentage;
  cout << ", "
       << "\"inputSum\": " << inputSum;
  cout << ", "
       << "\"outputSum\": " << sum + failedTransactionSum;
  cout << "}" << endl;

#if TM_TRACK
  printf("Conflict Reason: %d %d %d %d %d %d %d %d\n", b1, b2, b3, b4, b5, b6, b7, b8);
#endif //TM_TRACK

  free(buckets);
  // delete[] conflicts;
  // delete[] conflictCounts;
  delete[] conflictRanges;
  delete[] conflictRangeCounts;
}
