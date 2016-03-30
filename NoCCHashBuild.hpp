#pragma once

#include <iostream>
#include <tbb/tbb.h>
#include <cmath>
#include <sys/time.h>

#include "config.h"

using namespace std;
using namespace tbb;

void
NoCCHashBuild(uint32_t* relR, uint32_t rSize,
#if ENABLE_PROBE
    uint32_t* relS, uint32_t sSize,
#endif
    uint32_t scaleOutput, uint32_t numPartitions,
    uint32_t probeLength) {
  uint32_t tableSize = rSize*2;
  uint32_t inputPartitionSize = rSize / numPartitions;
  uint32_t outputPartitionSize = tableSize / numPartitions;

  uint32_t* output = new uint32_t[tableSize]{};

  uint32_t* conflicts = new uint32_t[rSize]{};
  uint32_t* conflictCounts = new uint32_t[numPartitions]{};

#if ENABLE_PROBE
  uint32_t* matchCounter = new uint32_t[numPartitions];
#endif // ENABLE_PROBE

  struct timeval before, after;
  gettimeofday(&before, NULL);

  uint32_t tableMask = tableSize - 1;
  parallel_for(blocked_range<size_t>(0, rSize, inputPartitionSize),
      [output, inputPartitionSize, tableMask,
      probeLength, conflicts, conflictCounts, relR](const auto range) {
    auto localConflictCount = 0;
    auto localPartitionId = range.begin() / inputPartitionSize;
    auto conflictPartitionStart = inputPartitionSize * localPartitionId;
    for (size_t i = range.begin(); i < range.end(); i += 1) {
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

    conflictCounts[localPartitionId] = localConflictCount;
  });

#if ENABLE_PROBE
  uint32_t sPartitionSize = sSize/numPartitions;
  parallel_for(blocked_range<size_t>(0, sSize, sPartitionSize),
               [relS, matchCounter, sPartitionSize, tableMask, probeLength, output](auto range) {
                 uint32_t pId = range.begin() / sPartitionSize;
                 uint32_t matches = 0;
                 for(size_t i = range.begin(); i< range.end(); i++) {
                   uint32_t curSlot = relS[i] & tableMask;
                   uint32_t probeBudget = probeLength;
                   while(probeBudget-- && output[curSlot] != 0) {
                     if (output[curSlot] == relS[i]) {matches++; curSlot++;}
                     else if (output[curSlot] != 0) curSlot++;
                     else break;
                   }
                 }
                 matchCounter[pId] = matches;
               });
#endif // ENABLE_PROBE

  gettimeofday(&after, NULL);

  auto inputSum = parallel_deterministic_reduce(blocked_range<size_t>(0, rSize, 1024), 0ul,
      [&relR](auto range, auto init) {
    for(size_t i = range.begin(); i < range.end(); i++) {
      init += relR[i];
    }
    return init;
  },
  [](auto a, auto b) { return a + b; });

  auto sum = parallel_deterministic_reduce(blocked_range<size_t>(0, rSize, 1024), 0ul,
      [&output](auto range, auto init) {
    for(size_t i = range.begin(); i < range.end(); i++) {
      init += output[i];
    }
    return init;
  },
  [](auto a, auto b) { return a + b; });

  auto conflictSum = parallel_deterministic_reduce(
      blocked_range<size_t>(0, numPartitions, 1), 0ul,
      [&conflicts, inputPartitionSize, &conflictCounts](auto range, auto init) {
    for(size_t i = range.begin(); i < range.end(); i++) {
      for(int j=inputPartitionSize*i; j<inputPartitionSize*i + conflictCounts[i]; j++) {
        init += conflicts[j];
      }
    }
    return init;
  },
  [](auto a, auto b) { return a + b; });

  int conflictCount = 0;
  for (int i=0; i<numPartitions; i++) {
    conflictCount += conflictCounts[i];
  }

#if ENABLE_PROBE
  int totalMatches = 0;
  for (int i=0; i<numPartitions; i++) {
    totalMatches += matchCounter[i];
  }
#endif //ENABLE_PROBE

  cout << "{"
       << "\"algo\": \"nocc\"",
  cout << ","
       << "\"rSize\": " << rSize;
  cout << ", "
       << "\"probeLength\": " << probeLength;
  cout << ", \"hashBuildTimeInMicroseconds\": "
       << (after.tv_sec * 1000000 + after.tv_usec) -
               (before.tv_sec * 1000000 + before.tv_usec);
  cout << ", "
       << "\"conflicts\": " << conflictCount;
#if ENABLE_PROBE
  cout << ", "
       << "\"totalMatches\": " << totalMatches;
#endif
  cout << ", "
       << "\"inputSum\": " << inputSum;
  cout << ", "
       << "\"outputSum\": " << sum + conflictSum;
  cout << "}" << endl;

  delete[] output;
  delete[] conflicts;
  delete[] conflictCounts;
}
