#pragma once

#include <iostream>
#include <tbb/tbb.h>
#include <cmath>
#include <sys/time.h>

using namespace std;
using namespace tbb;

void
NoCCHashBuild(uint32_t* relR, uint32_t rSize, uint32_t scaleOutput, uint32_t numPartitions,
    uint32_t probeLength) {
  uint32_t tableSize = rSize*2;
  uint32_t inputPartitionSize = rSize / numPartitions;
  uint32_t outputPartitionSize = tableSize / numPartitions;

  uint32_t* output = (uint32_t*) calloc(tableSize, sizeof(uint32_t));
  uint32_t* conflicts = (uint32_t*) malloc(sizeof(uint32_t) * rSize);
  uint32_t* conflictCounts = (uint32_t*) malloc(sizeof(uint32_t) * numPartitions);

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
  cout << ", "
       << "\"inputSum\": " << inputSum;
  cout << ", "
       << "\"outputSum\": " << sum + conflictSum;
  cout << "}" << endl;

  free(output);
  free(conflicts);
  free(conflictCounts);
}
