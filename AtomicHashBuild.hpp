#pragma once

#include <iostream>
#include <tbb/tbb.h>
#include <cmath>
#include <sys/time.h>
#include <atomic>

using namespace std;
using namespace tbb;

void
AtomicHashBuild(uint32_t* relR, uint32_t rSize, uint32_t scaleOutput, uint32_t numPartitions,
    uint32_t probeLength) {
  uint32_t tableSize = rSize*2;
  uint32_t inputPartitionSize = rSize / numPartitions;
  uint32_t outputPartitionSize = tableSize / numPartitions;

  struct timeval before, after;
  gettimeofday(&before, NULL);

  auto output = new std::atomic<uint32_t>[tableSize]{};
  uint32_t* conflictCounts = (uint32_t*) malloc(sizeof(uint32_t) * numPartitions);
  uint32_t* conflicts = (uint32_t*) malloc(sizeof(uint32_t) * tableSize);

  uint32_t tableMask = tableSize - 1;
  parallel_for(blocked_range<size_t>(0, rSize, inputPartitionSize),
      [output, probeLength, relR, tableMask, conflicts, conflictCounts,
      inputPartitionSize, outputPartitionSize](auto range) {
    uint32_t localConflictCount = 0;
    auto localPartitionId = range.begin() / inputPartitionSize;
    auto conflictPartitionStart = outputPartitionSize * localPartitionId;
    for (size_t i = range.begin(); i < range.end(); i += 1) {
      uint32_t curSlot = relR[i] & tableMask;
      uint32_t probeBudget = probeLength;
      while (probeBudget != 0) {
        uint32_t prevVal = output[curSlot].load(std::memory_order_relaxed);
        if (prevVal == 0) {
          unsigned int zero = 0;
          bool success = output[curSlot].compare_exchange_strong(zero, relR[i]);
          if (success) {
            break;
          }
          probeBudget--;
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

  auto inputSum = parallel_deterministic_reduce(
      blocked_range<size_t>(0, rSize, 1024), 0ul,
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
      init += output[i].load(std::memory_order_relaxed);
    }
    return init;
  },
  [](auto a, auto b) { return a + b; });

  auto conflictSum = parallel_deterministic_reduce(
      blocked_range<size_t>(0, numPartitions, 1), 0ul,
      [&conflicts, outputPartitionSize, &conflictCounts](auto range, auto init) {
    for(size_t i = range.begin(); i < range.end(); i++) {
      for(int j=outputPartitionSize*i; j<outputPartitionSize*i + conflictCounts[i]; j++) {
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
       << "\"algo\": \"atomic\"",
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

  free(conflicts);
  free(conflictCounts);
  delete[] output;
}
