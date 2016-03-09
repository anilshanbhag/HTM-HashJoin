/* vim: set filetype=cpp: */
#include <iostream>
#include <tbb/tbb.h>
#include <memory>
#include <cmath>
#include <sys/time.h>

#include "include/data_gen.h"

using namespace std;
using namespace tbb;

#define NUM_PARTITIONS 64

int main(int argc, char* argv[]) {
  if(argc != 4) {
    cout << "usage: JustHashBuild $sizeInTuples $probeLength $dataDistr" << endl;
    exit(1);
  }

  const size_t numberOfThreads = 8;
  const size_t sizeInTuples = atol(argv[1]);
  const size_t probeLength = atol(argv[2]);
  const string dataDistr = argv[3];
  const size_t partitionSize = sizeInTuples / NUM_PARTITIONS;

  cout << "{"
       << "\"sizeInTuples\": " << sizeInTuples;
  cout << ", "
       << "\"probeLength\": " << probeLength;

  uint32_t tableSize = sizeInTuples;
  auto input = generate_data(dataDistr, tableSize, sizeInTuples);
  auto output = new uint32_t[tableSize]{};

  struct timeval before, after;
  gettimeofday(&before, NULL);

  uint32_t conflictCounts[NUM_PARTITIONS] = {};
  tbb::atomic<size_t> partitionCounter{0};
  auto conflicts = std::make_unique<uint32_t[]>(tableSize);

  uint32_t tableMask = tableSize - 1;
  parallel_for(blocked_range<size_t>(0, sizeInTuples, partitionSize),
      [&output, &partitionCounter, partitionSize, tableMask,
      probeLength, &conflicts, &conflictCounts, &input](const auto range) {
    auto localConflictCount = 0;
    auto localPartitionId = partitionCounter++;
    auto conflictPartitionStart = partitionSize * localPartitionId;
    for (size_t i = range.begin(); i < range.end(); i += 1) {
      int offset = 0;
      uint32_t startSlot = input[i] & tableMask;
      while (output[startSlot + offset] && offset < probeLength)
        offset++; // we could use quadratic probing by doing <<1
      if (offset < probeLength)
        output[startSlot + offset] = input[i];
      else
        conflicts[conflictPartitionStart + localConflictCount++] = input[i];
    }

    conflictCounts[localPartitionId] += localConflictCount;
  });
  gettimeofday(&after, NULL);
  std::cout << ", \"hashBuildTimeInMicroseconds\": "
            << (after.tv_sec * 1000000 + after.tv_usec) -
                   (before.tv_sec * 1000000 + before.tv_usec);

  auto inputSum = parallel_deterministic_reduce(blocked_range<size_t>(0, sizeInTuples, 1024), 0ul,
      [&input](auto range, auto init) {
    for(size_t i = range.begin(); i < range.end(); i++) {
      init += input[i];
    }
    return init;
  },
  [](auto a, auto b) { return a + b; });

  auto sum = parallel_deterministic_reduce(blocked_range<size_t>(0, sizeInTuples, 1024), 0ul,
      [&output](auto range, auto init) {
    for(size_t i = range.begin(); i < range.end(); i++) {
      init += output[i];
    }
    return init;
  },
  [](auto a, auto b) { return a + b; });

  auto conflictSum = parallel_deterministic_reduce(
      blocked_range<size_t>(0, NUM_PARTITIONS, 1), 0ul,
      [&conflicts, partitionSize, &conflictCounts](auto range, auto init) {
    for(size_t i = range.begin(); i < range.end(); i++) {
      for(int j=partitionSize*i; j<partitionSize*i + conflictCounts[i]; j++) {
        init += conflicts[j];
      }
    }
    return init;
  },
  [](auto a, auto b) { return a + b; });

  int conflictCount = 0;
  for (int i=0; i<NUM_PARTITIONS; i++) {
    conflictCount += conflictCounts[i];
  }

  cout << ", "
       << "\"conflicts\": " << conflictCount;
  cout << ", "
       << "\"inputSum\": " << inputSum;
  cout << ", "
       << "\"outputSum\": " << sum + conflictSum;
  cout << "}" << endl;

  return 0;
}
