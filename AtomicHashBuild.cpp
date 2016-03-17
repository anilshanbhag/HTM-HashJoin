#include <iostream>
#include <memory>
#include <cmath>
#include <sys/time.h>
#include <atomic>

#include "include/data_gen.h"

using namespace std;
using namespace tbb;

#define NUM_PARTITIONS 64

int main(int argc, char* argv[]) {
  if (argc != 4) {
    cout << "usage: AtomicHashBuild $sizeInTuples $probeLength $dataDistr" << endl;
    exit(1);
  }

  const size_t sizeInTuples = atol(argv[1]);
  const size_t probeLength = atol(argv[2]);
  const string dataDistr = argv[3];
  const size_t inputPartitionSize = sizeInTuples / NUM_PARTITIONS;

  cout << "{"
       << "\"sizeInTuples\": " << sizeInTuples;
  cout << ", "
       << "\"probeLength\": " << probeLength;

  uint32_t tableSize = sizeInTuples*2;
  size_t outputPartitionSize = tableSize / NUM_PARTITIONS;
  auto input = generate_data(dataDistr, sizeInTuples, sizeInTuples);

  uint32_t counters[64]{0};
  uint32_t* num_count = new uint32_t[sizeInTuples]{};
  int zero = 0, above0 = 0, above1 = 0, above2 = 0, above3 = 0, above4 = 0;
  for (int i=0; i<sizeInTuples; i++) {
    uint32_t inp = (input[i] - 1);
    uint32_t part = inp / inputPartitionSize;
    counters[part] += 1;
    num_count[inp] += 1; 
  }
  std::cout<<"In HERE !"<<std::endl;

  for(int i=0; i<sizeInTuples; i++) {
     if (num_count[i] == 0)
       zero += 1;
     if (num_count[i] > 0) 
       above0 += 1;
     if (num_count[i] > 1) 
       above1 += 1;
     if (num_count[i] > 2) 
       above2 += 1;
     if (num_count[i] > 3) 
       above3 += 1;
     if (num_count[i] > 4) 
       above4 += 1;
  }
  std::cout<<std::endl;
  std::cout<<"Above "<<zero<<" "<<above0<<" "<<above1<<" "<<above2<<" "<<above3<<" "<<above4<<std::endl;
  for(int i=0; i<64; i++) {
    std::cout<<counters[i]<<" ";
  }
  cout<<std::endl;

  auto output = new std::atomic<uint32_t>[tableSize]{};

  struct timeval before, after;
  gettimeofday(&before, NULL);

  uint32_t* conflicts = new uint32_t[tableSize];
  uint32_t* conflictCounts = new uint32_t[NUM_PARTITIONS];

  uint32_t tableMask = tableSize - 1;
  parallel_for(blocked_range<size_t>(0, sizeInTuples, inputPartitionSize),
      [output, probeLength, input, tableMask, conflicts, conflictCounts,
      inputPartitionSize, outputPartitionSize](auto range) {
    uint32_t localConflictCount = 0;
    auto localPartitionId = range.begin() / inputPartitionSize;
    auto conflictPartitionStart = outputPartitionSize * localPartitionId;
    for (size_t i = range.begin(); i < range.end(); i += 1) {
      uint32_t curSlot = murmur(input[i]) & tableMask;
      uint32_t probeBudget = probeLength;
      while (probeBudget != 0) {
        uint32_t prevVal = output[curSlot].load(std::memory_order_relaxed);
        if (prevVal == 0) {
          unsigned int zero = 0;
          bool success = output[curSlot].compare_exchange_strong(zero, input[i]);
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
        conflicts[conflictPartitionStart + localConflictCount++] = input[i];
    }

    conflictCounts[localPartitionId] = localConflictCount;
  });

  gettimeofday(&after, NULL);
  std::cout << ", \"hashBuildTimeInMicroseconds\": "
            << (after.tv_sec * 1000000 + after.tv_usec) -
                   (before.tv_sec * 1000000 + before.tv_usec);

  auto inputSum = parallel_deterministic_reduce(
      blocked_range<size_t>(0, sizeInTuples, 1024), 0ul,
      [&input](auto range, auto init) {
    for(size_t i = range.begin(); i < range.end(); i++) {
      init += input[i];
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
      blocked_range<size_t>(0, NUM_PARTITIONS, 1), 0ul,
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
