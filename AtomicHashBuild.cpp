#include <iostream>
#include <memory>
#include <cmath>
#include <sys/time.h>
#include "include/data_gen.h"
#include <atomic>

using namespace std;
using namespace tbb;

int main(int argc, char* argv[]) {
  if (argc != 4) {
    cout << "usage: AtomicHashBuild $sizeInTuples $grainsize $probeLength" << endl;
    exit(1);
  }

  const size_t sizeInTuples = atol(argv[1]);
  const size_t grainSize = atol(argv[2]);
  const size_t probeLength = atol(argv[3]);
  cout << "{"
       << "\"sizeInTuples\": " << sizeInTuples;
  cout << ", "
       << "\"grainSize\": " << grainSize;
  cout << ", "
       << "\"probeLength\": " << grainSize;

  uint32_t tableSize = sizeInTuples;
  auto input = generate_data("sorted", tableSize, sizeInTuples);
  auto output = new std::atomic<uint32_t>[tableSize]{};

  struct timeval before, after;
  gettimeofday(&before, NULL);

  uint32_t tableMask = tableSize - 1;
  parallel_for(blocked_range<size_t>(0, sizeInTuples, grainSize),
      [&output, probeLength, &input, tableMask](auto range) {
    unsigned int zero = 0;
    for(size_t i = range.begin(); i < range.end(); i += 1) {
      int offset = 0;
      uint32_t startSlot = input[i] & tableMask;
      while(offset < probeLength) {
        uint32_t prevVal = output[startSlot + offset].load(std::memory_order_relaxed);
        if (prevVal == 0) {
          bool success = output[startSlot + offset].compare_exchange_strong(zero, input[i]);
          if (success) {
            break;
          }
        }
        offset++;
      }

      if (offset == probeLength) {
        //TODO: Put in overflow.
      }
    }
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

  auto outputSum = parallel_deterministic_reduce(blocked_range<size_t>(0, sizeInTuples, 1024), 0ul,
      [&output](auto range, auto init) {
    for(size_t i = range.begin(); i < range.end(); i++) {
      init += output[i].load(std::memory_order_relaxed);
    }
    return init;
  },
  [](auto a, auto b) { return a + b; });

	cout << ", "
			 << "\"inputSum\": " << inputSum;
	cout << ", "
			 << "\"outputSum\": " << outputSum;
	cout << "}" << endl;

  return 0;
}
