/* vim: set filetype=cpp: */
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

#include "include/data_gen.h"

using namespace std;
using namespace tbb;

int main(int argc, char* argv[]) {
	if(argc < 5) {
		cout << "usage: HTMHashBuild $inputSize $transactionSize $probeLength $dataDistr $[localShuffleRange]" << endl;
		exit(1);
	}
	uint32_t inputSize = atoi(argv[1]);
	uint32_t transactionSize = atoi(argv[2]);
	uint32_t probeLength = atoi(argv[3]);
	const string dataDistr = argv[4];
	int localShuffleRange = 16;
	if (argc == 6) localShuffleRange = atoi(argv[5]);
	uint32_t inputPartitionSize = inputSize;

	cout << "{"
			 << "\"inputSize\": " << inputSize;
	cout << ", "
			 << "\"transactionSize\": " << transactionSize;

	uint32_t tableSize = inputSize;
  uint32_t outputPartitionSize = tableSize;
	uint32_t* input = generate_data(dataDistr, tableSize, inputSize, localShuffleRange);
	uint32_t* output = new uint32_t[tableSize]{};

	struct timeval before, after;
	gettimeofday(&before, NULL);

	uint32_t* conflicts = new uint32_t[inputSize];
	uint32_t* conflictRanges = new uint32_t[inputSize];

	uint32_t tableMask = tableSize - 1;
  uint32_t conflictCount = 0;
  uint32_t conflictRangeCount = 0;
  uint32_t localPartitionId = 0;
  uint32_t conflictPartitionStart = inputPartitionSize * localPartitionId;
  for(size_t j = 0; j < inputSize; j += transactionSize) {
   auto status = _xbegin();
   if(status == _XBEGIN_STARTED) {
     for(size_t i = j; i < j + transactionSize; i++) {
       conflicts[i] = input[i];
       conflictCount++;
     }
     _xend();
   } else {
     conflictRanges[conflictPartitionStart + conflictRangeCount] = j;
     conflictRangeCount++;
   }
  }

	gettimeofday(&after, NULL);
	std::cout << ", \"hashBuildTimeInMicroseconds\": "
						<< (after.tv_sec * 1000000 + after.tv_usec) -
									 (before.tv_sec * 1000000 + before.tv_usec);

	uint64_t inputSum = 0;
  for(size_t i = 0; i < inputSize; i++) {
    inputSum += input[i];
  }

	uint64_t sum = 0;
  for(size_t i = 0; i < tableSize; i++) {
    sum += output[i];
  }

	uint64_t failedTransactionSum = 0;
  for(int j = 0; j < conflictRangeCount; j++) {
    for(size_t k = conflictRanges[j]; k < conflictRanges[j] + transactionSize; k++) {
      failedTransactionSum += input[k];
    }
  }

	uint64_t conflictSum = 0;
  for(int j = 0; j < conflictCount; j++) {
    conflictSum += conflicts[j];
  }

	uint64_t failedTransactions = conflictRangeCount * transactionSize;
	double failedTransactionPercentage = (failedTransactions) / (1.0 * inputSize);
	double totalFailedPercentage = (failedTransactions + conflictCount) / (1.0 * inputSize);

	cout << ", "
			 << "\"conflicts\": " << conflictCount;
	cout << ", "
			 << "\"failedTransaction\": " << failedTransactions;
	cout << ", "
			 << "\"failedTransactionPercentage\": " << failedTransactionPercentage;
	cout << ", "
			 << "\"inputSum\": " << inputSum;
	cout << ", "
			 << "\"sum\": " << sum + conflictSum + failedTransactionSum;
	cout << "}" << endl;

	return 0;
}
