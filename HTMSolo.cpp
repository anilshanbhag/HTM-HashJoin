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
#include <unistd.h>
#include <cstring>

#include "include/DataGen.hpp"

using namespace std;
using namespace tbb;

#define NUM_PARTITIONS 1

int main(int argc, char* argv[]) {
	if(argc != 5) {
		cout << "usage: HTMHashBuild $inputSize $transactionSize $probeLength $dataDistr" << endl;
		exit(1);
	}
	const uint32_t inputSize = atol(argv[1]);
	const uint32_t transactionSize = atol(argv[2]);
	const uint32_t probeLength = atol(argv[3]);
	const string dataDistr = argv[4];
	const uint32_t partitionSize = inputSize / NUM_PARTITIONS;
	int localShuffleRange = 16;
	if(argc == 6)
		localShuffleRange = atoi(argv[5]);
	uint32_t inputPartitionSize = inputSize / NUM_PARTITIONS;

	cout << "{"
			 << "\"inputSize\": " << inputSize;
	cout << ", "
			 << "\"transactionSize\": " << transactionSize;
	cout << ", "
			 << "\"localShuffleRange\": " << localShuffleRange;
	cout << ", "
			 << "\"numberOfPartitions\": " << NUM_PARTITIONS;
	cout << ", "
			 << "\"probeLength\": " << probeLength;

	uint32_t tableSize = inputSize;
	auto input = generate_data(dataDistr, tableSize, inputSize);
	auto output = new uint32_t[tableSize];

	struct timeval before, after;
	gettimeofday(&before, NULL);

	uint32_t* conflicts = new uint32_t[tableSize];
	uint32_t* conflictCounts = new uint32_t[NUM_PARTITIONS];

	uint32_t* conflictRanges = new uint32_t[tableSize];
	uint32_t* conflictRangeCounts = new uint32_t[NUM_PARTITIONS];

	uint32_t tableMask = tableSize - 1;
    uint32_t b1=0,b2=0,b3=0,b4=0,b5=0,b6=0, b7 = 0;

								 uint32_t localConflictCount = 0;
                 uint32_t localConflictRangeCount = 0;
								 uint32_t localPartitionId = 0 / partitionSize;
								 uint32_t conflictPartitionStart = partitionSize * localPartitionId;
								 for(uint32_t j = 0; j < inputSize; j += transactionSize) {
									 auto status = _xbegin();
									 if(status == _XBEGIN_STARTED) {
										 for(uint32_t i = j; i < j + transactionSize; i++) {
                       output[i] = input[i];
                       // if (probeBudget == 0)
											 //	 conflicts[conflictPartitionStart + localConflictCount++] = input[i];
										 }
										 _xend();
									 } else {
										 conflictRanges[conflictPartitionStart + localConflictRangeCount++] = j;
                                         if (status == _XABORT_EXPLICIT) b1 += 1;
                                         else if (status == _XABORT_RETRY) b2 += 1;
                                         else if (status == _XABORT_CONFLICT) b3 += 1;
                                         else if (status == _XABORT_CAPACITY) b4 += 1;
                                         else if (status == _XABORT_DEBUG) b5 += 1;
                                         else if (status == _XABORT_NESTED) b6 += 1;
                                         else b7 += 1;
									 }
								 }
								 conflictCounts[localPartitionId] = localConflictCount;
								 conflictRangeCounts[localPartitionId] = localConflictRangeCount;

	gettimeofday(&after, NULL);
	std::cout << ", \"hashBuildTimeInMicroseconds\": "
						<< (after.tv_sec * 1000000 + after.tv_usec) -
									 (before.tv_sec * 1000000 + before.tv_usec)<<std::endl;
    printf("%d %d %d %d %d %d %d\n", b1, b2, b3, b4, b5, b6, b7);

	struct timeval a1, a2;
	gettimeofday(&a1, NULL);
  int l = 0;
  for (int i=0; i<tableSize; i++) l += output[i];
  gettimeofday(&a2, NULL);
	std::cout << endl << "TimeInMicroseconds: "
						<< (a2.tv_sec * 1000000 + a2.tv_usec) -
									 (a1.tv_sec * 1000000 + a1.tv_usec)<< " " << l<<endl;
	auto inputSum =
			parallel_deterministic_reduce(blocked_range<uint32_t>(0, inputSize, 1024), 0ul,
																		[&input](auto range, auto init) {
																			for(uint32_t i = range.begin(); i < range.end(); i++) {
																				init += input[i];
																			}
																			return init;
																		},
																		[](auto a, auto b) { return a + b; });

	auto sum = parallel_deterministic_reduce(blocked_range<uint32_t>(0, inputSize, 1024), 0ul,
																					 [&output](auto range, auto init) {
																						 for(uint32_t i = range.begin(); i < range.end(); i++) {
																							 init += output[i];
																						 }
																						 return init;
																					 },
																					 [](auto a, auto b) { return a + b; });

	auto failedTransactionSum = parallel_deterministic_reduce(
			blocked_range<uint32_t>(0, NUM_PARTITIONS, 1), 0ul,
			[input, transactionSize, &conflictRanges, &conflictRangeCounts, partitionSize](auto range, auto init) {
				for(uint32_t i = range.begin(); i < range.end(); i++) {
					for(int j = partitionSize * i; j < partitionSize * i + conflictRangeCounts[i]; j++) {
            for(uint32_t k = conflictRanges[j]; k < conflictRanges[j] + transactionSize; k++) {
              init += input[k];
            }
          }
				}
				return init;
			},
			[](auto a, auto b) { return a + b; });


	auto conflictSum = parallel_deterministic_reduce(
			blocked_range<uint32_t>(0, NUM_PARTITIONS, 1), 0ul,
			[&conflicts, &conflictCounts, partitionSize](auto range, auto init) {
				for(uint32_t i = range.begin(); i < range.end(); i++) {
					for(int j = partitionSize * i; j < partitionSize * i + conflictCounts[i]; j++) {
						init += conflicts[j];
					}
				}
				return init;
			},
			[](auto a, auto b) { return a + b; });

	int conflictCount = 0;
	for(int i = 0; i < NUM_PARTITIONS; i++) {
		conflictCount += conflictCounts[i];
	}

	int conflictRangeCount = 0;
	for(int i = 0; i < NUM_PARTITIONS; i++) {
		conflictRangeCount += conflictRangeCounts[i];
	}
  int failedTransactions = conflictRangeCount * transactionSize;
  double failedPercentage = (failedTransactions + conflictCount) / (1.0 * inputSize);

	double failedTransactionPercentage = round(100000.0*failedTransactions/inputSize)/1000.0;
	double conflictPercentage = round(100000.0*conflictCount/inputSize)/1000.0;
	cout << ", "
			 << "\"conflicts\": " << conflictCount;
	cout << ", "
			 << "\"failedTransaction\": " << failedTransactions;
	cout << ", "
			 << "\"failedPercentage\": " << failedPercentage;
	cout << ", "
			 << "\"failedTransactionPercentage\": " << failedTransactionPercentage;
	cout << ", "
			 << "\"conflictPercentage\": " << conflictPercentage;
	cout << ", "
			 << "\"inputSum\": " << inputSum;
	cout << ", "
			 << "\"sum\": " << sum + conflictSum + failedTransactionSum;
	cout << "}" << endl;

	return 0;
}
