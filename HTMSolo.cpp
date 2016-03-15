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

#define NUM_PARTITIONS 64

int main(int argc, char* argv[]) {
	if(argc != 5) {
		cout << "usage: HTMHashBuild $sizeInTuples $transactionSize $probeLength $dataDistr" << endl;
		exit(1);
	}
	const size_t sizeInTuples = atol(argv[1]);
	const size_t transactionSize = atol(argv[2]);
	const size_t probeLength = atol(argv[3]);
	const string dataDistr = argv[4];
	const size_t partitionSize = sizeInTuples / NUM_PARTITIONS;

	cout << "{"
			 << "\"sizeInTuples\": " << sizeInTuples;
	cout << ", "
			 << "\"transactionSize\": " << transactionSize;
	cout << ", "
			 << "\"probeLength\": " << probeLength;

	uint32_t tableSize = sizeInTuples;
	auto input = generate_data(dataDistr, tableSize, sizeInTuples);
	auto output = new uint32_t[tableSize]{};

	struct timeval before, after;
	gettimeofday(&before, NULL);

	uint32_t* conflicts = new uint32_t[tableSize];
	uint32_t* conflictCounts = new uint32_t[NUM_PARTITIONS];

	uint32_t* conflictRanges = new uint32_t[tableSize];
	uint32_t* conflictRangeCounts = new uint32_t[NUM_PARTITIONS];

	uint32_t tableMask = tableSize - 1;
	parallel_for(blocked_range<size_t>(0, sizeInTuples, partitionSize),
							 [output, tableMask, transactionSize, partitionSize, probeLength,
								conflicts, conflictCounts, input, conflictRanges,
								conflictRangeCounts](const auto range) {
								 uint32_t localConflictCount = 0;
                 uint32_t localConflictRangeCount = 0;
								 uint32_t localPartitionId = range.begin() / partitionSize;
								 uint32_t conflictPartitionStart = partitionSize * localPartitionId;
								 for(size_t j = range.begin(); j < range.end(); j += transactionSize) {
									 auto status = _xbegin();
									 if(status == _XBEGIN_STARTED) {
										 for(size_t i = j; i < j + transactionSize; i++) {
											 uint32_t curSlot = input[i] & tableMask;
                       uint32_t probeBudget = probeLength;
											 while (probeBudget != 0) {
                          if (output[curSlot] == 0) {
                            output[curSlot] = input[i];
                            break;
                          } else {
                            curSlot += 1; // we could use quadratic probing by doing <<1
                            curSlot &= tableMask;
                            probeBudget--;
                          }
                       }

                       // if (probeBudget == 0)
											 //	 conflicts[conflictPartitionStart + localConflictCount++] = input[i];
										 }
										 _xend();
									 } else {
										 conflictRanges[conflictPartitionStart + localConflictRangeCount++] = j;
									 }
								 }
								 conflictCounts[localPartitionId] = localConflictCount;
								 conflictRangeCounts[localPartitionId] = localConflictRangeCount;
							 });
	gettimeofday(&after, NULL);
	std::cout << ", \"hashBuildTimeInMicroseconds\": "
						<< (after.tv_sec * 1000000 + after.tv_usec) -
									 (before.tv_sec * 1000000 + before.tv_usec);

	auto inputSum =
			parallel_deterministic_reduce(blocked_range<size_t>(0, sizeInTuples, 1024), 0ul,
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

	auto failedTransactionSum = parallel_deterministic_reduce(
			blocked_range<size_t>(0, NUM_PARTITIONS, 1), 0ul,
			[input, transactionSize, &conflictRanges, &conflictRangeCounts, partitionSize](auto range, auto init) {
				for(size_t i = range.begin(); i < range.end(); i++) {
					for(int j = partitionSize * i; j < partitionSize * i + conflictRangeCounts[i]; j++) {
            for(size_t k = conflictRanges[j]; k < conflictRanges[j] + transactionSize; k++) {
              init += input[k];
            }
          }
				}
				return init;
			},
			[](auto a, auto b) { return a + b; });


	auto conflictSum = parallel_deterministic_reduce(
			blocked_range<size_t>(0, NUM_PARTITIONS, 1), 0ul,
			[&conflicts, &conflictCounts, partitionSize](auto range, auto init) {
				for(size_t i = range.begin(); i < range.end(); i++) {
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
  double failedPercentage = (failedTransactions + conflictCount) / (1.0 * sizeInTuples);

	cout << ", "
			 << "\"conflicts\": " << conflictCount;
	cout << ", "
			 << "\"failedTransaction\": " << failedTransactions;
	cout << ", "
			 << "\"failedPercentage\": " << failedPercentage;
	cout << ", "
			 << "\"inputSum\": " << inputSum;
	cout << ", "
			 << "\"sum\": " << sum + conflictSum + failedTransactionSum;
	cout << "}" << endl;

	return 0;
}
