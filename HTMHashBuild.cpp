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
	auto conflicts = std::make_unique<uint32_t[]>(tableSize);
	auto conflictsRanges = std::make_unique<uint32_t[]>(tableSize);

	struct timeval before, after;
	gettimeofday(&before, NULL);

	uint32_t conflictCounts[NUM_PARTITIONS] = {};
	tbb::atomic<size_t> failedTransactionCount{0};
	tbb::atomic<size_t> partitionCounter{0};

	uint32_t tableMask = tableSize - 1;
	parallel_for(blocked_range<size_t>(0, sizeInTuples, partitionSize),
							 [output, &partitionCounter, tableMask, transactionSize, partitionSize, probeLength,
								&conflicts, &conflictCounts, input, &conflictsRanges,
								&failedTransactionCount](const auto range) {
								 unsigned int* localConflictsPtr = conflicts.get();
								 auto localConflictCount = 0;
								 auto localPartitionId = partitionCounter++;
								 auto conflictPartitionStart = partitionSize * localPartitionId;
								 for(size_t j = range.begin(); j < range.end(); j += transactionSize) {
									 auto status = _xbegin();
									 if(status == _XBEGIN_STARTED) {
										 for(size_t i = j; i < j + transactionSize; i++) {
											 uint32_t startSlot = input[i] & tableMask;
											 int offset = 0;
											 while(output[(startSlot + offset) & tableMask] && offset < probeLength)
												 offset++; // we could use quadratic probing by doing <<1
											 if(offset < probeLength)
												 output[(startSlot + offset) & tableMask] = input[i];
											 else
												 localConflictsPtr[conflictPartitionStart + localConflictCount++] = input[i];
										 }
										 _xend();
									 } else {
										 conflictsRanges[failedTransactionCount++] = j;
									 }
								 }
								 conflictCounts[localPartitionId] += localConflictCount;
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
			blocked_range<size_t>(0, failedTransactionCount), 0ul,
			[&input, &conflictsRanges, transactionSize](auto range, auto init) {
				for(size_t i = range.begin(); i < range.end(); i++) {
					for(size_t j = conflictsRanges[i]; j < conflictsRanges[i] + transactionSize; j++) {
						init += input[j];
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

	cout << ", "
			 << "\"conflicts\": " << conflictCount;
	cout << ", "
			 << "\"failedTransaction\": " << failedTransactionCount;
	cout << ", "
			 << "\"inputSum\": " << inputSum;
	cout << ", "
			 << "\"sum\": " << sum + conflictSum + failedTransactionSum;
	cout << "}" << endl;

	return 0;
}
