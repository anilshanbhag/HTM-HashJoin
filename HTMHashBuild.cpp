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

using namespace std;
using namespace tbb;
int main(int argc, char* argv[]) {
	if(argc < 3) {
		cout << "usage: HTMHashBuild $sizeInTuples $grainsize $transactionSize $probelength" << endl;
		exit(1);
	}
	const size_t sizeInTuples = atol(argv[1]);
	const size_t grainSize = atol(argv[2]);
	const size_t transactionsPerGrain = atol(argv[3]);
	const size_t probelength = atol(argv[4]);
	const size_t transactionSize = grainSize / transactionsPerGrain;
	if(transactionSize * transactionsPerGrain != grainSize) {
		cout << "grain not equally divisable by transactionsPerGrain" << endl;
		exit(1);
	}
	cout << "{"
			 << "\"sizeInTuples\": " << sizeInTuples;
	cout << ", "
			 << "\"grainSize\": " << grainSize;
	cout << ", "
			 << "\"transactionsPerGrain\": " << transactionsPerGrain;
	cout << ", "
			 << "\"transactionSize\": " << transactionSize;


	
	auto input = std::make_unique<int[]>(sizeInTuples);
	auto output = std::make_unique<int[]>(sizeInTuples);
	auto conflicts = std::make_unique<int[]>(sizeInTuples);
	auto conflictsRanges = std::make_unique<int[]>(sizeInTuples);

	parallel_for(blocked_range<size_t>(0, sizeInTuples, std::llround(std::ceil(sizeInTuples / 64.0))),
							 [&input, &output, sizeInTuples](auto range) {
								 unsigned int seed = 12738 * range.begin();
								 for(size_t i = range.begin(); i < range.end(); i++) {
									 // input[i] = (rand_r(&seed) % sizeInTuples) + 1;
									 input[i] = i;
									 output[0] = 0;
								 }
							 });

	tbb::atomic<size_t> failureHistogram[255] = {};

	struct timeval before, after;
	gettimeofday(&before, NULL);

	size_t conflictCount = 0;
	tbb::atomic<size_t> failedTransactionCount{0};
	parallel_for(blocked_range<size_t>(0, sizeInTuples, grainSize),
							 [&output, &failureHistogram, transactionSize, probelength, &conflicts, &conflictCount, &input,
								&conflictsRanges, &failedTransactionCount](auto range) {
								 for(size_t j = range.begin(); j < range.end(); j += transactionSize) {
									 auto status = _xbegin();
									 if(status == _XBEGIN_STARTED) {
										 for(size_t i = j; i < j + transactionSize; i++) {
											 int offset = 0;
											 while(output[input[i]] && offset < probelength)
												 offset++;
											 if(offset < probelength)
												 output[input[i] + offset] = input[i];
											 else
												 conflicts[conflictCount++] = input[i];
										 }
										 _xend();
									 } else {
										 conflictsRanges[failedTransactionCount++] = j;
									 }
								 }
							 });
	gettimeofday(&after, NULL);
	std::cout << ", \"hashBuildTimeInMicroseconds\": "
						<< (after.tv_sec * 1000000 + after.tv_usec) -
									 (before.tv_sec * 1000000 + before.tv_usec);

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

	auto conflictSum =
			parallel_deterministic_reduce(blocked_range<size_t>(0, conflictCount, 16), 0ul,
																		[&conflicts](auto range, auto init) {
																			for(size_t i = range.begin(); i < range.end(); i++) {
																				init += conflicts[i];
																			}
																			return init;
																		},
																		[](auto a, auto b) { return a + b; });
	cout << ", "
			 << "\"conflicts\": " << conflictCount;

	cout << ", "
			 << "\"failedTransaction\": " << failedTransactionCount;

	cout << ", "
			 << "\"sum\": " << sum + conflictSum + failedTransactionSum;

	cout << "}" << endl;

	return 0;
}
