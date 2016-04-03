#include "SortMerge.hpp"
#include <tbb/tbb.h>
#include "cpp-sort/sort.h"
#include "cpp-sort/sorters.h"
using namespace tbb;
using namespace std;
void SortMerge(uint32_t* build, uint32_t buildSize, uint32_t* probe, uint32_t probseSize,
							 uint32_t numPartitions) {
	struct timeval beforeSort, afterSort;
	auto partitions = 64;
	gettimeofday(&beforeSort, NULL);
	parallel_for(blocked_range<size_t>(0, buildSize, buildSize / partitions), [build](auto range) {
		cppsort::sort(build + range.begin(), build + range.end(), cppsort::tim_sort);
	});
	gettimeofday(&afterSort, NULL);

	cppsort::sort(build, build + buildSize, cppsort::tim_sort);

	struct timeval beforeMerge, afterMerge;
	gettimeofday(&beforeMerge, NULL);
	auto totalMatches = parallel_deterministic_reduce(
			blocked_range<size_t>(0, buildSize, buildSize / partitions), 0ul,
			[build, probe](auto range, auto init) {
				size_t leftI = range.begin();
				size_t rightI = range.begin();
				while(leftI < range.end() && rightI < range.end()) {
					auto left = build[leftI];
					auto right = probe[rightI];
					init += left == right;
					leftI += left <= right;
					rightI += left >= right;
				}
				return init;
			},
			[](auto a, auto b) { return a + b; });

	gettimeofday(&afterMerge, NULL);

	auto inputSum =
			parallel_deterministic_reduce(blocked_range<size_t>(0, buildSize, 1024), 0ul,
																		[&build](auto range, auto init) {
																			for(size_t i = range.begin(); i < range.end(); i++) {
																				init += build[i];
																			}
																			return init;
																		},
																		[](auto a, auto b) { return a + b; });

	cout << "{"
			 << "\"algo\": \"nocc\"";
	cout << ","
			 << "\"rSize\": " << buildSize;
	cout << ", \"hashBuildTimeInMicroseconds\": "
			 << (afterSort.tv_sec * 1000000 + afterSort.tv_usec) -
							(beforeSort.tv_sec * 1000000 + beforeSort.tv_usec) +
							(afterMerge.tv_sec * 1000000 + afterMerge.tv_usec) -
							(beforeMerge.tv_sec * 1000000 + beforeMerge.tv_usec);
	cout << ", \"sortTimeInMicroseconds\": "
			 << (afterSort.tv_sec * 1000000 + afterSort.tv_usec) -
							(beforeSort.tv_sec * 1000000 + beforeSort.tv_usec);
	cout << ", \"mergeTimeInMicroseconds\": "
			 << (afterMerge.tv_sec * 1000000 + afterMerge.tv_usec) -
							(beforeMerge.tv_sec * 1000000 + beforeMerge.tv_usec);
	cout << ", "
			 << "\"totalMatches\": " << totalMatches;
	cout << ", "
			 << "\"inputSum\": " << inputSum;
	cout << "}" << endl;
}
