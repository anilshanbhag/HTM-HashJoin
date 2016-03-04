#include <iostream>
#include <rtm.h>
#include <tbb/tbb.h>
#include <memory>
using namespace std;
using namespace tbb;
int main(int argc, char* argv[]) {
	if(argc < 3) {
		cout << "usage: HTMHashBuild $sizeInTuples $transactionSize $probelength" << endl;
		exit(1);
	}
	const size_t sizeInTuples = atol(argv[1]);
	const size_t transactionSize = atol(argv[2]);
	const size_t probelength = atol(argv[3]);
	auto input = std::make_unique<int[]>(sizeInTuples);
	auto output = std::make_unique<int[]>(sizeInTuples);
	auto outliers = std::make_unique<int[]>(sizeInTuples);
	auto outliersRanges = std::make_unique<int[]>(sizeInTuples);

	parallel_for(blocked_range<size_t>(0, sizeInTuples, sizeInTuples / 64),
							 [&input, &output, sizeInTuples](auto range) {
								 unsigned int seed = clock() * range.begin();
								 for(size_t i = range.begin(); i < range.end(); i++) {
									 input[i] = (rand_r(&seed) % sizeInTuples) + 1;
									 output[0] = 0;
								 }
							 });

	size_t outlierCount = 0;
	size_t outlierRangeCount = 0;
	parallel_for(blocked_range<size_t>(0, sizeInTuples, transactionSize),
							 [&output, probelength, &outliers, &outlierCount, &input, &outliersRanges,
								&outlierRangeCount](auto range) {
								 if(_xbegin() == _XBEGIN_STARTED) {
									 for(size_t i = range.begin(); i < range.end(); i++) {
										 int offset = 0;
										 while(output[input[i]] && offset < probelength)
											 offset++;
										 if(offset < probelength)
											 output[input[i] + offset] = input[i];
										 else
											 outliers[outlierCount++] = input[i];

										 _xend();
									 }
								 } else {
									 outliersRanges[outlierRangeCount++] = range.begin();
								 }
							 });

	cout << outlierCount << ": " << outlierRangeCount << endl;
	return 0;
}
