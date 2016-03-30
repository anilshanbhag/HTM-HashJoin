#include <iostream>
#include <memory>
#include <tbb/tbb.h>
#include <cmath>
#include <sys/time.h>
#include <atomic>
#include <getopt.h>

#include "config.h"
#include "AtomicHashBuild.hpp"
#include "HTMAdaptive.hpp"
#include "NoCCHashBuild.hpp"
#include "include/DataGen.hpp"

using namespace std;
using namespace tbb;

struct param_t {
  // Algorithm to use: atomic, nocc, htm.
  string algo;

  uint32_t rSize;
  uint32_t transactionSize;
  uint32_t probeLength;

  // Data Distribution: uniform, sorted, shuffle.
  string dataDistr;

  // Used only by the shuffle.
  uint32_t shuffleRange;

  // Output hashtable is scaleOutput * inputSize.
  // Make sure this is multiple of 2.
  uint32_t scaleOutput;

  // Number of partitions to create of input.
  uint32_t numPartitions;
};

void
parseArgs(int argc, char ** argv, param_t * cmdParams) {
  // TODO: Make this clean using getopt.
  for (int i=1; i<argc; i++) {
    if (strcmp(argv[i], "--algo") == 0) {
      cmdParams->algo = argv[i+1];
    } else if (strcmp(argv[i], "--rSize") == 0) {
      cmdParams->rSize = atoi(argv[i+1]);
    } else if (strcmp(argv[i], "--transactionSize") == 0) {
      cmdParams->transactionSize = atoi(argv[i+1]);
    } else if (strcmp(argv[i], "--probeLength") == 0) {
      cmdParams->dataDistr = atoi(argv[i+1]);
    } else if (strcmp(argv[i], "--dataDistr") == 0) {
      cmdParams->dataDistr = argv[i+1];
    } else if (strcmp(argv[i], "--shuffleRange") == 0) {
      cmdParams->shuffleRange = atoi(argv[i+1]);
    }  else if (strcmp(argv[i], "--scaleOutput") == 0) {
      cmdParams->scaleOutput = atoi(argv[i+1]);
    }  else if (strcmp(argv[i], "--numPartitions") == 0) {
      cmdParams->numPartitions = atoi(argv[i+1]);
    } else {
      std::cout<<"Found Unknown Arg: "<<argv[i]<<std::endl;
      exit(1);
    }

    // Since we processed the arg.
    i++;
  }
}

int
main(int argc, char* argv[]) {
  /* Command line parameters */
  param_t cmdParams;

  cmdParams.algo = "htm";
  cmdParams.rSize = 1<<28;
  cmdParams.probeLength = 4;
  cmdParams.dataDistr = "shuffle";
  cmdParams.transactionSize = 16;
  cmdParams.scaleOutput = 2;
  cmdParams.numPartitions = 64;
  cmdParams.shuffleRange = 16;

  parseArgs(argc, argv, &cmdParams);

#if ENABLE_PROBE
  uint32_t* relR = generate_data(cmdParams.dataDistr, cmdParams.rSize, cmdParams.rSize, cmdParams.shuffleRange);
  uint32_t* relS = generate_data("sorted", cmdParams.rSize, cmdParams.rSize, cmdParams.shuffleRange);

  if (cmdParams.algo == "atomic")
    AtomicHashBuild(relR, cmdParams.rSize, relS, cmdParams.rSize, cmdParams.scaleOutput, cmdParams.numPartitions, cmdParams.probeLength);
  else if (cmdParams.algo == "htm")
    HTMHashBuild(relR, cmdParams.rSize, relS, cmdParams.rSize, cmdParams.transactionSize, cmdParams.scaleOutput, cmdParams.numPartitions, cmdParams.probeLength);
  else if (cmdParams.algo == "nocc")
    NoCCHashBuild(relR, cmdParams.rSize, relS, cmdParams.rSize, cmdParams.scaleOutput, cmdParams.numPartitions, cmdParams.probeLength);
  else
    cout<<"Unknown Algo: "<<cmdParams.algo<<endl;

  free(relR);
  free(relS);
#else
  uint32_t* relR = generate_data(cmdParams.dataDistr, cmdParams.rSize, cmdParams.rSize, cmdParams.shuffleRange);

  if (cmdParams.algo == "atomic")
    AtomicHashBuild(relR, cmdParams.rSize, cmdParams.scaleOutput, cmdParams.numPartitions, cmdParams.probeLength);
  else if (cmdParams.algo == "htm")
    HTMHashBuild(relR, cmdParams.rSize, cmdParams.transactionSize, cmdParams.scaleOutput, cmdParams.numPartitions, cmdParams.probeLength);
  else if (cmdParams.algo == "nocc")
    NoCCHashBuild(relR, cmdParams.rSize, cmdParams.scaleOutput, cmdParams.numPartitions, cmdParams.probeLength);
  else
    cout<<"Unknown Algo: "<<cmdParams.algo<<endl;

  free(relR);
#endif

  return 0;
}
