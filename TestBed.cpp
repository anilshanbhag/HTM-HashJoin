#include <cstdlib>
#include <sys/time.h>
#include <iostream>
#include <cstring>
#include <cstdio>
#include <tbb/tbb.h>
using namespace std;
using namespace tbb;

int main() {
  int len = 1<<27;
  int* a = new int[len]{1};
  int *b = new int[len]{0};

  struct timeval before, after;
  gettimeofday(&before, NULL);

  // memcpy(b, a, len*4);
  parallel_for(blocked_range<size_t>(0, len, len/8),
               [a,b](const auto range) {
       //for(size_t j = range.begin(); j < range.end(); j += 1)
       // b[j] = a[j];
       int j = range.begin(); int r = range.end() - range.begin();
       memcpy(&b[j], &a[j], r*4);
  });
  // for (int i=0; i<len; i++) b[i] = a[i];

  gettimeofday(&after, NULL);

  cout<<b[0]<<b[1]<<endl;

  cout << "totalTime: "
       << (after.tv_sec * 1000000 + after.tv_usec) -
               (before.tv_sec * 1000000 + before.tv_usec);


  return 0;
}
