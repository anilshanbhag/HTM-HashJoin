#pragma once

#include <tbb/tbb.h>
#include <algorithm>
#include <string>
#include <random>
using namespace std;
using namespace tbb;

//----- Constants -----------------------------------------------------------
#define  FALSE          0       // Boolean false
#define  TRUE           1       // Boolean true

inline uint32_t murmur( uint32_t h )
{
  h ^= h >> 16;
  h *= 0x85ebca6b;
  h ^= h >> 13;
  h *= 0xc2b2ae35;
  h ^= h >> 16;

  return h;
}

// local_shuffle_range used only by local_shuffle
uint64_t* generate_data(string dist, uint32_t size_in_tuples, uint32_t distinct_keys, int local_shuffle_range = 16) {
  srand(0);
  uint32_t mod_mask = distinct_keys - 1;
  uint64_t* input = new uint64_t[size_in_tuples];
  if (dist == "uniform") {
    /*parallel_for(blocked_range<size_t>(0, size_in_tuples,
        size_in_tuples / 64),
        [&input, size_in_tuples, mod_mask](auto range) {
      uint32_t seed = range.begin() + 1;
      // std::cout<<"Seed "<<seed<<std::endl;
      for(size_t i = range.begin(); i < range.end(); i++) {
        input[i] = (rand_r(&seed) & mod_mask) + 1;
      }
    });*/
    for (int i=0; i<size_in_tuples; i++) {
      input[i] = (rand() & mod_mask) + 1;
    }
    sort(input, input + size_in_tuples);
    bool* shuffled = new bool[size_in_tuples]{false};

    for(size_t i = 0; i < size_in_tuples - 1; i++) {
      if(!shuffled[i]) {
        int swap = rand() % min(local_shuffle_range, (int)(size_in_tuples - i));
        uint64_t temp = input[i];
        input[i] = input[(i + swap)];
        input[(i + swap)] = temp;
        shuffled[(i + swap)] = true;
      }
    }
  } else if (dist == "random") {
    for (int i=0; i<size_in_tuples; i++) {
      input[i] = rand();
      while (input[i] == 0) input[i] = rand();
    }
    sort(input, input + size_in_tuples);
    bool* shuffled = new bool[size_in_tuples]{false};

    for(size_t i = 0; i < size_in_tuples - 1; i++) {
      if(!shuffled[i]) {
        int swap = rand() % min(local_shuffle_range, (int)(size_in_tuples - i));
        uint64_t temp = input[i];
        input[i] = input[(i + swap)];
        input[(i + swap)] = temp;
        shuffled[(i + swap)] = true;
      }
    }
  } else if (dist == "zipf") {
/*    rand_val(1);*/
    //for (i=0; i<size_in_tuples; i++)
    //{
      //keys[i] = zipf(alpha, n);
    /*}*/
  } else if (dist == "sorted") {
    parallel_for(blocked_range<size_t>(0, size_in_tuples,
        std::llround(std::ceil(size_in_tuples / 64.0))),
        [&input, size_in_tuples](auto range) {
      for(size_t i = range.begin(); i < range.end(); i++) {
        input[i] = i + 1;
      }
    });
  } else if (dist == "shuffle") {
    parallel_for(blocked_range<size_t>(0, size_in_tuples,
        std::llround(std::ceil(size_in_tuples / 64.0))),
        [&input, size_in_tuples](auto range) {
      for(size_t i = range.begin(); i < range.end(); i++) {
        input[i] = i + 1;
      }
    });

    random_shuffle(&input[0], &input[size_in_tuples]);
  } else if (dist == "local_shuffle") {
    bool* shuffled = new bool[size_in_tuples]{false};

    parallel_for(
        blocked_range<size_t>(0, size_in_tuples, std::llround(std::ceil(size_in_tuples / 64.0))),
        [&input, size_in_tuples](auto range) {
          for(size_t i = range.begin(); i < range.end(); i++) {
            input[i] = i + 1;
          }
        });

    for(size_t i = 0; i < size_in_tuples - 1; i++) {
      if(!shuffled[i]) {
        int swap = rand() % min(local_shuffle_range, (int)(size_in_tuples - i));
        uint64_t temp = input[i];
        input[i] = input[(i + swap)];
        input[(i + swap)] = temp;
        shuffled[(i + swap)] = true;
      }
    }
  } else {
    cout<<"Unknown distribution"<<endl;
    exit(1);
  }

  return input;
}

/*//===========================================================================*/
////=  Function to generate Zipf (power law) distributed random variables     =
////=    - Input: alpha and N                                                 =
////=    - Output: Returns with Zipf distributed random variable              =
////=  Source: http://www.csee.usf.edu/~christen/tools/genzipf.c
////===========================================================================
//int zipf(double alpha, int n)
//{
  //static int first = TRUE;      // Static first time flag
  //static double c = 0;          // Normalization constant
  //double z;                     // Uniform random number (0 < z < 1)
  //double sum_prob;              // Sum of probabilities
  //double zipf_value;            // Computed exponential value to be returned
  //int    i;                     // Loop counter

  //// Compute normalization constant on first call only
  //if (first == TRUE)
  //{
    //for (i=1; i<=n; i++)
      //c = c + (1.0 / pow((double) i, alpha));
    //c = 1.0 / c;
    //first = FALSE;
  //}

  //// Pull a uniform random number (0 < z < 1)
  //do
  //{
    //z = rand_val(0);
  //}
  //while ((z == 0) || (z == 1));

  //// Map z to the value
  //sum_prob = 0;
  //for (i=1; i<=n; i++)
  //{
    //sum_prob = sum_prob + c / pow((double) i, alpha);
    //if (sum_prob >= z)
    //{
      //zipf_value = i;
      //break;
    //}
  //}

  //// Assert that zipf_value is between 1 and N
  //assert((zipf_value >=1) && (zipf_value <= n));

  //return(zipf_value);
//}

////=========================================================================
////= Multiplicative LCG for generating uniform(0.0, 1.0) random numbers    =
////=   - x_n = 7^5*x_(n-1)mod(2^31 - 1)                                    =
////=   - With x seeded to 1 the 10000th x value should be 1043618065       =
////=   - From R. Jain, "The Art of Computer Systems Performance Analysis," =
////=     John Wiley & Sons, 1991. (Page 443, Figure 26.2)                  =
////=========================================================================
//double rand_val(int seed)
//{
  //const long  a =      16807;  // Multiplier
  //const long  m = 2147483647;  // Modulus
  //const long  q =     127773;  // m div a
  //const long  r =       2836;  // m mod a
  //static long x;               // Random int value
  //long        x_div_q;         // x divided by q
  //long        x_mod_q;         // x modulo q
  //long        x_new;           // New x value

  //// Set the seed if argument is non-zero and then return zero
  //if (seed > 0)
  //{
    //x = seed;
    //return(0.0);
  //}

  //// RNG using integer arithmetic
  //x_div_q = x / q;
  //x_mod_q = x % q;
  //x_new = (a * x_mod_q) - (r * x_div_q);
  //if (x_new > 0)
    //x = x_new;
  //else
    //x = x_new + m;

  //// Return a random value between 0.0 and 1.0
  //return((double) x / m);
/*}*/
