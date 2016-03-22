/**
 * @file    parallel_radix_join.h
 * @author  Cagri Balkesen <cagri.balkesen@inf.ethz.ch>
 * @date    Sun Feb 20:09:59 2012
 * @version $Id: parallel_radix_join.h 3017 2012-12-07 10:56:20Z bcagri $
 *
 * @brief  Provides interfaces for several variants of Radix Hash Join.
 *
 * (c) 2012, ETH Zurich, Systems Group
 *
 */

#ifndef PARALLEL_RADIX_JOIN_H
#define PARALLEL_RADIX_JOIN_H

#include "types.h" /* relation_t */

/**
 * PRO: Parallel Radix Join Optimized.
 *
 * The "Parallel Radix Join Optimized" implementation denoted as PRO implements
 * the parallel radix join idea by Kim et al. with further optimizations. Mainly
 * it uses the bucket chaining for the build phase instead of the
 * histogram-based relation re-ordering and performs better than other
 * variations such as PRHO, which applies SIMD and prefetching
 * optimizations.

 * @param relR  input relation R - inner relation
 * @param relS  input relation S - inner relation
 *
 * @return number of result tuples
 */
int64_t
PRO(relation_t * relR, relation_t * relS, int nthreads);

#endif /* PARALLEL_RADIX_JOIN_H */
