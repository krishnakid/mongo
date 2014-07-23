/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#pragma once

#include <iostream> 
#include <utility>

#include "mongo/db/exec/st_histogram_binrun.h"
#include "mongo/db/query/st_histogram_cache.h"

namespace mongo {

    struct StHistogramUpdateParams;
    typedef std::list<StHistogramRun>::iterator StHistogramRunIter;
    typedef std::pair<StHistogramRunIter, StHistogramRunIter> MergePair;

  
    // define StHistogram class wrapper
    class StHistogram {
    public:
        StHistogram(int size, double binInit, double lowBound, double highBound);
        ~StHistogram();
        const static double kAlpha;                // universal damping term
        const static double kMergeThreshold;       // merge threshold parameter
        const static double kSplitThreshold;       // split threshold parameter
        const static int kMergeInterval;           // merge interval parameter

        int nBuckets;                              // number of buckets in the histogram
        int nObs;                                  // number of observations seen
        double* freqs;                             // array of value estimations for histogram
        Bounds* bounds;                            // array of range bounds

        /* map an arbitrary BSONElement onto a double while weakly preserving the order
         * defined by woCompare() */
        static double mapBSONElementToDouble(const BSONElement&);

        /* update the histogram with the (lowBound, highBound, nReturned) information */
        void update(const StHistogramUpdateParams&);

        /* restructure the histogram to achieve higher granularity on high-frequency bins */
        void restructure();

        /* request histogram estimate for a given range bound */
        double getFreqOnRange(const IndexBounds&);

        /* DEBUG : for pretty printing */
        std::string toString() const;
    private:
        /* function used to order runs by range bound for histogram update */
        static bool rangeBoundOrderingFunction(const StHistogramRun&, const StHistogramRun&);

        /* function used to order runs during split ordering */
        static bool splitOrderingFunction(const StHistogramRun&, const StHistogramRun&);

        /* update step restricted to a single interval */
        void updateOne(BSONProjection start, BSONProjection end, size_t nReturned);

        /* frequency estimation step restricted to a single interval */
        double getFreqOnOneRange(BSONProjection start, BSONProjection end);

        /* merge portion of histogram restructuring */
        void merge(std::list<StHistogramRun>&, std::list<StHistogramRun>&);
        
        /* split portion of histogram restructuring */
        void split(std::list<StHistogramRun>&, std::list<StHistogramRun>&);

        /* get initial index containing search term */
        int getStartIdx(BSONProjection);
    };

    // histogram pretty print overloading
    std::ostream& operator<<(std::ostream &strm, const StHistogram &hist);
}
