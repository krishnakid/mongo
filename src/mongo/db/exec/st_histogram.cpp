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

#include "st_histogram.h"

#include <stdlib.h>
#include <stdio.h>
#include <utility>
#include <sstream>

#include "mongo/db/exec/st_histogram_binrun.h"
#include "mongo/db/query/qlog.h"

namespace mongo { 
    const double StHistogram::kAlpha = 0.5;                // universal damping term
    const double StHistogram::kMergeThreshold = 0.00025;   // merge threshold parameter
    const double StHistogram::kSplitThreshold = 0.1;       // split threshold parameter
    const int StHistogram::kMergeInterval = 200;        // merge interval paramter
    
    // StHistogram constructor
    StHistogram::StHistogram(int size, double binInit, double lowBound, double highBound) {
        nBuckets = size;
        freqs = new double[nBuckets];    
        bounds = new Bounds[nBuckets];
        nObs = 0;
        
        // if the bounds are -Inf to Inf, then we want equal binning density to the 
        
        // initialize
        double curStart = lowBound;
        double stepSize = (highBound/nBuckets) - (lowBound / nBuckets); 

        for (int i = 0; i < nBuckets - 1; i++) {
            freqs[i] = binInit;
            bounds[i].first = curStart;
            bounds[i].second = curStart + stepSize;
            curStart += stepSize;
        }
        freqs[nBuckets - 1] = binInit;
        bounds[nBuckets - 1].first = curStart;
        bounds[nBuckets - 1].second = highBound;
    }

    // StHistogram destructor
    StHistogram::~StHistogram() {
        delete[] freqs;
        delete[] bounds;
    }

    bool StHistogram::rangeBoundOrderingFunction(const StHistogramRun& run1,
                                                 const StHistogramRun& run2) {
        return run1.getRangeBounds().first < run2.getRangeBounds().first;
    }

    bool StHistogram::splitOrderingFunction(const StHistogramRun& run1, 
                                            const StHistogramRun& run2) {
        if (run1.isMerged() && run2.isMerged()) {
            return run1.getTotalFreq() > run2.getTotalFreq();
        } 
        else if (run1.isMerged()) {
            return false;
        } 
        else if (run2.isMerged()) {
            return true;
        } 
        else {
            return run1.getTotalFreq() > run2.getTotalFreq();
        }
    }

    // recalibrates histogram based on batch input
    void StHistogram::update(const StHistogramUpdateParams& data) {
        nObs++;
        if ((nObs % kMergeInterval) == (kMergeInterval - 1)) {
            restructure(); 
        }
        // estimate the result size of the selection using current data
        double est = 0;
        bool doesIntersect [nBuckets];
        double minIntersect, maxIntersect;

        for (int i = 0; i < nBuckets; i++) {
            minIntersect = std::max(data.start, bounds[i].first);
            maxIntersect = std::min(data.end, bounds[i].second);
           
            double intersectFrac = std::max((maxIntersect - minIntersect) /
                         (bounds[i].second - bounds[i].first), 0.0);
        
            doesIntersect[i] = intersectFrac > 0;

            est += freqs[i] * intersectFrac;
        }

        // compute absolute estimation error
        double esterr = data.nReturned - est;              // error term
        
        // distribute error amongst buckets in proportion to frequency
        for (int i = 0; i < nBuckets; i++) {     
            minIntersect = std::max(data.start, bounds[i].first);
            maxIntersect = std::min(data.end, bounds[i].second);
            double frac = (maxIntersect - minIntersect + 1) /
                          (bounds[i].second - bounds[i].first + 1);
            if (doesIntersect[i]) {
                freqs[i] = std::max(0.0, freqs[i] + (frac * kAlpha * esterr * freqs[i] /(est)));
            }
        }

    }

    void StHistogram::merge(std::list<StHistogramRun>& runs, std::list<StHistogramRun>& reclaimed) {
        double totalFreq = 0;

        for(int i = 0; i < nBuckets; i++) {
            totalFreq += freqs[i];
            StHistogramRun nr (i, freqs[i], Bounds(bounds[i].first, bounds[i].second));
            runs.push_back(nr); 
        }
        
        // for every two consercuive runs of buckets, find the maximum
        // difference in frequency between a bucket in the first run and
        // a bucket in the second run
        bool mergeComplete = false;

        while (!mergeComplete) { //  && reclaimed.size() + 1 < runs.size() 
            mergeComplete = true;
            MergePair best;
            double minDiff = std::numeric_limits<double>::infinity();
            
            StHistogramRunIter curRun = runs.begin();
            size_t nUnmerged = 0;

            for (StHistogramRunIter i = ++(runs.begin()); i != runs.end(); i++) {
                double cDiff = curRun->getMaxDiff(*i); 
               
                if (cDiff < minDiff) { 
                    best.first = curRun;
                    best.second = i;
                    minDiff = cDiff;
                }

                if (!(curRun->isMerged())) {
                    nUnmerged++;
                }

                curRun = i;
            }

            // ensures that not too many buckets are split.  This is a departure from the algorithm
            // in (Aboulnaga, Chaudhuri)
            if (nUnmerged <= reclaimed.size()) {
                break;                                   // otherwise merged buckets will be split
            }

            if (minDiff < (kMergeThreshold * totalFreq)) {
                best.first->merge(*(best.second));          // merge
                reclaimed.push_back(*(best.second));        // reclaim Run
                runs.erase(best.second);
                mergeComplete = false;
            }
            // and repeat.
        }
    }

    void StHistogram::split(std::list<StHistogramRun>& runs, std::list<StHistogramRun>& reclaimed) {
        size_t nToSplit = nBuckets * kSplitThreshold;

        std::list<StHistogramRun> candidates;
        for (StHistogramRunIter i = runs.begin(); 
            candidates.size() <= nToSplit && i != runs.end(); i++) {
            
            candidates.push_front(*i);          // for reverse access later
            i = runs.erase(i);
            i--;
        }

        double fullFreq = 0;
        for(StHistogramRunIter i = candidates.begin(); i != candidates.end(); i++) {
            fullFreq += i->getTotalFreq();
        }

        // need to ensure that sum is full range
        int totalReclaimed = reclaimed.size();
        size_t candidatesProcessed = 0;

        for (StHistogramRunIter i = candidates.begin(); i != candidates.end(); i++) {
            int nAlloc;
            if (candidatesProcessed < candidates.size() - 1) { 
                nAlloc = totalReclaimed * i->getTotalFreq() / fullFreq;
            } 
            else { 
                nAlloc = reclaimed.size();  // ensure full split.
            }

            std::list<StHistogramRun> updateRuns;
            for (int i = 0; i < nAlloc; i++) {
                updateRuns.push_back(*(reclaimed.begin()));
                reclaimed.erase(reclaimed.begin());
            }
            i->split(updateRuns);           // merge step

            // merge back onto candidates
            runs.push_back(*i);
            runs.splice(runs.end(), updateRuns);

            candidatesProcessed++;
        } 

    }



    void StHistogram::restructure() {
        // initialize B runs of buckets
        std::list<StHistogramRun> runs;                 // original runs
        std::list<StHistogramRun> reclaimed;            // runs reclaimed

        // start algorithm
        merge(runs, reclaimed);                                    // merge runs
        runs.sort(splitOrderingFunction);               // order to split
        split(runs, reclaimed);                                    // split runs
        runs.sort(rangeBoundOrderingFunction);          // order to map

        // map onto memory allocated for original histogram
        int counter = 0;
        for (StHistogramRunIter i = runs.begin(); i != runs.end(); i++) { 
            freqs[counter] = i->getTotalFreq();
            bounds[counter].first = i->getRangeBounds().first;
            bounds[counter].second = i->getRangeBounds().second;
            counter++;
        }
    }

    double StHistogram::getFreqOnRange(Bounds& range) {
        double freq = 0;
        for (int i = 0; i < nBuckets; i++) {
            double overlap = std::min(range.second, bounds[i].second) -
                             std::max(range.first, bounds[i].first);
            overlap = std::max(overlap / (bounds[i].second-bounds[i].first), 0.0);
            freq += overlap * freqs[i];
        }
        return freq;
    }

    std::string StHistogram::toString() const {
        std::ostringstream s;
        for(int i = 0; i < nBuckets; i++) {
            s << bounds[i].first << ","
              << bounds[i].second << ","
              << freqs[i] << std::endl;
        }
        return s.str();
    }

    std::ostream& operator<<(std::ostream &strm, const StHistogram &hist) {
        std::ostream& retStrm = strm;
        for(int i = 0; i < hist.nBuckets; i++) {
            retStrm << hist.bounds[i].first << "," 
                    << hist.bounds[i].second << ","
                    << hist.freqs[i] << std::endl;
        }
        return retStrm;
    }
}
