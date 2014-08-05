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

#include "mongo/db/exec/st_histogram.h"

#include <stdlib.h>
#include <stdio.h>
#include <utility>
#include <sstream>
#include <cmath>

#include "mongo/db/query/qlog.h"

namespace mongo { 
    // for details see "Self-tuning Histograms: Building Histograms
    // Without Looking at Data.
    const double StHistogram::kAlpha = 0.5;                 // universal damping term
    const double StHistogram::kMergeThreshold = 0.00025;    // merge threshold parameter
    const double StHistogram::kSplitThreshold = 0.1;        // split threshold parameter
    const int StHistogram::kMergeInterval = 200;            // merge interval paramter

    // StHistogram constructor
    //
    // might have to get rid of the double lowBound / highBound issue for now
    // change one part at a time.
    StHistogram::StHistogram(int size, double binInit, double lowBound, double highBound):
                _nBuckets(size),
                _nObs(0),
                _totalFreq(binInit*size),
                _freqs(new double[_nBuckets]), 
                _bounds(new Bounds[_nBuckets]) {
        
        int NUMBER_DOUBLE_TYPE = 1;     // being short circuited here for testing purposes.
        int NUMBER_DOUBLE_CANON_TYPE = 10;

        // initialize
        double curStart = lowBound;
        double stepSize = (highBound - lowBound) / _nBuckets; 

        for (int i = 0; i < _nBuckets - 1; i++) {
            BSONProjection bstart(NUMBER_DOUBLE_CANON_TYPE, NUMBER_DOUBLE_TYPE, curStart);
            BSONProjection bend(NUMBER_DOUBLE_CANON_TYPE, NUMBER_DOUBLE_TYPE, curStart+stepSize);

            _freqs[i] = binInit;
            _bounds[i].first = bstart;
            _bounds[i].second = bend;
            curStart += stepSize;
        }
        
        BSONProjection bstart(NUMBER_DOUBLE_CANON_TYPE, NUMBER_DOUBLE_TYPE, curStart);
        BSONProjection bend(NUMBER_DOUBLE_CANON_TYPE, NUMBER_DOUBLE_TYPE, highBound);

        _freqs[_nBuckets - 1] = binInit;
        _bounds[_nBuckets - 1].first = bstart;
        _bounds[_nBuckets - 1].second = bend;
    }

    // StHistogram destructor
    StHistogram::~StHistogram() {}

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
        typedef std::vector<OrderedIntervalList>::iterator KeyFieldIter;
        typedef std::vector<Interval>::iterator IntervalIter;

        _nObs++;
        if ((_nObs % kMergeInterval) == (kMergeInterval - 1)) {
            restructure(); 
        }
        // parse through the IntervalBounds code and send off all assignments

        // one OrderedIntervalList per field in the index key shape
        std::vector<OrderedIntervalList> fields = data.bounds->fields; 
        for (KeyFieldIter i = fields.begin(); i != fields.end(); ++i) {
            // TODO: this would be the input platform for the multidimensional extension. 
            
            std::vector<Interval> intervals = i->intervals;
            for (IntervalIter j = intervals.begin(); j != intervals.end(); ++j) {
                BSONProjection start(j->start), end(j->end);
                updateOne(start, end, data.nReturned/intervals.size());
            }
            break;
        }
    }

    void StHistogram::updateOne(BSONProjection start, 
                                BSONProjection end, 
                                size_t nReturned) {
        // estimate the result size of the selection using current data
        double est = 0;
        bool doesIntersect [_nBuckets];

        int startIdx = getStartIdx(start);
        if (startIdx == -1) {
            return;                // not in bounds
        }

        for (int i = startIdx; i < _nBuckets; i++) {
            BSONProjection minIntersect = std::max<BSONProjection>(start, _bounds[i].first);
            BSONProjection maxIntersect = std::min<BSONProjection>(end, _bounds[i].second);
            
            double intervalWidth = maxIntersect - minIntersect;
            if (std::isinf(intervalWidth))  {
                doesIntersect[i] = false; 
                break;           // crossing type boundary
            }

            double intersectFrac = std::max<double>(intervalWidth /
                         (_bounds[i].second - _bounds[i].first), 0.0);
        
            if (!(doesIntersect[i] = (intersectFrac > 0)))  break;

            est += _freqs[i] * intersectFrac;
        }

        // compute absolute estimation error
        double esterr = nReturned - est;              // error term
        
        // distribute error amongst buckets in proportion to frequency
        for (int i = startIdx; i < _nBuckets; i++) {     
            if (doesIntersect[i]) {
                BSONProjection minIntersect = std::max<BSONProjection>(start, _bounds[i].first);
                BSONProjection maxIntersect = std::min<BSONProjection>(end, _bounds[i].second);
                
                double frac = (maxIntersect - minIntersect + 1) /
                              (_bounds[i].second - _bounds[i].first + 1);

                double newFreq = std::max<double>(0.0, _freqs[i] + 
                                (frac * kAlpha * esterr * _freqs[i] /(est)));
                double delta = newFreq - _freqs[i]; 
                _totalFreq += delta;
                _freqs[i] = newFreq;
            }
            else {
                break;
            }
        }

    }

    int StHistogram::getStartIdx(BSONProjection val) { 
        // binary search for start point
        int blo = 0, bhi = _nBuckets, probe; 
        int startIdx = -1;

        while (blo <= bhi) {
            probe = (blo + bhi) / 2;
            if (val >= _bounds[probe].first) {
                if (val < _bounds[probe].second) {
                    startIdx = probe;
                    break;
                }
                else {
                    blo = probe + 1;
                }
            }
            else {
                bhi = probe - 1;
            }
        }
        return startIdx;
    }

    void StHistogram::merge(std::list<StHistogramRun>& runs, std::list<StHistogramRun>& reclaimed) {
        double totalFreq = 0;

        for(int i = 0; i < _nBuckets; i++) {
            totalFreq += _freqs[i];
            StHistogramRun nr (i, _freqs[i], Bounds(_bounds[i].first, _bounds[i].second));
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
        size_t nToSplit = _nBuckets * kSplitThreshold;

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
            _freqs[counter] = i->getTotalFreq();
            _bounds[counter].first = i->getRangeBounds().first;
            _bounds[counter].second = i->getRangeBounds().second;
            counter++;
        }
    }

    double StHistogram::getFreqOnRange(const IndexBounds& bounds) { 
        typedef std::vector<OrderedIntervalList>::iterator KeyFieldIter;
        typedef std::vector<Interval>::iterator IntervalIter;

        double agg = 0;
        // one OrderedIntervalList per field in the index key shape
        std::vector<OrderedIntervalList> fields = bounds.fields; 
        for (KeyFieldIter i = fields.begin(); i != fields.end(); ++i) {
            // TODO: this would be the input platform for the multidimensional extension. 
            
            std::vector<Interval> intervals = i->intervals;
            for (IntervalIter j = intervals.begin(); j != intervals.end(); ++j) {
                BSONProjection start(j->start), end(j->end);
                agg += getFreqOnOneRange(start, end);
            }
            break;
        }
        return agg;
    }

    double StHistogram::getTotalFreq() { 
/*
        double agg = 0;
        for (int ix = 0; ix < _nBuckets; ++ix) {
            agg += _freqs[ix];
        }
        return agg;
*/ 
        return _totalFreq;
    }

    // returns 0 for intervals spanning multiple type classifications
    double StHistogram::getFreqOnOneRange(BSONProjection start, BSONProjection end) {
        double freq = 0;
        int startIdx = getStartIdx(start);

        for (int i = startIdx; i < _nBuckets; i++) {
            double overlap = std::min(end, _bounds[i].second) -
                             std::max(start, _bounds[i].first);
            if (std::isinf(overlap)) return 0;
            
            overlap = std::max(overlap / (_bounds[i].second-_bounds[i].first), 0.0);
            freq += overlap * _freqs[i];
            if (overlap == 0) break;
        }
        return freq;
    }

    std::string StHistogram::toString() const {
        std::ostringstream s;
        for(int i = 0; i < _nBuckets; i++) {
            s << _bounds[i].first.data << ","
              << _bounds[i].second.data << ","
              << _freqs[i] << std::endl;
        }
        return s.str();
    }

    std::ostream& operator<<(std::ostream &strm, const StHistogram &hist) {
        std::ostream& retStrm = strm;
        for(int i = 0; i < hist._nBuckets; i++) {
            retStrm << hist._bounds[i].first.data << "," 
                    << hist._bounds[i].second.data << ","
                    << hist._freqs[i] << std::endl;
        }
        return retStrm;
    }
}
