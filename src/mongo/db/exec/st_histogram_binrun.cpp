/**
 *    Copyright (C) 2013 10gen Inc.
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

#include "mongo/db/exec/st_histogram_binrun.h"

#include <list>
#include <utility>
#include <string>
#include <iostream>

namespace mongo {
    StHistogramRun::StHistogramRun(int bucket, double freq, std::pair<double, double> bounds) {
        freqBounds.first = freqBounds.second = totalFreq = freq;
        rangeBounds.first = bounds.first;
        rangeBounds.second = bounds.second;
        buckets.push_back(bucket);
    }
    // return minimum of maximum differences.
    double StHistogramRun::getMaxDiff (StHistogramRun& run) {
        return std::max(run.getHiFreq() - getLoFreq(),
                        getHiFreq() - run.getLoFreq());
    }


    void StHistogramRun::setRangeBounds(Bounds nwBnds) {
        rangeBounds.first = nwBnds.first;
        rangeBounds.second = nwBnds.second;
    }


    // split information from local StHistogramRun across external runs
    void StHistogramRun::split (std::list<StHistogramRun>& runs) {
        int nNew = runs.size();

        double newFreq = getTotalFreq() / (nNew + 1) ;
        double rangeStep = (rangeBounds.second - rangeBounds.first) / (nNew + 1);
        double curStart = rangeBounds.first;
        for (StHistogramRunIter i = runs.begin(); i != runs.end(); i++) { 
            i->setTotalFreq(newFreq);
            i->setRangeBounds(Bounds(curStart, curStart+rangeStep));
            curStart += rangeStep;
        }

        // update main object and deal with rounding errors
        setTotalFreq(newFreq);
        setRangeBounds(Bounds(curStart, rangeBounds.second));
    }

    // merge two runs and store info in first run.
    void StHistogramRun::merge (StHistogramRun& run) { 
        // merge bucket ranges
        std::list<int> exBuckets = run.getBuckets();
        buckets.splice(buckets.end(), exBuckets);    
        totalFreq += run.getTotalFreq();
        freqBounds.first = std::min(getLoFreq(), run.getLoFreq());
        freqBounds.second = std::max(getHiFreq(), run.getHiFreq());
        
        Bounds extBounds = run.getRangeBounds();
        rangeBounds.first = std::min(extBounds.first, rangeBounds.first);
        rangeBounds.second = std::max(extBounds.second, rangeBounds.second);
    }

    void StHistogramRun::printBuckets() {
        typedef std::list<int>::iterator Iter;
        std::cout << "[" ;
        for(Iter i = buckets.begin(); i != buckets.end(); i++) {
           std::cout << *i << ", " ;
        }
        std::cout << std::string("]");
    }
}
