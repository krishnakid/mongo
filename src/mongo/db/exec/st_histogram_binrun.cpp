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
    // necessary operators for BSONProjection
    //
    // TODO: code for defining projection should go here. Projection currently is defined as:
    // /
    // f(x) = number(x) if x is a numeric type
    //      | 0         otherwise
    BSONProjection::BSONProjection(BSONElement elem) { 
        canonVal = elem.canonicalType();
        data = elem.number();                               // return 0 if not number
    }

    BSONProjection::BSONProjection(int canonVal, double data) : canonVal(canonVal), data(data) {}

    BSONProjection::BSONProjection() : canonVal(0), data(0.0) {}

    double BSONProjection::operator-(BSONProjection right) const { 
        if (canonVal != right.canonVal) { 
            return std::numeric_limits<double>::infinity() * (canonVal - right.canonVal);
        }
        return data - right.data;
    }

    bool BSONProjection::operator<=(const BSONProjection& right) const {
        return canonVal < right.canonVal ? true : data <= right.data;
    }

    bool BSONProjection::operator<(const BSONProjection& right) const {
        return canonVal < right.canonVal ? true : data < right.data;
    }

    bool BSONProjection::operator>=(const BSONProjection& right) const {
        return canonVal > right.canonVal ? true : data >= right.data;
    }
 
    bool BSONProjection::operator>(const BSONProjection& right) const {
        return canonVal > right.canonVal ? true : data > right.data;
    }
 
    BSONProjection BSONProjection::operator+(const double right) const { 
        return BSONProjection(canonVal, data + right);
    }

    StHistogramRun::StHistogramRun(int bucket, double freq, Bounds bounds) {
        _freqBounds.first = _freqBounds.second = _totalFreq = freq;
        _rangeBounds.first = bounds.first;
        _rangeBounds.second = bounds.second;
        _buckets.push_back(bucket);
    }

    // return minimum of maximum differences.
    double StHistogramRun::getMaxDiff (StHistogramRun& run) {
        return std::max(run.getHiFreq() - getLoFreq(),
                        getHiFreq() - run.getLoFreq());
    }

    void StHistogramRun::setRangeBounds(Bounds nwBnds) {
        _rangeBounds.first = nwBnds.first;
        _rangeBounds.second = nwBnds.second;
    }

    // split information from local StHistogramRun across external runs
    void StHistogramRun::split (std::list<StHistogramRun>& runs) {
        int nNew = runs.size();

        double newFreq = getTotalFreq() / (nNew + 1) ;
        double rangeStep = (_rangeBounds.second - _rangeBounds.first) / (nNew + 1);
        BSONProjection curStart = _rangeBounds.first;
        for (StHistogramRunIter i = runs.begin(); i != runs.end(); i++) { 
            i->setTotalFreq(newFreq);
            i->setRangeBounds(Bounds(curStart, curStart+rangeStep));
            curStart = curStart + rangeStep;
        }

        // update main object and deal with rounding errors
        setTotalFreq(newFreq);
        setRangeBounds(Bounds(curStart, _rangeBounds.second));
    }

    // merge two runs and store info in first run.
    void StHistogramRun::merge (StHistogramRun& run) { 
        // merge bucket ranges
        std::list<int> exBuckets = run.getBuckets();
        _buckets.splice(_buckets.end(), exBuckets);    
        _totalFreq += run.getTotalFreq();
        _freqBounds.first = std::min(getLoFreq(), run.getLoFreq());
        _freqBounds.second = std::max(getHiFreq(), run.getHiFreq());
        
        Bounds extBounds = run.getRangeBounds();
        _rangeBounds.first = std::min(extBounds.first, _rangeBounds.first);
        _rangeBounds.second = std::max(extBounds.second, _rangeBounds.second);
    }

    // DEBUG
    void StHistogramRun::printBuckets() {
        typedef std::list<int>::iterator Iter;
        std::cout << "[" ;
        for(Iter i = _buckets.begin(); i != _buckets.end(); i++) {
           std::cout << *i << ", " ;
        }
        std::cout << std::string("]");
    }
}
