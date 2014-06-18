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

#pragma once

#include <list>
#include <utility>
#include <string>

namespace mongo {
    typedef std::pair<double, double> Bounds;

    class StHistogramRun {
    public:
        StHistogramRun(int bucket, double freq, Bounds bounds);
        
        // accesssors and mutators
        double getHiFreq() { return freqBounds.second; }
        double getLoFreq() { return freqBounds.first; }
        double getTotalFreq() const { return totalFreq; }
        bool isMerged() const { return buckets.size() > 1; }
        std::list<int> getBuckets() { return buckets; }
        void setTotalFreq(double freq) { totalFreq = freq; }
        Bounds getRangeBounds () const { return rangeBounds; }
        void setRangeBounds(Bounds);

        // bin management functions
        void split(std::list<StHistogramRun>& run);
        void merge(StHistogramRun& run);
        double getMaxDiff (StHistogramRun& run);
        
        // debug functions
        void printBuckets();
    private:
        std::list<int> buckets;
        Bounds freqBounds;
        Bounds rangeBounds;
        double totalFreq;
    };

    typedef std::list<StHistogramRun>::iterator StHistogramRunIter;
}
