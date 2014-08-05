// st_histogram_cache.cpp
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

#include "mongo/db/query/st_histogram_cache.h"

#include <boost/unordered_map.hpp>
#include <limits>
#include <fstream>

#include "mongo/db/exec/st_histogram.h"
#include "mongo/db/query/qlog.h"
#include "mongo/db/query/query_knobs.h"
#include "mongo/db/lasterror.h"
#include "mongo/db/query/lru_key_value.h"

namespace mongo {
    StHistogramCache::StHistogramCache() : _cache() {}

    const int StHistogramCache::kInitialHistogramSize = 15;
    const double StHistogramCache::kInitialHistogramBinValue = 20.0;
    const double StHistogramCache::kInitialHistogramLowBound = -100.0;
    const double StHistogramCache::kInitialHistogramHighBound = 200.0;
   
    bool StHistogramCache::get(const BSONObj& keyPattern, StHistogram** value) {
        StHistMap::iterator histEntry = _cache.find(keyPattern);
        if (histEntry == _cache.end()) {
            return false;               // ERROR, no histogram found for the index
        }

        *value = histEntry->second;
        return true;
    }

    void StHistogramCache::update(const BSONObj& keyPattern, const StHistogramUpdateParams& params) {
        if (_cache.find(keyPattern) == _cache.end()) {
            createNewHistogram(keyPattern);
        }
        StHistMap::iterator histEntry = _cache.find(keyPattern);
        histEntry->second->update(params);
        
        std::ofstream testStream;                                   // DEBUG log entry
        testStream.open("/data/db/debug.log", std::ofstream::out);
        testStream << *(histEntry->second);
        testStream.close(); 
    }

    int StHistogramCache::createNewHistogram(const BSONObj& keyPattern) { 
        StHistogram* newHist = new StHistogram(kInitialHistogramSize,
                                               kInitialHistogramBinValue,
                                               kInitialHistogramLowBound,
                                               kInitialHistogramHighBound);
        _cache.insert(std::make_pair(keyPattern, newHist));
        return 0;
    }

}
