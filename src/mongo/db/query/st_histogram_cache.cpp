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
#include <fstream>                                  // for debug only

#include "mongo/db/exec/st_histogram.h"
#include "mongo/db/query/qlog.h"
#include "mongo/db/query/query_knobs.h"
#include "mongo/db/lasterror.h"
#include "mongo/db/query/lru_key_value.h"

namespace mongo {
    StHistogramCache::StHistogramCache() : _cache() {}
    
    int StHistogramCache::get(const BSONObj& keyPattern, StHistogram** value) {
        StHistMap::iterator histEntry = _cache.find(keyPattern);
        if (histEntry == _cache.end()) {
            return -1;      // ERROR, no histogram found for the index
        }

        *value = histEntry->second;
        return 0;
    }

    bool StHistogramCache::contains(const BSONObj& keyPattern) { 
        return false;
    }   

    void StHistogramCache::update(const BSONObj& keyPattern, const StHistogramUpdateParams& params) {
        if (_cache.find(keyPattern) == _cache.end()) {
            createNewHistogram(keyPattern);
        }
      
        log() << "updating " << keyPattern << " with data ( " << params.start << ", " << 
            params.end << ", " << params.nReturned << " )" << endl;

        StHistMap::iterator histEntry = _cache.find(keyPattern);
        histEntry->second->update(params);

        std::ofstream testStream;
        testStream.open("/data/db/debug.log", std::ofstream::out);
        testStream << *(histEntry->second);
        testStream.close();
    }

    /* called by get() when a histogram is *not* found for a given keyPattern.
     * lazily creates a new histogram and adds it to the LRU mapping
     *
     */
    int StHistogramCache::createNewHistogram(const BSONObj& keyPattern) { 
        // create a new histogram and add it to the LRUKeyValue cache.
        
        StHistogram* newHist = new StHistogram(30, 20, -1000, 1000);
        _cache.insert(std::make_pair(keyPattern, newHist));

        log() << "new key added : " << keyPattern << endl;

        // _cacheVal = new StHistogram(20, 20, - std::numeric_limits<double>::max() + 1,
        //                                     std::numeric_limits<double>::max() - 1);
        return 0;
    }


}
