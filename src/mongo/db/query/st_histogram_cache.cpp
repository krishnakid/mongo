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

#include <limits>
#include <fstream>                                  // for debug only

#include "mongo/db/exec/st_histogram.h"
#include "mongo/db/query/qlog.h"
#include "mongo/db/lasterror.h"

namespace mongo {

    StHistogramCache::StHistogramCache() { 
       _cacheVal = NULL;
       _cacheKey = NULL;                // NULL out shim pointers.
    }

    int StHistogramCache::get(const BSONObj& keyPattern, StHistogram* value) {
        log() << " attempting to retreive object "  << keyPattern
              << " from HistogramCache " << endl;
        return 0;
    }

    bool StHistogramCache::contains(const BSONObj& keyPattern) { 
        return false;
    }   

    // verify that everything is here
    void StHistogramCache::ping() {
    }

    void StHistogramCache::update(const BSONObj& keyPattern, StHistogramUpdateParams& params) {
        if (_cacheKey == NULL){
            createNewHistogram(keyPattern);
        }
        _cacheVal->update(params);

        std::ofstream testStream;
        testStream.open("/data/db/debug.log", std::ofstream::out);
        testStream << *_cacheVal;
        testStream.close();
    }

    /* called by get() when a histogram is *not* found for a given keyPattern.
     * lazily creates a new histogram and adds it to the LRU mapping
     *
     */
    int StHistogramCache::createNewHistogram(const BSONObj& keyPattern) { 
        // create a new histogram and add it to the LRUKeyValue cache.
        
        if (_cacheKey != NULL) {
            return 0;
        }

        // TODO: don't like these floating "new/delete" statements, but putting in to make 
        //       this stage work.  Will be replacing this with a more sophisticated KV-store
        //       anyway.
        _cacheKey = new BSONObj(keyPattern);
        _cacheVal = new StHistogram(20, 20, 0, 100);
        // _cacheVal = new StHistogram(20, 20, - std::numeric_limits<double>::max() + 1,
        //                                     std::numeric_limits<double>::max() - 1);
        return 0;
    }


}
