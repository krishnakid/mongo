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

#include <boost/thread/mutex.hpp>
#include <boost/unordered_map.hpp>

#include "mongo/bson/bsonobj.h"
#include "mongo/db/query/lru_key_value.h"
#include "mongo/db/query/index_bounds.h"

namespace mongo {

    class StHistogram;
    class BSONObj;
    class Status;

    struct stEqual : std::binary_function<BSONObj, BSONObj, bool> {
        bool operator()(const BSONObj& b1, const BSONObj& b2) const {
            return b1.equal(b2);
        }
    };

    struct stHash : std::unary_function<BSONObj, std::size_t> {
        std::size_t operator()(const BSONObj& b1) const {
            return b1.hash();
        }
    };

    // typedefs for convenient reference
    typedef boost::unordered_map<BSONObj, StHistogram*, stHash, stEqual> StHistMap;

    // an input struct for the HistogramCache
    struct StHistogramUpdateParams {
        StHistogramUpdateParams (const IndexBounds&, size_t);    
        const IndexBounds& bounds;
        size_t nReturned;
    };

    // acts as an abstraction layer for the management of StHistograms 
    class StHistogramCache { 
    public:
        StHistogramCache();
        
        /* gets the histogram associated with the supplied keyPattern */
        int get(const BSONObj& keyPattern, StHistogram** value);

        /* asks whether the cache contains a histogram matching the supplied keyPattern */
        bool contains(const BSONObj& keyPattern);

        /* updates the histogram cached with the supplied keyPattern or creates one
         * if none exists */
        void update(const BSONObj& keyPattern, const StHistogramUpdateParams& params);

    private:
        /* called to create a new histogram when one does not exist for a supplied 
         * keyPattern in the interface methods */
        int createNewHistogram(const BSONObj& keyPattern);
        
        StHistMap _cache;
    };

}

