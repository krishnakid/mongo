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

    /**
     * BSONObj equality function wrapper for creation of an unordered map from BSONObj
     * to StHistogram*.
     */
    struct BSONObjEqual : std::binary_function<BSONObj, BSONObj, bool> {
        bool operator()(const BSONObj& b1, const BSONObj& b2) const {
            return b1.equal(b2);
        }
    };

    /**
     * BSONObj hash function wrapper, mapping BSONObj onto a size_t for internal use in 
     * the standard library implementation of unordered_map.
     */
    struct BSONObjHash : std::unary_function<BSONObj, std::size_t> {
        std::size_t operator()(const BSONObj& b1) const {
            return b1.hash();
        }
    };

    /**
     * define a StHistMap as an unordered map from BSONObj -> StHistogram*.  This will be used
     * to store StHistogram pointers indexed by their keyPattern, which is extracted at query
     * time.
     */
    typedef boost::unordered_map<BSONObj, StHistogram*, BSONObjHash, BSONObjEqual> StHistMap;

    /**
     * struct that encapsulates the information requried by an StHistogram to perform an update.
     * bounds is a set of ordered intervals, and nReturned is a size_t indicating how many
     * documents were found in the index on those ranges.
     */
    struct StHistogramUpdateParams {
        StHistogramUpdateParams (const IndexBounds* bounds, size_t nRet):
                                                    bounds(bounds),
                                                    nReturned(nRet) {};    
        const IndexBounds* bounds;
        size_t nReturned;
    };

    /**
     * StHistogramCache lives in the CollectionInfoCache and does memory managment for creation
     * of StHistograms associated with different keyPatterns.  It also acts as the interface
     * layer for the query planner when it tries to make predictions for IXSCAN cardinality.
     * 
     * It is perhaps badly named, probably closer to a StHistogramMap but named it
     * StHistogramCache because of where it lives (and the fact it is not persisted to disk).
     */
    class StHistogramCache { 
    public:
        const static int kInitialHistogramSize;                 // number of bins per type range
        const static double kInitialHistogramBinValue;          // initialization frequency for 
                                                                // histogram bins

        /**
         * loads the histogram associated with the given keyPattern into value.  
         * Returns: 
         * true     if the histogram was found and successfully loaded
         * false    if a histogram could not be found matching the supplied keyPattern
         */
        bool get(const BSONObj& keyPattern, StHistogram** value);

        /** 
         * updates the histogram cached with the supplied keyPattern or creates one
         * if none exists 
         * keyPattern corresponds to the index key pattern. 
         */
        void update(const BSONObj& keyPattern, const StHistogramUpdateParams& params);

    private:
        /** 
         * called by get() when a histogram is *not* found for a given keyPattern.
         * creates a new histogram and adds it to the mapping
         */
        void createNewHistogram(const BSONObj& keyPattern);
       
        /* maps from index keyPattern to the StHistogram corresponding to the index. */
        StHistMap _cache;
    };
}

