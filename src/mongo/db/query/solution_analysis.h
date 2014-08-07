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

#include <string>

#include "mongo/db/query/st_histogram_cache.h"
#include "mongo/db/query/query_solution.h"
#include "mongo/db/query/stage_types.h"

namespace mongo { 
    class Collection;

    /* maps from StageType to a string representing that StageType */
    extern std::string stageTypeString(StageType ty);

    struct StQuerySolutionCost { 
        StQuerySolutionCost(): card(0), mem(0), cpu(0) {}
        double card;           // cardinality of a subquery           Valid on: [0, Inf)
        double mem;            // memory loaded in bytes by subquery  Valid on: [0, Inf)
        double cpu;            // cpu cycles used by subquery         Valid on: [0, Inf)
    };

    class SolutionAnalysis {
    /* SolutionAnalysis contains a set of functions used by the query planner to estimate
     * the cost of a query represented by a QuerySolutionNode and use the resulting
     * cost estimation in planning.
     */
    public:
        /**
         * estimate the cost of executing the query represented by the 
         * QuerySolution passed in.
         */
        static StQuerySolutionCost estimateSolutionCost(Collection* coll,
                                           QuerySolutionNode* solnRoot);

        /**
         * debug function -- prints out a DOT graph representation of the 
         * solution tree passed in.
         */
        static void dotSolution(Collection* coll, QuerySolutionNode* solnRoot);
    
    private:
        /** 
         * returns some measurement of the expected number of CPU cycles a match would take
         * to resolve on a single document
         *
         * This is currently the number of nodes in a MatchExpression.
         */
        static double estimateMatchCost(MatchExpression* filter);
    };
}

