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
    
    struct StQuerySolutionCost { 
        double card;
        double mem;
        double cpu;
    };

    class SolutionAnalysis {
    public:
        // estimate the cost of executing the query represented by the 
        // QuerySolution passed in.
        //
        // TODO: extend cost to be an abstract data type
        static StQuerySolutionCost estimateSolutionCost(Collection* coll,
                                           QuerySolutionNode* solnRoot);

        // debug function -- prints out a DOT graph representation of the 
        // solution tree passed in.
        static void dotSolution(Collection* coll, QuerySolutionNode* solnRoot);
    
    private:
        static std::string typeToString(StageType ty);

        /* returns some measurement of the expected number of CPU cycles a match would take
         * to resolve on a single document
         */
        static double estimateMatchCost(MatchExpression* filter);

        static StQuerySolutionCost buildSolutionCost(double card, double mem, double cpu);
    };
}






