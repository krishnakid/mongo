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

#include "mongo/db/query/solution_analysis.h"

#include <algorithm>
#include <vector>
#include <list>
#include <map>
#include <string>
#include <boost/lexical_cast.hpp>

#include "mongo/db/catalog/collection.h"
#include "mongo/db/exec/st_histogram.h"
#include "mongo/db/query/query_planner_common.h"
#include "mongo/db/query/qlog.h"
#include "mongo/db/query/stage_types.h"

namespace mongo { 
    // static
    double SolutionAnalysis::analyzeIndexSelectivity(StHistogramCache* histCache,
                                                         QuerySolutionNode* solnRoot) {
        typedef std::vector<QuerySolutionNode*>::iterator ChildIter;

        if (solnRoot->getType() != STAGE_IXSCAN) {
            if (solnRoot->children.size() == 0) {
                // then there are no children
                return -1;
            }
            double retVal = 0;
            // recurse through children
            for (ChildIter it = solnRoot->children.begin(); it != solnRoot->children.end(); it++) {
                double childVal = analyzeIndexSelectivity(histCache, *it);
                if (std::min(retVal, childVal) > 0) {
                    retVal = 0;         // index intersection, ignore for now
                }
                else {
                    retVal = std::max(retVal, childVal);
                }
            }
            return retVal;
        } 
        else {
            // make a safe type cast
            IndexScanNode* ixNode = dynamic_cast<IndexScanNode*>(solnRoot);
            
            StHistogram* ixHist;
            int flag = histCache->get(ixNode->indexKeyPattern, &ixHist);
            if (flag == -1) {
                return -1;          // return early, no histogram found in cache
            } 
            // print out histogram prediction on the simple range
            
            std::vector<OrderedIntervalList> ixBounds = ixNode->bounds.fields;

            // TODO: Generalize this to include a full generalization of capabilities.
            
            if (ixBounds.size() > 1) { 
                log() << "compound index - ignoring during planning stage right now" << endl;
                return -1;
            }
            if (ixBounds.begin()->intervals.size() > 1) {
                log() << "bounds have more than one interval - ignore during planning stage" << endl;
                return -1;
            }
            std::vector<Interval> intervals = ixBounds.begin()->intervals;
            BSONElement st = intervals.begin()->start;
            BSONElement end = intervals.begin()->end;
            if (!st.isNumber() || !end.isNumber()) { 
                log() << "field bounds are not numeric -- ignore for now" << endl;
                return -1;
            }
            
            std::pair<double, double> numericBounds = std::make_pair(st.numberDouble(),
                                                                     end.numberDouble());

            double value = ixHist->getFreqOnRange(numericBounds);
            // deal with the index scan logic and histogram prediction
            return value;
        }

    }


    // static
    double SolutionAnalysis::estimateSolutionCost(Collection* coll, QuerySolutionNode* solnRoot) {
        typedef std::vector<QuerySolutionNode*>::iterator ChildIter;

        std::vector<QuerySolutionNode*> children = solnRoot->children;
        StageType ty = solnRoot->getType();

        double agg = 0;
        for (ChildIter i = children.begin(); i != children.end(); ++i) { 
            agg += estimateSolutionCost(coll, *i);
        }
            
        // core switch statement for recursion.
        switch (ty) {
        case STAGE_AND_HASH:
        case STAGE_AND_SORTED: {
            // return the minimum cost amongst all children
            double minCost = estimateSolutionCost(coll, *children.begin());
            for (ChildIter i = ++children.begin(); i != children.end(); ++i) { 
                double tCost = estimateSolutionCost(coll, *i);
                if (tCost < minCost) {
                    minCost = tCost;
                }
            }
            return 2*minCost;
        }
        case STAGE_CACHED_PLAN: 
            return -1;

        case STAGE_COLLSCAN:
            // return the number of documents in the collection
            return static_cast<double>(coll->numRecords());

        case STAGE_COUNT:
            // this should be thought about in much the same way as the general
            // IXSCAN, and we can think of this as the cost of an IXSCAN minus the 
            // penalty incurred for a fetch.
            return -1;

        case STAGE_DISTINCT:
            return agg;

        case STAGE_EOF:
            return 0.0;

        case STAGE_KEEP_MUTATIONS:
            return agg;

        case STAGE_FETCH:
            return 2*agg;

        case STAGE_GEO_NEAR_2D:
        case STAGE_GEO_NEAR_2DSPHERE:
        case STAGE_IDHACK:
            return -1;

        case STAGE_IXSCAN: {
            StHistogramCache* histCache = coll->infoCache()->getStHistogramCache();

            // make a safe type cast
            IndexScanNode* ixNode = dynamic_cast<IndexScanNode*>(solnRoot);
            
            StHistogram* ixHist;
            int flag = histCache->get(ixNode->indexKeyPattern, &ixHist);
            if (flag == -1) {
                return -1;          // return early, no histogram found in cache
            } 
            // print out histogram prediction on the simple range
            
            std::vector<OrderedIntervalList> ixBounds = ixNode->bounds.fields;

            // TODO: Generalize this to include a full generalization of capabilities.
            
            if (ixBounds.size() > 1) { 
                log() << "compound index - ignoring during planning stage right now" << endl;
                return -1;
            }
            if (ixBounds.begin()->intervals.size() > 1) {
                log() << "bounds have more than one interval - ignore during planning stage" << endl;
                return -1;
            }
            std::vector<Interval> intervals = ixBounds.begin()->intervals;
            BSONElement st = intervals.begin()->start;
            BSONElement end = intervals.begin()->end;
            if (!st.isNumber() || !end.isNumber()) { 
                log() << "field bounds are not numeric -- ignore for now" << endl;
                return -1;
            }
            
            std::pair<double, double> numericBounds = std::make_pair(st.numberDouble(),
                                                                     end.numberDouble());

            double value = ixHist->getFreqOnRange(numericBounds);
            // deal with the index scan logic and histogram prediction
            return value;
        }
        case STAGE_LIMIT: {
            // return min(agg, limit);
            LimitNode* lm = static_cast<LimitNode*>(solnRoot);
            return std::min<double>(agg, static_cast<double>(lm->limit));
        }
        case STAGE_MOCK:
        case STAGE_MULTI_PLAN:
        case STAGE_OPLOG_START:
            return -1;

        case STAGE_OR:
            return agg;
        
        case STAGE_PROJECTION:
            return agg;

        case STAGE_SHARDING_FILTER:
            return -1;
            
        case STAGE_SKIP: {
            // return max(agg - nToSkip, 0) 
            SkipNode* skp = static_cast<SkipNode*>(solnRoot);
            return std::max<double>(agg - skp->skip, 0.0);
        }
        case STAGE_SORT:
        case STAGE_SORT_MERGE:
            return 2*agg;

        case STAGE_SUBPLAN:
        case STAGE_TEXT:
        case STAGE_UNKNOWN:
            return -1;
        };
    }

    // static
    //
    // assumes graph represented by solnRoot is acyclic
    void SolutionAnalysis::dotSolution(QuerySolutionNode* solnRoot) {

        std::vector<QuerySolutionNode*> children = solnRoot->children;
        // need to have some way of assigning unique names to each node
        // when building the actual digraph representation at the end.
        
        std::map<QuerySolutionNode*, string> nameMap;
        std::list<std::pair<QuerySolutionNode*, QuerySolutionNode*> > edges;

        // depth first traversal assigns names to nodes
        
        int nameCounter = 0;
        std::list<QuerySolutionNode*> traversal;

        nameMap[solnRoot] = typeToString(solnRoot->getType()) + boost::lexical_cast<string>(nameCounter++);
        traversal.push_back(solnRoot);
        
        while (!traversal.empty()) {
            QuerySolutionNode* curNode = traversal.front();
            std::vector<QuerySolutionNode*> children = curNode->children;

            typedef std::vector<QuerySolutionNode*>::iterator ChildIt;
            for(ChildIt i = children.begin(); i != children.end(); i++) { 
                edges.push_back(std::make_pair(curNode, *i));
                nameMap[*i] = typeToString((*i)->getType()) + boost::lexical_cast<string>(nameCounter++);
                
                traversal.push_back(*i);
            }

            traversal.pop_front();
        }

        std::cout << "digraph testGraph {" << std::endl;
        // now go through each edge pair and draw it
        typedef std::list<std::pair<QuerySolutionNode*, QuerySolutionNode*> >::iterator EdgeIt;
        for (EdgeIt i = edges.begin(); i != edges.end(); i++) {
            std::cout << nameMap[(*i).first] << " -> " << nameMap[(*i).second] << ";" << std::endl; 
        }
        std::cout << "}" << std::endl;
    }

    // private
    std::string SolutionAnalysis::typeToString(StageType ty) {
        switch (ty) {
        case STAGE_AND_HASH:
            return "AND_HASH";   
        case STAGE_AND_SORTED:
            return "AND_SORTED"; 
        case STAGE_CACHED_PLAN: 
            return "CACHED_PLAN"; 
        case STAGE_COLLSCAN:
            return "COLLSCAN";
        case STAGE_COUNT:
            return "COUNT";
        case STAGE_DISTINCT:
            return "DISTINCT";
        case STAGE_EOF:
            return "EOF";
        case STAGE_KEEP_MUTATIONS:
            return "KEEP_MUTATIONS";
        case STAGE_FETCH:
            return "FETCH";
        case STAGE_GEO_NEAR_2D:
            return "GEO_NEAR_2D";
        case STAGE_GEO_NEAR_2DSPHERE:
            return "GEO_NEAR_2DSPHERE";
        case STAGE_IDHACK:
            return "IDHACK";
        case STAGE_IXSCAN:
            return "IXSCAN";
        case STAGE_LIMIT:
            return "LIMIT";
        case STAGE_MOCK:
            return "MOCK";
        case STAGE_MULTI_PLAN:
            return "MULTI_PLAN";
        case STAGE_OPLOG_START:
            return "OPLOG_START";
        case STAGE_OR:
            return "OR";
        case STAGE_PROJECTION:
            return "PROJECTION";
        case STAGE_SHARDING_FILTER:
            return "SHARDING_FILTER";
        case STAGE_SKIP:
            return "SKIP";
        case STAGE_SORT:
            return "SORT";
        case STAGE_SORT_MERGE:
            return "SORT_MERGE";
        case STAGE_SUBPLAN:
            return "SUBPLAN";
        case STAGE_TEXT:
            return "TEXT";
        case STAGE_UNKNOWN:
            return "UNKNOWN";
        };

    }
    
}

