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
    // StQuerySolutionCost datatype
    StQuerySolutionCost SolutionAnalysis::buildSolutionCost(double card = 0,
                                                            double mem = 0,
                                                            double cpu = 0) {
        StQuerySolutionCost n;
        n.card = card;
        n.mem = mem;
        n.cpu = cpu;
        return n;
    }
    
    // static
    double SolutionAnalysis::estimateMatchCost(MatchExpression* filter) {
        if (filter == NULL) {
            return 0;
        }
        if (filter->numChildren() == 0) { 
            return 1;
        }

        double agg = 0;
        std::vector<MatchExpression*> childVect = *(filter->getChildVector());
        for (size_t ix = 0; ix < childVect.size(); ++ix) { 
            agg += estimateMatchCost(childVect[ix]);
        }
        agg += 1;

        return agg;
    }


    // static
    StQuerySolutionCost SolutionAnalysis::estimateSolutionCost(Collection* coll, 
                                                               QuerySolutionNode* solnRoot) {
        typedef std::vector<QuerySolutionNode*>::iterator ChildIter;

        std::vector<QuerySolutionNode*> children = solnRoot->children;
        StageType ty = solnRoot->getType();

        double aggCard = 0;
        double aggMem = 0;
        double aggCpu = 0;

        // load all children
        std::vector<StQuerySolutionCost> childCosts (children.size());
        for (size_t ix = 0; ix < children.size(); ++ix) { 
            StQuerySolutionCost childCost = estimateSolutionCost(coll, children[ix]);
            childCosts[ix] = childCost;
            aggCard += childCost.card;
            aggMem += childCost.mem;
            aggCpu += childCost.cpu;
        }

        // core switch statement for recursion.
        switch (ty) {
        case STAGE_AND_HASH:
        case STAGE_AND_SORTED: {
            // return the minimum cost amongst all children            
            double minCardIx = 0;
            for (size_t ix = 1; ix < children.size(); ++ix) { 
                if (childCosts[ix].card < childCosts[minCardIx].card) {
                    minCardIx = ix;
                }
            }
            double newCard = childCosts[minCardIx].card;
            double newMem = aggMem;
            double newCpu = aggCpu + newCard*estimateMatchCost(solnRoot->filter.get());
            return buildSolutionCost(newCard, newMem, newCpu);
        }
        case STAGE_CACHED_PLAN: 
            return buildSolutionCost();

        case STAGE_COLLSCAN: {
            // return the number of documents in the collection
            double nRecords = static_cast<double>(coll->numRecords());
            double avgRecordSize = static_cast<double>(coll->averageObjectSize());
            double newCard = nRecords;
            double newMem = aggMem + avgRecordSize*nRecords;
            double newCpu = aggCpu + newCard*estimateMatchCost(solnRoot->filter.get());
            return buildSolutionCost(newCard, newMem, newCpu);
        }
        case STAGE_COUNT:
            // this should be thought about in much the same way as the general
            // IXSCAN, and we can think of this as the cost of an IXSCAN minus the 
            // penalty incurred for a fetch.
            break;

        case STAGE_DISTINCT:
            break;

        case STAGE_EOF:
            return buildSolutionCost();

        case STAGE_KEEP_MUTATIONS:
            return buildSolutionCost(aggCard, aggMem, aggCpu);

        case STAGE_FETCH: {   //TODO
            double avgRecordSize = static_cast<double>(coll->averageObjectSize());
            double newCard = aggCard;
            double newMem = aggMem + newCard*avgRecordSize;
            double newCpu = aggCpu + newCard*estimateMatchCost(solnRoot->filter.get());
            return buildSolutionCost(aggCard, newMem, newCpu);
        }
        case STAGE_GEO_NEAR_2D:
        case STAGE_GEO_NEAR_2DSPHERE:
        case STAGE_IDHACK:
            break;

        case STAGE_IXSCAN: {
            StHistogramCache* histCache = coll->infoCache()->getStHistogramCache();
            // make a safe type cast
            IndexScanNode* ixNode = dynamic_cast<IndexScanNode*>(solnRoot); 

            StHistogram* ixHist;
            int flag = histCache->get(ixNode->indexKeyPattern, &ixHist);
            if (flag == -1) {
                break;          // return early, no histogram found in cache
            } 
            double newCard = ixHist->getFreqOnRange(ixNode->bounds);
        
            // TODO: this info *has* to be stored somewhere in the Index itself - extract it.
            double ixSize = ixHist->getTotalFreq();

            double newMem = ixSize + newCard*static_cast<double>(coll->averageObjectSize());
            double newCpu = std::log(ixSize) * 8;
            return buildSolutionCost(newCard, newMem, newCpu);
        }
        case STAGE_LIMIT: {
            LimitNode* lm = static_cast<LimitNode*>(solnRoot); 
            double newCard = std::min<double>(aggCard, static_cast<double>(lm->limit));
            return buildSolutionCost(newCard, aggMem, aggCpu);
        }
        case STAGE_MOCK:
        case STAGE_MULTI_PLAN:
        case STAGE_OPLOG_START:
            break;

        case STAGE_OR:
            return buildSolutionCost(aggCard, aggMem, aggCpu);
        
        case STAGE_PROJECTION:
        case STAGE_SHARDING_FILTER:
            break;
            
        case STAGE_SKIP: {
            // return max(agg - nToSkip, 0) 
            SkipNode* skp = static_cast<SkipNode*>(solnRoot);
            double newCard = std::max<double>(aggCard - skp->skip, 0.0);
            return buildSolutionCost(newCard, aggMem, aggCpu);
        }
        case STAGE_SORT:
        case STAGE_SORT_MERGE: {
            double newCpu = aggCpu + aggCard*std::log(aggCard);
            return buildSolutionCost(aggCard, aggMem, newCpu);
        }
        case STAGE_SUBPLAN:
        case STAGE_TEXT:
        case STAGE_UNKNOWN:
            break;
        };

        // for not currently implemented code paths
        return buildSolutionCost();
    }

    // static
    //
    // assumes graph represented by solnRoot is acyclic
    void SolutionAnalysis::dotSolution(Collection* coll, QuerySolutionNode* solnRoot) {

        std::vector<QuerySolutionNode*> children = solnRoot->children;
        // need to have some way of assigning unique names to each node
        // when building the actual digraph representation at the end.
        
        std::map<QuerySolutionNode*, string> nameMap;
        std::map<QuerySolutionNode*, StQuerySolutionCost> costMap;

        std::list<std::pair<QuerySolutionNode*, QuerySolutionNode*> > edges;

        // depth first traversal assigns names to nodes
        
        int nameCounter = 0;
        std::list<QuerySolutionNode*> traversal;

        nameMap[solnRoot] = typeToString(solnRoot->getType()) + boost::lexical_cast<string>(nameCounter++);
        costMap[solnRoot] = estimateSolutionCost(coll, solnRoot);

        traversal.push_back(solnRoot);
        
        while (!traversal.empty()) {
            QuerySolutionNode* curNode = traversal.front();
            std::vector<QuerySolutionNode*> children = curNode->children;

            typedef std::vector<QuerySolutionNode*>::iterator ChildIt;
            for(ChildIt i = children.begin(); i != children.end(); i++) { 
                edges.push_back(std::make_pair(curNode, *i));
                nameMap[*i] = typeToString((*i)->getType()) + boost::lexical_cast<string>(nameCounter++);
                costMap[*i] = estimateSolutionCost(coll, *i);
                
                traversal.push_back(*i);
            }
            traversal.pop_front();
        }

        std::cout << "digraph testGraph {" << std::endl;
        // enumerate all of the nodes and their names 
        typedef std::map<QuerySolutionNode*, string>::iterator NodeIter;
        for (NodeIter i = nameMap.begin(); i != nameMap.end(); ++i) { 
            std::cout << i->second << "[label=<" << typeToString(i->first->getType())
                      << "<BR /> <FONT POINT-SIZE=\"10\"> Cost : "
                      << "{ card : " << costMap[i->first].card 
                      << " , mem : " << costMap[i->first].mem 
                      << " , cpu : " << costMap[i->first].cpu << " } "
                      << "</FONT>>];" << std::endl;
        }
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

