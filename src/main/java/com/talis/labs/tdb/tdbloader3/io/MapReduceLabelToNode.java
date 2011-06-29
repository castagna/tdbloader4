/*
 * Copyright 2010,2011 Talis Systems Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.talis.labs.tdb.tdbloader3.io;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobID;
import org.openjena.riot.lang.LabelToNode;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.rdf.model.AnonId;

public class MapReduceLabelToNode extends LabelToNode { 

    public MapReduceLabelToNode(JobID jobId, Path path) {
        super(new SingleScopePolicy(), new MapReduceAllocator(jobId, path));
    }
    
    private static class SingleScopePolicy implements ScopePolicy<String, Node, Node> { 
        private Map<String, Node> map = new HashMap<String, Node>() ;
        @Override public Map<String, Node> getScope(Node scope) { return map ; }
        @Override public void clear() { map.clear(); }
    }
    
    private static class MapReduceAllocator implements Allocator<String, Node> {
        
        private JobID jobId ;
        private Path path ;

        public MapReduceAllocator (JobID jobId, Path path) {
            this.jobId = jobId;
            this.path = path;
        }

        @Override 
        public Node create(String label) {
            return Node.createAnon(new AnonId("mrbnode_" + jobId.hashCode() + "_" + path.hashCode() + "_" + label)) ;
        }

        @Override public void reset() {}
    };
    
}