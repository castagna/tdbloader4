/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jena.tdbloader4;

import java.io.IOException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.jena.tdbloader4.io.QuadWritable;
import org.apache.jena.tdbloader4.io.SinkQuadMapReduce;
import org.openjena.atlas.event.Event;
import org.openjena.atlas.event.EventManager;
import org.openjena.atlas.lib.Sink;
import org.openjena.riot.pipeline.inf.InfFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.util.FileManager;

public class InferMapper extends Mapper<LongWritable, QuadWritable, Text, NullWritable> {

    private static final Logger log = LoggerFactory.getLogger(InferMapper.class);

    private Counters counters;
    private Sink<Quad> sink;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    	counters = new Counters(context);
    	Model vocabulary = ModelFactory.createDefaultModel();
    	Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
    	for ( Path file : files ) {
    		log.debug("Loading {} ...", file.toString());
    		FileManager.get().readModel(vocabulary, file.toString());
    		
    	}
        sink = InfFactory.infQuads(new SinkQuadMapReduce(context), vocabulary);
    };
    
    @Override
	public void map (LongWritable key, QuadWritable value, Context context) throws IOException, InterruptedException {
        log.debug("< ({}, {})", key, value);

        Quad quad = value.getQuad();
		if ( quad.isTriple() ) {
			EventManager.send(counters, new Event(Constants.eventQuad, null));
		} else {
			EventManager.send(counters, new Event(Constants.eventTriple, null));
		}
		sink.send(quad);
	}
	
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	counters.close();
    }

}
