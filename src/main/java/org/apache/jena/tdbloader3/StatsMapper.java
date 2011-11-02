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

package org.apache.jena.tdbloader3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.jena.tdbloader3.io.QuadWritable;
import org.openjena.atlas.event.Event;
import org.openjena.atlas.event.EventManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.vocabulary.RDF;

public class StatsMapper extends Mapper<LongWritable, QuadWritable, Text, IntWritable> {

    private static final Logger log = LoggerFactory.getLogger(StatsMapper.class);

    private Counters counters;
    private Text outputKey = new Text();
    private static final IntWritable one = new IntWritable(1);
    private static final String LABEL_PROPERTY = "p";
    private static final String LABEL_CLASS = "c";
    private static final String LABEL_NAMESPACE = "n";
    private static final String LABEL_TOTAL_POSTFIX = "t";
    private static final String LABEL_PER_GRAPH_POSTFIX = "g";
    
//    @Override
//    protected void setup(Context context) throws IOException, InterruptedException {
//    	counters = new Counters(context);
//    };
    
    @Override
	public void map (LongWritable key, QuadWritable value, Context context) throws IOException, InterruptedException {
        log.debug("< ({}, {})", key, value);

        Quad quad = value.getQuad();
		if ( quad.isTriple() ) {
			EventManager.send(counters, new Event(Constants.eventQuad, null));
		} else {
			EventManager.send(counters, new Event(Constants.eventTriple, null));
		}

		Node g = quad.getGraph();
		Node p = quad.getPredicate();
		emit (LABEL_PROPERTY, g, p, context);
		
		if ( RDF.type.asNode().equals(p) ) {
			Node o = quad.getObject();
			if ( o.isURI() ) {
				emit (LABEL_CLASS, g, o, context);
			} else {
				// TODO: WARN!
			}
		}
	}

    private void emit(String label, Node graph, Node node, Context context) throws IOException, InterruptedException {
		StringBuffer sb = new StringBuffer(label);
		sb.append(LABEL_TOTAL_POSTFIX);
		sb.append("|");
		sb.append(node.getURI());
		outputKey.set(sb.toString());
		context.write(outputKey, one);
		log.debug("> ({}, {})", outputKey, one);
		outputKey.clear();
		
		sb = new StringBuffer(label);
		sb.append(LABEL_PER_GRAPH_POSTFIX);
		sb.append("|");
		sb.append(graph.getURI());
		sb.append("|");
		sb.append(node.getURI());
		outputKey.set(sb.toString());
		context.write(outputKey, one);
		log.debug("> ({}, {})", outputKey, one);
		outputKey.clear();
		
		sb = new StringBuffer(LABEL_NAMESPACE);
		sb.append(LABEL_TOTAL_POSTFIX);
		sb.append("|");
		sb.append(node.getNameSpace());
		outputKey.set(sb.toString());
		context.write(outputKey, one);
		log.debug("> ({}, {})", outputKey, one);
		outputKey.clear();

		sb = new StringBuffer(LABEL_NAMESPACE);
		sb.append(LABEL_PER_GRAPH_POSTFIX);
		sb.append("|");
		sb.append(graph.getURI());
		sb.append("|");
		sb.append(node.getNameSpace());
		outputKey.set(sb.toString());
		context.write(outputKey, one);
		log.debug("> ({}, {})", outputKey, one);
		outputKey.clear();
    }
    
//    @Override
//    protected void cleanup(Context context) throws IOException, InterruptedException {
//    	super.cleanup(context);
//    	counters.close();
//    }

}
