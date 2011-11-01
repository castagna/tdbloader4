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

package org.apache.jena.tdbloader3.io;

import java.io.IOException;
import java.io.StringWriter;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.openjena.atlas.lib.Sink;
import org.openjena.riot.out.NodeFmtLib;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.sparql.core.Quad;

public class QuadMapReduceSink implements Sink<Quad> {

    private static final Logger log = LoggerFactory.getLogger(QuadMapReduceSink.class);
	
	private final TaskInputOutputContext<? extends Writable, ? extends Writable,Text,NullWritable> context;
	private Text key = new Text();
    private final NullWritable value = NullWritable.get();

	public QuadMapReduceSink (TaskInputOutputContext<? extends Writable,? extends Writable,Text,NullWritable> context) {
		this.context = context;
		log.debug("constructed with context {}", context);
	}
	
	@Override
	public void send(Quad quad) {
		log.debug("< {}", quad);
		key.set(serialize(quad));
		try {
			context.write(key, value);
			log.debug("> ({}, {})", key, value);
		} catch (IOException e) {
			log.error(e.getMessage(), e);
		} catch (InterruptedException e) {
			log.error(e.getMessage(), e);
		}
		key.clear();
	}

	private String serialize(Quad quad) {
        StringWriter sw = new StringWriter();
        NodeFmtLib.str(sw, quad.getSubject());       
        sw.append(" ");
        NodeFmtLib.str(sw, quad.getPredicate());
        sw.append(" ");
        NodeFmtLib.str(sw, quad.getObject());
        sw.append(" ");
        if ( !quad.isTriple() ) {
            NodeFmtLib.str(sw, quad.getGraph());
            sw.append(" ");
        }
        sw.append(".");
		return sw.toString();
	}
	
	@Override public void flush() { /* do nothing */ }
	@Override public void close() { /* do nothing */ }
	
}
