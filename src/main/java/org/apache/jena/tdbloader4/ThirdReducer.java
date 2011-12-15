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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.jena.tdbloader4.io.LongQuadWritable;
import org.openjena.atlas.event.Event;
import org.openjena.atlas.event.EventManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThirdReducer extends Reducer<Text, Text, LongQuadWritable, NullWritable> {

    private static final Logger log = LoggerFactory.getLogger(ThirdReducer.class);

    private LongQuadWritable outputKey = new LongQuadWritable();
    private final NullWritable outputValue = NullWritable.get();
    private Counters counters;

	@Override
    protected void setup(Context context) throws IOException, InterruptedException {
		counters = new Counters(context);
    }

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		long s = -1l;
		long p = -1l;
		long o = -1l;
		long g = -1l;
		
		for (Text value : values) {
	        log.debug("< ({}, {})", key, value);
			String[] v = value.toString().split("\\|");
			
			long id = Long.parseLong(v[0]);
			if ( v[1].equals("S") ) {
				if ( s != -1l ) EventManager.send(counters, new Event(Constants.eventDuplicate, null));
				s = id; 
			}
			if ( v[1].equals("P") ) p = id;
			if ( v[1].equals("O") ) o = id;
			if ( v[1].equals("G") ) g = id;
		}
		
		if ( ( g != -1l ) && ( s != -1l ) && ( p != -1l ) && ( o != -1l ) ) {
		    outputKey.set(s, p, o, g);
		    EventManager.send(counters, new Event(Constants.eventQuad, null));
	        context.write(outputKey, outputValue);
	        log.debug("> ({}, {})", outputKey, outputValue);
		} else if ( ( s != -1l ) && ( p != -1l ) && ( o != -1l ) ) {
		    outputKey.set(s, p, o);
		    EventManager.send(counters, new Event(Constants.eventTriple, null));
	        context.write(outputKey, outputValue);
	        log.debug("> ({}, {})", outputKey, outputValue);
		} else {
			EventManager.send(counters, new Event(Constants.eventMalformed, null));
        	log.warn("WARNING: unexpected values for key {}", key );
		}
        outputKey.clear();
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		counters.close();
	}

}
