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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.jena.tdbloader3.io.LongQuadWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ThirdReducer extends Reducer<Text, Text, LongQuadWritable, NullWritable> {

    private static final Logger log = LoggerFactory.getLogger(ThirdReducer.class);

    private LongQuadWritable outputKey = new LongQuadWritable();
    private final NullWritable outputValue = NullWritable.get();

    protected void setup(Context context) throws IOException, InterruptedException {
        context.getCounter(FirstDriver.TDBLOADER3_COUNTER_GROUPNAME, FirstDriver.TDBLOADER3_COUNTER_TRIPLES).increment(0);
        context.getCounter(FirstDriver.TDBLOADER3_COUNTER_GROUPNAME, FirstDriver.TDBLOADER3_COUNTER_QUADS).increment(0);
    	context.getCounter(FirstDriver.TDBLOADER3_COUNTER_GROUPNAME, FirstDriver.TDBLOADER3_COUNTER_DUPLICATES).increment(0);
    };
    
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		long s = -1l;
		long p = -1l;
		long o = -1l;
		long g = -1l;
		
		for (Text value : values) {
	        if ( log.isDebugEnabled() ) log.debug("< ({}, {})", key, value);
			String[] v = value.toString().split("\\|");
			
			long id = Long.parseLong(v[0]);
			if ( v[1].equals("S") ) {
				if ( s != -1l ) context.getCounter(FirstDriver.TDBLOADER3_COUNTER_GROUPNAME, FirstDriver.TDBLOADER3_COUNTER_DUPLICATES).increment(1);
				s = id; 
			}
			if ( v[1].equals("P") ) p = id;
			if ( v[1].equals("O") ) o = id;
			if ( v[1].equals("G") ) g = id;
		}
		
		if ( ( g != -1l ) && ( s != -1l ) && ( p != -1l ) && ( o != -1l ) ) {
		    outputKey.set(s, p, o, g);
	        context.getCounter(FirstDriver.TDBLOADER3_COUNTER_GROUPNAME, FirstDriver.TDBLOADER3_COUNTER_QUADS).increment(1);
		} else if ( ( s != -1l ) && ( p != -1l ) && ( o != -1l ) ) {
		    outputKey.set(s, p, o);
	        context.getCounter(FirstDriver.TDBLOADER3_COUNTER_GROUPNAME, FirstDriver.TDBLOADER3_COUNTER_TRIPLES).increment(1);
		} else {
	        context.getCounter(FirstDriver.TDBLOADER3_COUNTER_GROUPNAME, FirstDriver.TDBLOADER3_COUNTER_MALFORMED).increment(1);
        	if ( log.isWarnEnabled() ) log.warn("WARNING: unexpected values for key {}", key );
		}
        context.write(outputKey, outputValue);
        if ( log.isDebugEnabled() ) log.debug("> ({}, {})", outputKey, outputValue);
        outputKey.clear();
	}

}
