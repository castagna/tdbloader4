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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.jena.tdbloader3.io.LongQuadWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FourthMapper extends Mapper<LongQuadWritable, NullWritable, LongQuadWritable, NullWritable> {

    private static final Logger log = LoggerFactory.getLogger(FourthMapper.class);

    private LongQuadWritable outputKey = new LongQuadWritable();
    private NullWritable outputValue = NullWritable.get();
    private Counters counters;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    	counters = new Counters(context);
    };
    
    @Override
	public void map (LongQuadWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
        if ( log.isDebugEnabled() ) log.debug("< ({}, {})", key, value);

		long s = key.get(0);
        long p = key.get(1);
        long o = key.get(2);
        long g = key.get(3);

		if ( g != -1l ) {
	        emit (context, s, p, o, g, "SPOG");
			emit (context, p, o, s, g, "POSG");
			emit (context, o, s, p, g, "OSPG");
			emit (context, g, s, p, o, "GSPO");
			emit (context, g, p, o, s, "GPOS");
			emit (context, g, o, s, p, "GOSP");
			counters.incrementQuads();
		} else {
			emit (context, s, p, o, -1l, "SPO");
			emit (context, p, o, s, -1l, "POS");
			emit (context, o, s, p, -1l, "OSP");
			counters.incrementTriples();
		}

	}
	
	private void emit (Context context, long s, long p, long o, long g, String indexName) throws IOException, InterruptedException {
	    outputKey.clear();
	    outputKey.set(s, p, o, g, indexName);
	    context.write(outputKey, outputValue);
        if ( log.isDebugEnabled() ) log.debug("> ({}, {})", outputKey, outputValue);	    
	}
	
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	counters.close();
    }

}
