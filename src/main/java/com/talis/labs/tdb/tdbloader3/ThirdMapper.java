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

package com.talis.labs.tdb.tdbloader3;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talis.labs.tdb.tdbloader3.io.LongQuadWritable;

public class ThirdMapper extends Mapper<NullWritable, LongQuadWritable, LongQuadWritable, NullWritable> {

    private static final Logger log = LoggerFactory.getLogger(ThirdMapper.class);

    private LongQuadWritable outputKey = new LongQuadWritable();
    private NullWritable outputValue = NullWritable.get();
    
    @Override
	public void map (NullWritable key, LongQuadWritable value, Context context) throws IOException, InterruptedException {
        if ( log.isDebugEnabled() ) log.debug("< ({}, {})", key, value);

		long s = value.get(0);
        long p = value.get(1);
        long o = value.get(2);
        long g = value.get(3);

		if ( g != -1l ) {
	        emit (context, s, p, o, g, "SPOG");
			emit (context, p, o, s, g, "POSG");
			emit (context, o, s, p, g, "OSPG");
			emit (context, g, s, p, o, "GSPO");
			emit (context, g, p, o, s, "GPOS");
			emit (context, g, o, s, p, "GOSP");
		} else {
			emit (context, s, p, o, -1l, "SPO");
			emit (context, p, o, s, -1l, "POS");
			emit (context, o, s, p, -1l, "OSP");
		}
		
	}
	
	private void emit (Context context, long s, long p, long o, long g, String indexName) throws IOException, InterruptedException {
	    outputKey.clear();
	    outputKey.set(s, p, o, g, indexName);
	    context.write(outputKey, outputValue);
        if ( log.isDebugEnabled() ) log.debug("> ({}, {})", outputKey, outputValue);	    
	}

}
