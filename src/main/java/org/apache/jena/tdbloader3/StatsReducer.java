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
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
    private static final Logger log = LoggerFactory.getLogger(StatsReducer.class);
	private IntWritable value = new IntWritable(-1);
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		Iterator<IntWritable> iter = values.iterator();
		int sum = 0;
		while ( iter.hasNext() ) {
			IntWritable v = iter.next();
			log.debug("< ({}, {}", key, v);
			sum += v.get();
		}
		value.set(sum);
		context.write(key, value);
		log.debug("> ({}, {})", key, value);
	}

}
