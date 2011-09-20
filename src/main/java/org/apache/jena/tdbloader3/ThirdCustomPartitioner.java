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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.jena.tdbloader3.io.LongQuadWritable;


public class ThirdCustomPartitioner extends Partitioner<LongQuadWritable, NullWritable>{

	@Override
	public int getPartition(LongQuadWritable key, NullWritable value, int numPartitions) {
		String indexName = key.getIndexName();

		if ( indexName.equals("SPO") ) return 0;
		if ( indexName.equals("POS") ) return 1;
		if ( indexName.equals("OSP") ) return 2;
		if ( indexName.equals("GSPO") ) return 3;
		if ( indexName.equals("GPOS") ) return 4;
		if ( indexName.equals("GOSP") ) return 5;
		if ( indexName.equals("SPOG") ) return 6;
		if ( indexName.equals("POSG") ) return 7;
		if ( indexName.equals("OSPG") ) return 8;
		
		return 0;
	}

}
