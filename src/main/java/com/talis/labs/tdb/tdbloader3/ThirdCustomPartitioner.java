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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ThirdCustomPartitioner extends Partitioner<Text, NullWritable>{

	@Override
	public int getPartition(Text key, NullWritable value, int numPartitions) {
		String v = key.toString().split("\\|")[1];

		if ( v.equals("SPO") ) return 0;
		if ( v.equals("POS") ) return 1;
		if ( v.equals("OSP") ) return 2;
		if ( v.equals("GSPO") ) return 3;
		if ( v.equals("GPOS") ) return 4;
		if ( v.equals("GOSP") ) return 5;
		if ( v.equals("SPOG") ) return 6;
		if ( v.equals("POSG") ) return 7;
		if ( v.equals("OSPG") ) return 8;
		
		return 0;
	}

}
