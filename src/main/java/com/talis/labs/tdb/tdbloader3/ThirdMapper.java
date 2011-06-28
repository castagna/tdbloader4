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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.openjena.atlas.lib.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThirdMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private static final Logger log = LoggerFactory.getLogger(ThirdMapper.class);

    private Text outputKey = new Text();
    private NullWritable outputValue = NullWritable.get();
    
    @Override
	public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if ( log.isDebugEnabled() ) log.debug("< ({}, {})", key, value);

		String v[] = value.toString().split(" ");

		Text s = toHex(v[0]);
        Text p = toHex(v[1]);
        Text o = toHex(v[2]);

		if ( v.length == 4 ) {
	        Text g = toHex(v[3]);
	        emit (context, String.format("%s %s %s %s|%s", s, p, o, g, "SPOG"));
			emit (context, String.format("%s %s %s %s|%s", p, o, s, g, "POSG"));
			emit (context, String.format("%s %s %s %s|%s", o, s, p, g, "OSPG"));
			emit (context, String.format("%s %s %s %s|%s", g, s, p, o, "GSPO"));
			emit (context, String.format("%s %s %s %s|%s", g, p, o, s, "GPOS"));
			emit (context, String.format("%s %s %s %s|%s", g, o, s, p, "GOSP"));
		} else if ( v.length == 3 ) {
			emit (context, String.format("%s %s %s|%s", s, p, o, "SPO"));
			emit (context, String.format("%s %s %s|%s", p, o, s, "POS"));
			emit (context, String.format("%s %s %s|%s", o, s, p, "OSP"));
		}
		
	}

	private Text toHex(String str) {
	    long id = Long.parseLong(str);
        byte[] b = new byte[16];
        Hex.formatUnsignedLongHex(b, 0, id, 16);
        return new Text(b);
	}
	
	private void emit (Context context, String key) throws IOException, InterruptedException {
	    outputKey.clear();
	    outputKey.set(key);
	    context.write(outputKey, outputValue);
        if ( log.isDebugEnabled() ) log.debug("> ({}, {})", outputKey, outputValue);	    
	}

}
