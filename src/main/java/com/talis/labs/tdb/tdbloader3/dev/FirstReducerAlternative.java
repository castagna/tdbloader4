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

package com.talis.labs.tdb.tdbloader3.dev;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.openjena.atlas.logging.Log;
import org.openjena.riot.Lang;
import org.openjena.riot.system.ParserProfile;
import org.openjena.riot.system.RiotLib;
import org.openjena.riot.tokens.Token;
import org.openjena.riot.tokens.Tokenizer;
import org.openjena.riot.tokens.TokenizerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.tdb.nodetable.Nodec;
import com.hp.hpl.jena.tdb.nodetable.NodecSSE;
import com.talis.labs.tdb.tdbloader3.TDBLoader3Exception;

public class FirstReducerAlternative extends Reducer<Text, Text, Text, LongWritable> {
	
    private static final Logger log = LoggerFactory.getLogger(FirstReducerAlternative.class);
    
    private static Nodec nodec = new NodecSSE() ;
	private long sum;
	private String id;
	private Text key;

    @Override
    public void setup(Context context) {
    	sum = 0;
        id = String.valueOf(context.getTaskAttemptID().getTaskID().getId());
        key = new Text(id);
    }

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Node node = parse(key.toString());
		
		ByteBuffer bb = ByteBuffer.allocate(nodec.maxSize(node));
		int len = nodec.encode(node, bb, null);
		sum += 4 + len; // 4 is the overhead to store the length of the ByteBuffer

		if ( log.isDebugEnabled() ) log.debug("< {}: ({}, (null))", id, key);
	}

    @Override
    public void cleanup(Context context) throws IOException {
        LongWritable value = new LongWritable(sum);
        if ( log.isDebugEnabled() ) log.debug("> ({}, {})", key, value);
        try {
            context.write(key, value);
            super.cleanup(context); // is this necessary?
        } catch (InterruptedException e) {
            throw new TDBLoader3Exception(e);
        }
    }

    private static Node parse(String string) {
    	ParserProfile profile = RiotLib.profile(Lang.NQUADS, null, null) ;
        Tokenizer tokenizer = TokenizerFactory.makeTokenizerString(string) ;
        if ( ! tokenizer.hasNext() )
            return null ;
        Token t = tokenizer.next();
        Node n = profile.create(null, t) ;
        if ( tokenizer.hasNext() )
            Log.warn(RiotLib.class, "String has more than one token in it: "+string) ;
        return n ;
    }
    
}
