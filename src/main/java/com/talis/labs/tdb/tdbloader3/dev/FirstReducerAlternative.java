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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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
    private FileSystem fs;
    private Path outLocal;
    private Path outRemote;
	private long sum;
    
    @Override
    public void setup(Context context) {
        try {
            fs = FileSystem.get(context.getConfiguration());
            outRemote = FileOutputFormat.getWorkOutputPath(context);
            outLocal = new Path("/tmp", context.getJobName() + "_" + context.getJobID() + "_" + context.getTaskAttemptID());
            fs.startLocalOutput(outRemote, outLocal);
            sum = 0;
        } catch (Exception e) {
            throw new TDBLoader3Exception(e);
        }
    }

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Node node = parse(key.toString());
		sum += nodec.maxSize(node);
		if ( log.isDebugEnabled() ) log.debug("< ({}, (null))", key);
	}

    @Override
    public void cleanup(Context context) throws IOException {
        Text key = new Text(String.valueOf(context.getTaskAttemptID().getTaskID().getId()));
        LongWritable value = new LongWritable(sum);
        if ( log.isDebugEnabled() ) log.debug("> ({}, {})", key, value);
        try {
            context.write(key, value);
        } catch (InterruptedException e) {
            throw new TDBLoader3Exception(e);
        }
        if ( fs != null ) fs.completeLocalOutput(outRemote, outLocal);
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
