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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
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

public class SecondReducerAlternative extends Reducer<Text, Text, LongWritable, Text> {
	
    private static final Logger log = LoggerFactory.getLogger(SecondReducerAlternative.class);
    
    private static Nodec nodec = new NodecSSE() ;
	private long sum;
	private String id;
	private Text key;
	private ArrayList<Long> offsets;

    @Override
    public void setup(Context context) {
    	sum = 0;
        id = String.valueOf(context.getTaskAttemptID().getTaskID().getId());
        key = new Text(id);
        
        log.debug("Loading offsets from DistributedCache...");
        offsets = loadOffsets(context);
        log.debug("Finished loading offsets from DistributedCache.");

    }

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Node node = parse(key.toString());
		// TODO
	}

    @Override
    public void cleanup(Context context) throws IOException {
    	// TODO
    }

    private ArrayList<Long> loadOffsets(Context context) {
    	ArrayList<Long> offsets = new ArrayList<Long>();
        Configuration configuration = context.getConfiguration();
        try {
			Path[] cachedFiles = DistributedCache.getLocalCacheFiles(configuration);
			if ( log.isDebugEnabled() ) {
				log.debug("Files in DistributedCache are:");
				for ( Path file : cachedFiles ) {
					log.debug(file.toUri().toString());
				}
			}
			for (Path file : cachedFiles) {
				if ( "offsets.txt".equals(file.getName()) ) {
					log.debug("Reading offsets file found in DistributedCache...");
					BufferedReader in = new BufferedReader(new FileReader(file.toString()));
					String str;
					while ((str = in.readLine()) != null) {
						log.debug ("< {}", str);
						offsets.add(Long.parseLong(str.split("\t")[1]));
					}
					in.close();
				}
			}
		} catch (IOException e) {
			throw new TDBLoader3Exception(e);
		}
		return offsets;
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
