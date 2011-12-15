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

package org.apache.jena.tdbloader4;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.openjena.atlas.event.Event;
import org.openjena.atlas.event.EventManager;
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
import com.hp.hpl.jena.rdf.model.AnonId;
import com.hp.hpl.jena.tdb.base.file.FileFactory;
import com.hp.hpl.jena.tdb.base.file.Location;
import com.hp.hpl.jena.tdb.base.objectfile.ObjectFile;
import com.hp.hpl.jena.tdb.lib.NodeLib;
import com.hp.hpl.jena.tdb.sys.Names;

public class SecondReducer extends Reducer<Text, Text, LongWritable, Text> {
	
    private static final Logger log = LoggerFactory.getLogger(SecondReducer.class);
    
	private ArrayList<Long> offsets;
	private long offset = 0L;

    private ObjectFile objects;
    private FileSystem fs;
    private Path outLocal;
    private Path outRemote;
    private TaskAttemptID taskAttemptID;
    private Counters counters;
    
    @Override
    public void setup(Context context) {
        this.taskAttemptID = context.getTaskAttemptID();
        String id = String.valueOf(taskAttemptID.getTaskID().getId());
        
        log.debug("Loading offsets from DistributedCache...");
        offsets = loadOffsets(context);
        log.debug("Finished loading offsets from DistributedCache.");

        // this is the offset this reducer needs to add (the sum of all his 'previous' peers) 
        for (int i = 0; i < Integer.valueOf(id); i++) {
        	offset += offsets.get(i);
        }
        log.debug("Reducer's number {} offset is {}", id, offset);

        try {
            fs = FileSystem.get(context.getConfiguration());
            outRemote = FileOutputFormat.getWorkOutputPath(context);
            log.debug("outRemote is {}", outRemote);
            outLocal = new Path("/tmp", context.getJobName() + "_" + context.getJobID() + "_" + taskAttemptID);
            fs.startLocalOutput(outRemote, outLocal);
        } catch (Exception e) {
            throw new TDBLoader4Exception(e);
        }
        Location location = new Location(outLocal.toString());
        init(location);
        
        counters = new Counters(context);
    }

    private void init(Location location) {
        objects = FileFactory.createObjectFileDisk(location.getPath(Names.indexId2Node, Names.extNodeData)) ;
    }

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String keyStr = key.toString();
		Node node = parse(keyStr);
		// this is to ensure that offset between FirstReducer and SecondReducer are the same, even when blank nodes are present
		if ( node.isBlank() ) { node = Node.createAnon(new AnonId(keyStr)); }

		long id = NodeLib.encodeStore(node, objects) ;
		LongWritable _id = new LongWritable(id + offset);

        for (Text value : values) {
	        log.debug("< ({}, {})", key, value);
			context.write(_id, value);
	        log.debug("> ({}, {})", _id, value);
		}

        EventManager.send(counters, new Event(Constants.eventRdfNode, node));
	}

    @Override
    public void cleanup(Context context) throws IOException {
    	try {
			super.cleanup(context);
		} catch (InterruptedException e) {
			throw new TDBLoader4Exception(e);
		}
        if ( objects != null ) objects.sync();
        if ( objects != null ) objects.close();
        if ( fs != null ) fs.completeLocalOutput(outRemote, outLocal);
        counters.close();
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
				if ( Constants.OFFSETS_FILENAME.equals(file.getName()) ) {
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
			throw new TDBLoader4Exception(e);
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
