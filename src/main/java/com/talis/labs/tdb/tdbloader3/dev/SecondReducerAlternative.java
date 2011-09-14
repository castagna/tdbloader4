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

import static com.hp.hpl.jena.tdb.lib.NodeLib.setHash;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.openjena.atlas.lib.Bytes;
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
import com.hp.hpl.jena.tdb.base.file.FileSet;
import com.hp.hpl.jena.tdb.base.file.Location;
import com.hp.hpl.jena.tdb.base.objectfile.ObjectFile;
import com.hp.hpl.jena.tdb.base.record.Record;
import com.hp.hpl.jena.tdb.base.record.RecordFactory;
import com.hp.hpl.jena.tdb.index.Index;
import com.hp.hpl.jena.tdb.lib.NodeLib;
import com.hp.hpl.jena.tdb.store.Hash;
import com.hp.hpl.jena.tdb.sys.Names;
import com.hp.hpl.jena.tdb.sys.SystemTDB;
import com.talis.labs.tdb.setup.BlockMgrBuilder;
import com.talis.labs.tdb.setup.BlockMgrBuilderStd;
import com.talis.labs.tdb.setup.IndexBuilder;
import com.talis.labs.tdb.setup.IndexBuilderStd;
import com.talis.labs.tdb.setup.ObjectFileBuilder;
import com.talis.labs.tdb.setup.ObjectFileBuilderStd;
import com.talis.labs.tdb.tdbloader3.TDBLoader3Exception;

public class SecondReducerAlternative extends Reducer<Text, Text, LongWritable, Text> {
	
    private static final Logger log = LoggerFactory.getLogger(SecondReducerAlternative.class);
    
	private ArrayList<Long> offsets;
	private long offset = 0L;

    private Index nodeHashToId;
    private ObjectFile objects;
    private FileSystem fs;
    private Path outLocal;
    private Path outRemote;

    @Override
    public void setup(Context context) {
        String id = String.valueOf(context.getTaskAttemptID().getTaskID().getId());
        
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
            outRemote = new Path(FileOutputFormat.getWorkOutputPath(context), id);
            outLocal = new Path("/tmp", context.getJobName() + "_" + context.getJobID() + "_" + context.getTaskAttemptID());
            fs.startLocalOutput(outRemote, outLocal);
        } catch (Exception e) {
            throw new TDBLoader3Exception(e);
        }
        Location location = new Location(outLocal.toString());
        init(location);
    }

    private void init(Location location) {
        ObjectFileBuilder objectFileBuilder = new ObjectFileBuilderStd() ;
        BlockMgrBuilder blockMgrBuilder = new BlockMgrBuilderStd() ;
        IndexBuilder indexBuilder = new IndexBuilderStd(blockMgrBuilder, blockMgrBuilder) ;
        RecordFactory recordFactory = new RecordFactory(SystemTDB.LenNodeHash, SystemTDB.SizeOfNodeId) ;
        nodeHashToId = indexBuilder.buildIndex(new FileSet(location, Names.indexNode2Id), recordFactory) ;
        objects = objectFileBuilder.buildObjectFile(new FileSet(location, Names.indexId2Node), Names.extNodeData) ;
    }

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// we could avoid doing this, just generate an hash, id text file which we will be using to rebuild the B+Tree index at the end
		Node node = parse(key.toString());
        Hash hash = new Hash(nodeHashToId.getRecordFactory().keyLength()) ;
        setHash(hash, node) ;
        byte k[] = hash.getBytes() ;        
        Record r = nodeHashToId.getRecordFactory().create(k) ;
        long id = NodeLib.encodeStore(node, objects) ;
        Bytes.setLong(id, r.getValue(), 0) ;
        nodeHashToId.add(r);

        LongWritable _id = new LongWritable(id + offset);

		for (Text value : values) {
	        if ( log.isDebugEnabled() ) log.debug("< ({}, {})", key, value);
			context.write(_id, value);
	        if ( log.isDebugEnabled() ) log.debug("> ({}, {})", _id, value);
		}
	}

    @Override
    public void cleanup(Context context) throws IOException {
        try {
			super.cleanup(context);
		} catch (InterruptedException e) {
			throw new TDBLoader3Exception(e);
		}
        if ( nodeHashToId != null ) nodeHashToId.sync();
        if ( nodeHashToId != null ) nodeHashToId.close();
        if ( objects != null ) objects.sync();
        if ( objects != null ) objects.close();
        if ( fs != null ) fs.completeLocalOutput(outRemote, outLocal);
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
