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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.jena.tdbloader3.io.LongQuadWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.tdb.base.file.Location;
import com.hp.hpl.jena.tdb.store.bulkloader2.CmdIndexBuild;

public class FourthReducer extends Reducer<LongQuadWritable, NullWritable, NullWritable, NullWritable> {

    private static final Logger log = LoggerFactory.getLogger(FourthReducer.class);

	private Map<String, OutputStream> outputs;
    private FileSystem fs;
    private Path outLocal;
    private Path outRemote;

	@Override
	public void setup(Context context) {
		outputs = new HashMap<String, OutputStream>();
		try {
			fs = FileSystem.get(context.getConfiguration());
	        outRemote = FileOutputFormat.getWorkOutputPath(context);
            outLocal = new Path("/tmp", context.getJobName() + "_" + context.getJobID() + "_" + context.getTaskAttemptID());
	        new File(outLocal.toString()).mkdir();
	        // TODO: does this make sense?
	        fs.setReplication(outLocal, (short)2);
	        fs.startLocalOutput(outRemote, outLocal);
		} catch (Exception e) {
		    throw new TDBLoader3Exception(e);
		}
	}

	@Override
	public void reduce(LongQuadWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        if ( log.isDebugEnabled() ) log.debug("< ({}, {})", key, values.iterator().next());

		String filename = key.getIndexName();
		OutputStream out = getOutputStream(filename);
		if ( out != null ) {
			out.write(Utils.toHex(key.get(0)));
			out.write(' ');
			out.write(Utils.toHex(key.get(1)));
			out.write(' ');
			out.write(Utils.toHex(key.get(2)));
			if ( key.get(3) != -1l ) {
				out.write(' ');
				out.write(Utils.toHex(key.get(3)));				
			}
			out.write('\n');
		}
		context.progress();
		
        if ( log.isDebugEnabled() ) log.debug("> {}:{}", filename, key);
	}
	
	private OutputStream getOutputStream(String filename) throws FileNotFoundException {
		BufferedOutputStream output = null;
		if ( !outputs.containsKey(filename) ) {
			output = new BufferedOutputStream(new FileOutputStream(outLocal.toString() + "/" + filename));
			outputs.put(filename, output);
		}
		return outputs.get(filename);
	}

	@Override
	public void cleanup(Context context) throws IOException {
		for ( String filename : outputs.keySet() ) {
			outputs.get(filename).close();
		}

		Location location = new Location(outLocal.toString());
		for ( String indexName : Utils.indexNames ) {
		    String indexFilename = location.absolute(indexName);
		    if ( new File(indexFilename).exists() ) {
		        CmdIndexBuild.main(location.getDirectoryPath(), indexName, indexFilename);
	            // To save some disk space
	            new File (indexFilename).delete();
		    }
		}

    	fs.completeLocalOutput(outRemote, outLocal);
	}

}