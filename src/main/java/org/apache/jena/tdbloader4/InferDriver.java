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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.jena.tdbloader4.io.NQuadsInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InferDriver extends Configured implements Tool {

    private static final Logger log = LoggerFactory.getLogger(InferDriver.class);

    public InferDriver () {
		super();
		log.debug("constructed with no configuration.");
	}

	public InferDriver (Configuration configuration) {
		super(configuration);
        log.debug("constructed with configuration.");
	}

	@Override
	public int run(String[] args) throws Exception {
		if ( args.length != 3 ) {
			System.err.printf("Usage: %s [generic options] <vocabulary> <input> <output>\n", getClass().getName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		
		Configuration configuration = getConf();
        boolean useCompression = configuration.getBoolean(Constants.OPTION_USE_COMPRESSION, Constants.OPTION_USE_COMPRESSION_DEFAULT);
		
        if ( useCompression ) {
            configuration.setBoolean("mapred.compress.map.output", true);
    	    configuration.set("mapred.output.compression.type", "BLOCK");
    	    configuration.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        }

        boolean overrideOutput = configuration.getBoolean(Constants.OPTION_OVERRIDE_OUTPUT, Constants.OPTION_OVERRIDE_OUTPUT_DEFAULT);
        FileSystem fs = FileSystem.get(new Path(args[2]).toUri(), configuration);
        if ( overrideOutput ) {
            fs.delete(new Path(args[2]), true);
        }
        
        // All the mappers need to have the vocabulary/ontology available, typically they are very small
        Path vocabulary = new Path(args[0]);
        DistributedCache.addCacheFile(vocabulary.toUri(), configuration);
        
		Job job = new Job(configuration);
		job.setJobName(Constants.NAME_INFER);
		job.setJarByClass(getClass());
		
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setInputFormatClass(NQuadsInputFormat.class);
        job.setMapperClass(InferMapper.class);		    
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setNumReduceTasks(0); // map only job

       	job.setOutputFormatClass(TextOutputFormat.class);

       	if ( log.isDebugEnabled() ) Utils.log(job, log);

		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
	    log.debug("main method: {}", Utils.toString(args));
		int exitCode = ToolRunner.run(new InferDriver(), args);
		System.exit(exitCode);
	}
	
}
