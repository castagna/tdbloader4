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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talis.labs.tdb.tdbloader3.io.NQuadsInputFormat;

public class FirstDriver extends Configured implements Tool {

    private static final Logger log = LoggerFactory.getLogger(FirstDriver.class);
	public static final String TDBLOADER3_COUNTER_GROUPNAME = "TDBLoader3 Counters";
	public static final String TDBLOADER3_COUNTER_MALFORMED = "Malformed";
	public static final String TDBLOADER3_COUNTER_QUADS = "Quads (including duplicates)";
	public static final String TDBLOADER3_COUNTER_TRIPLES = "Triples (including duplicates)";
	public static final String TDBLOADER3_COUNTER_DUPLICATES = "Duplicates (quads or triples)";
	
	public static final int DEFAULT_NUM_REDUCERS = 10; 
    
	public FirstDriver () {
		super();
		if ( log.isDebugEnabled() ) log.debug("constructed with no configuration.");
	}

	public FirstDriver (Configuration configuration) {
		super(configuration);
        if ( log.isDebugEnabled() ) log.debug("constructed with configuration.");
	}

	@Override
	public int run(String[] args) throws Exception {
		if ( args.length != 2 ) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		Configuration configuration = getConf();
        boolean useCompression = configuration.getBoolean("useCompression", false);
		
        if ( useCompression ) {
            configuration.setBoolean("mapred.compress.map.output", true);
    	    configuration.set("mapred.output.compression.type", "BLOCK");
    	    configuration.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        }
		
        configuration.setInt("io.sort.factor", 100);

		Job job = new Job(configuration);
		job.setJobName("first-alternative");
		job.setJarByClass(getClass());
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(NQuadsInputFormat.class);
        job.setMapperClass(FirstMapper.class);		    
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setReducerClass(FirstReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(LongWritable.class);
		
	    setReducers(job, configuration);

       	job.setOutputFormatClass(TextOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void setReducers(Job job, Configuration configuration) {
        boolean runLocal = configuration.getBoolean("runLocal", true);
        int num_reducers = configuration.getInt("numReducers", DEFAULT_NUM_REDUCERS);

	    // TODO: should we comment this out and let Hadoop decide the number of reducers?
        if ( runLocal ) {
        	log.debug("Setting number of reducers to {}", 1);
            job.setNumReduceTasks(1);           
        } else {
        	// number of reducers must be the same as in FirstDriverAlternative to ensure offsets are correct
        	log.debug("Setting number of reducers to {}", num_reducers);
            job.setNumReduceTasks(num_reducers);
        }
	}
	
	public static void main(String[] args) throws Exception {
	    if ( log.isDebugEnabled() ) log.debug("main method: {}", Utils.toString(args));
		int exitCode = ToolRunner.run(new FirstDriver(), args);
		System.exit(exitCode);
	}
	
}
