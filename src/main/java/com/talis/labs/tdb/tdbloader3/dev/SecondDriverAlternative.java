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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talis.labs.tdb.tdbloader3.FirstMapper2;
import com.talis.labs.tdb.tdbloader3.Utils;
import com.talis.labs.tdb.tdbloader3.io.NQuadsInputFormat;

public class SecondDriverAlternative extends Configured implements Tool {

    private static final Logger log = LoggerFactory.getLogger(SecondDriverAlternative.class);
    private static boolean useNQuadsInputFormat = false;
	public static final String TDBLOADER3_COUNTER_GROUPNAME = "TDBLoader3 Counters";
	public static final String TDBLOADER3_COUNTER_MALFORMED = "Malformed";
	public static final String TDBLOADER3_COUNTER_QUADS = "Quads (including duplicates)";
	public static final String TDBLOADER3_COUNTER_TRIPLES = "Triples (including duplicates)";
	public static final String TDBLOADER3_COUNTER_DUPLICATES = "Duplicates (quads or triples)";
    
	public SecondDriverAlternative () {
		super();
		if ( log.isDebugEnabled() ) log.debug("constructed with no configuration.");
	}

	public SecondDriverAlternative (Configuration configuration) {
		super(configuration);
        if ( log.isDebugEnabled() ) log.debug("constructed with configuration.");
	}
	
	public static void setUseNQuadsInputFormat ( boolean flag ) {
		useNQuadsInputFormat = flag;
	}
	
	public static boolean getUseNQuadsInputFormat() {
		return useNQuadsInputFormat;
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
        boolean runLocal = configuration.getBoolean("runLocal", true);
		
        if ( useCompression ) {
            configuration.setBoolean("mapred.compress.map.output", true);
    	    configuration.set("mapred.output.compression.type", "BLOCK");
    	    configuration.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        }
		
        configuration.setInt("io.sort.factor", 100);

		Job job = new Job(configuration);
		job.setJobName("second-alternative");
		job.setJarByClass(getClass());
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(NQuadsInputFormat.class);
        job.setMapperClass(FirstMapper2.class);  
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(SecondReducerAlternative.class);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Text.class);
		
        if ( runLocal ) {
            job.setNumReduceTasks(1);           
        } else {
        	// number of reducers must be the same as in FirstDriverAlternative to ensure offsets are correct
            job.setNumReduceTasks(FirstDriverAlternative.NUM_REDUCERS);
        }
		
       	job.setOutputFormatClass(SequenceFileOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
	    if ( log.isDebugEnabled() ) log.debug("main method: {}", Utils.toString(args));
		int exitCode = ToolRunner.run(new SecondDriverAlternative(), args);
		System.exit(exitCode);
	}
	
}