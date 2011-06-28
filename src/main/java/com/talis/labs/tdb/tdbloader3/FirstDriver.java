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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talis.labs.tdb.tdbloader3.io.NQuadsInputFormat;

public class FirstDriver extends Configured implements Tool {

    private static final Logger log = LoggerFactory.getLogger(FirstDriver.class);
    
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
		
		Job job = Job.getInstance(getConf(), "first");
		job.setJarByClass(getClass());
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setInputFormatClass(NQuadsInputFormat.class);
		job.setMapperClass(FirstMapper2.class);
//		job.setMapperClass(FirstMapper.class);
		job.setReducerClass(FirstReducer.class);

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
		
		// TODO: this is bad, how could we merge node tables?
		job.setNumReduceTasks(1); 

		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
	    if ( log.isDebugEnabled() ) log.debug("main method: {}", toString(args));
		int exitCode = ToolRunner.run(new FirstDriver(), args);
		System.exit(exitCode);
	}

	public static String toString(String[] args) {
        StringBuffer sb = new StringBuffer();
        sb.append("{");
        for ( String arg : args ) sb.append(arg).append(", ");
        if ( sb.length() > 2 ) sb.delete(sb.length()-2, sb.length());
        sb.append("}");
        return sb.toString();
	}
	
}
