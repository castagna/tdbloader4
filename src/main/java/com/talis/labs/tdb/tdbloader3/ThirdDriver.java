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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talis.labs.tdb.tdbloader3.io.LongQuadWritable;

public class ThirdDriver extends Configured implements Tool {

    private static final Logger log = LoggerFactory.getLogger(ThirdDriver.class);
    
    public ThirdDriver () {
		super();
        if ( log.isDebugEnabled() ) log.debug("constructed with no configuration.");
	}

	public ThirdDriver (Configuration configuration) {
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
		
		Job job = Job.getInstance(getConf(), "third");
		job.setJarByClass(getClass());
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		job.setMapperClass(ThirdMapper.class);
		job.setMapOutputKeyClass(LongQuadWritable.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setReducerClass(ThirdReducer.class);
		job.setOutputKeyClass(LongQuadWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		// TODO: There must be a bug in the SNAPSHOTs of Hadoop
		// see: http://markmail.org/thread/n3wqbozf6ow2cib6
		//
		// WARN  Exception running child : java.lang.NullPointerException
		//    at org.apache.hadoop.mapred.TaskLogAppender.flush(TaskLogAppender.java:96)
		//    at org.apache.hadoop.mapred.TaskLog.syncLogs(TaskLog.java:239)
		//    at org.apache.hadoop.mapred.Child$4.run(Child.java:225)
		//    at java.security.AccessController.doPrivileged(Native Method)
		//    at javax.security.auth.Subject.doAs(Subject.java:396)
		//    at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1153)
		//    at org.apache.hadoop.mapred.Child.main(Child.java:217)
		//
		// job.setPartitionerClass(ThirdCustomPartitioner.class);
		// job.setNumReduceTasks(9);
		// 
		job.setNumReduceTasks(1);

		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
        if ( log.isDebugEnabled() ) log.debug("main method: {}", Utils.toString(args));
	    int exitCode = ToolRunner.run(new ThirdDriver(), args);
		System.exit(exitCode);
	}

}
