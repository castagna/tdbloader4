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
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SecondDriver extends Configured implements Tool {

    private static final Logger log = LoggerFactory.getLogger(SecondDriver.class);

	public SecondDriver () {
		super();
	    if ( log.isDebugEnabled() ) log.debug("constructed with no configuration.");
	}

	public SecondDriver (Configuration configuration) {
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
		
		Job job = Job.getInstance(getConf(), "second");
		job.setJarByClass(getClass());
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.setInputPathFilter(job, ExcludeNodeTableFilter.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(SecondMapper.class);
		job.setReducerClass(SecondReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
        if ( log.isDebugEnabled() ) log.debug("main method: {}", FirstDriver.toString(args));
	    int exitCode = ToolRunner.run(new SecondDriver(), args);
		System.exit(exitCode);
	}

}

class ExcludeNodeTableFilter implements PathFilter {
    public boolean accept(Path p) {
        String name = p.getName();
        return !name.startsWith("node") && !name.startsWith("first_");
    }
}