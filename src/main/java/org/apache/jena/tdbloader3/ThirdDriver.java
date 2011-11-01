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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.jena.tdbloader3.io.LongQuadWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ThirdDriver extends Configured implements Tool {

    private static final Logger log = LoggerFactory.getLogger(ThirdDriver.class);

    public ThirdDriver () {
		super();
	    log.debug("constructed with no configuration.");
	}

	public ThirdDriver (Configuration configuration) {
		super(configuration);
        log.debug("constructed with configuration.");
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if ( args.length != 2 ) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		
		log.debug("input: {}, output: {}", args[0], args[1]);
		
		Configuration configuration = getConf();
        boolean useCompression = configuration.getBoolean(Constants.OPTION_USE_COMPRESSION, Constants.OPTION_USE_COMPRESSION_DEFAULT);
        log.debug("Compression is {}", useCompression ? "enabled" : "disabled" );

        if ( useCompression ) {
            configuration.setBoolean("mapred.compress.map.output", true);
    	    configuration.set("mapred.output.compression.type", "BLOCK");
    	    configuration.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        }
		
		Job job = new Job(configuration);
		job.setJobName(Constants.NAME_THIRD);
		job.setJarByClass(getClass());
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.setInputPathFilter(job, ExcludeNodeTableFilter.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setInputFormatClass(SequenceFileInputFormat.class);			
		
		job.setMapperClass(ThirdMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(ThirdReducer.class);
		job.setOutputKeyClass(LongQuadWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
	    Utils.setReducers(job, configuration, log);
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		if ( useCompression ) {
			SequenceFileOutputFormat.setCompressOutput(job, true);
			SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
			SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		}

		if ( log.isDebugEnabled() ) Utils.log(job, log);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
        log.debug("main method: {}", Utils.toString(args));
	    int exitCode = ToolRunner.run(new ThirdDriver(), args);
		System.exit(exitCode);
	}

}

class ExcludeNodeTableFilter implements PathFilter {
    public boolean accept(Path p) {
        String name = p.getName();
        return !name.startsWith("node") && !name.startsWith(Constants.NAME_FIRST) && !name.startsWith(Constants.NAME_SECOND);
    }
}
