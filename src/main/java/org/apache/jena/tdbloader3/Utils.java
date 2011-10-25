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

import java.io.StringWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.jena.tdbloader3.io.MapReduceLabelToNode;
import org.apache.jena.tdbloader3.partitioners.TotalOrderPartitioner;
import org.openjena.atlas.lib.Hex;
import org.openjena.riot.ErrorHandlerFactory;
import org.openjena.riot.lang.LabelToNode;
import org.openjena.riot.out.NodeToLabel;
import org.openjena.riot.out.OutputLangUtils;
import org.openjena.riot.system.IRIResolver;
import org.openjena.riot.system.ParserProfile;
import org.openjena.riot.system.ParserProfileBase;
import org.openjena.riot.system.Prologue;
import org.slf4j.Logger;

import com.hp.hpl.jena.graph.Node;

public class Utils {

    public static String toString(String[] args) {
	    StringBuffer sb = new StringBuffer();
	    sb.append("{");
	    for ( String arg : args ) sb.append(arg).append(", ");
	    if ( sb.length() > 2 ) sb.delete(sb.length()-2, sb.length());
	    sb.append("}");
	    return sb.toString();
	}

	public static final NodeToLabel nodeToLabel = NodeToLabel.createBNodeByLabelAsGiven();
    public static String serialize(Node node) {
        StringWriter out = new StringWriter();
        OutputLangUtils.output(out, node, null, nodeToLabel);
        return out.toString();
    }

    public static ParserProfile createParserProfile(JobContext context, Path path) {
        Prologue prologue = new Prologue(null, IRIResolver.createNoResolve()); 
        LabelToNode labelMapping = new MapReduceLabelToNode(context, path);
        return new ParserProfileBase(prologue, ErrorHandlerFactory.errorHandlerStd, labelMapping);
    }

	public static byte[] toHex(long id) {
	    byte[] b = new byte[16];
	    Hex.formatUnsignedLongHex(b, 0, id, 16);
	    return b;
	}

	public static void setReducers(Job job, Configuration configuration, Logger log) {
	    boolean runLocal = configuration.getBoolean("runLocal", true);
	    int num_reducers = configuration.getInt("numReducers", Constants.DEFAULT_NUM_REDUCERS);
	
	    // TODO: should we comment this out and let Hadoop decide the number of reducers?
	    if ( runLocal ) {
	    	if (log != null) log.debug("Setting number of reducers to {}", 1);
	        job.setNumReduceTasks(1);           
	    } else {
	    	// number of reducers must be the same as in FirstDriverAlternative to ensure offsets are correct
	    	if (log != null) log.debug("Setting number of reducers to {}", num_reducers);
	    	if ( "fourth".equals( job.getJobName() ) ) {
				job.setPartitionerClass(TotalOrderPartitioner.class);
				job.setNumReduceTasks(9 * num_reducers);
	    	} else {
	    		job.setNumReduceTasks(num_reducers);
	    	}
	    }
	}

	public static void log(Job job, Logger log) throws ClassNotFoundException {
		log.debug ("{} -> {} ({}, {}) -> {}#{} ({}, {}) -> {}", 
			new Object[]{
				job.getInputFormatClass().getSimpleName(), job.getMapperClass().getSimpleName(), job.getMapOutputKeyClass().getSimpleName(), job.getMapOutputValueClass().getSimpleName(), 
				job.getReducerClass().getSimpleName(), job.getNumReduceTasks(), job.getOutputKeyClass().getSimpleName(), job.getOutputValueClass().getSimpleName(), job.getOutputFormatClass().getSimpleName()
			}
		);
		Path[] inputs = FileInputFormat.getInputPaths(job);
		Path output = FileOutputFormat.getOutputPath(job);
		log.debug("input: {}", inputs[0]);
		log.debug("output: {}", output);
	}

}
