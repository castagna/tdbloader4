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

package cmd;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.jena.tdbloader3.io.LongQuadWritable;
import org.apache.jena.tdbloader3.partitioners.InputSampler;
import org.apache.jena.tdbloader3.partitioners.Sampler;
import org.apache.jena.tdbloader3.partitioners.SplitSampler;
import org.apache.jena.tdbloader3.partitioners.TotalOrderPartitioner;

/**
 * Utility for collecting samples and writing a partition file for
 * {@link TotalOrderPartitioner}.
 */
public class sampler<K, V> extends Configured implements Tool {

	static int printUsage() {
		System.out.println("sampler -r <reduces>\n"
				+ "      [-inFormat <input format class>]\n"
				+ "      [-keyClass <map input & output key class>]\n"
				+ "       -splitSample <numSamples> <maxsplits> | "
				+ "             // Sample from first records in splits (random data)");
		System.out.println("Default sampler: -splitRandom 0.1 10000 10");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	public sampler(Configuration conf) {
		super(conf);
	}

	/**
	 * Driver for InputSampler from the command line. Configures a JobConf
	 * instance and calls {@link #writePartitionFile}.
	 */
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());
		ArrayList<String> otherArgs = new ArrayList<String>();
		Sampler<K, V> sampler = null;
		for (int i = 0; i < args.length; ++i) {
			try {
				if ("-r".equals(args[i])) {
					job.setNumReduceTasks(Integer.parseInt(args[++i]));
				} else if ("-inFormat".equals(args[i])) {
					job.setInputFormatClass(Class.forName(args[++i]).asSubclass(InputFormat.class));
				} else if ("-keyClass".equals(args[i])) {
					job.setMapOutputKeyClass(Class.forName(args[++i]).asSubclass(WritableComparable.class));
				} else if ("-splitSample".equals(args[i])) {
					int numSamples = Integer.parseInt(args[++i]);
					int maxSplits = Integer.parseInt(args[++i]);
					if (0 >= maxSplits) maxSplits = Integer.MAX_VALUE;
					sampler = new SplitSampler<K, V>(numSamples, maxSplits);
				} else {
					otherArgs.add(args[i]);
				}
			} catch (NumberFormatException except) {
				System.out.println("ERROR: Integer expected instead of " + args[i]);
				return printUsage();
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from " + args[i - 1]);
				return printUsage();
			}
		}
		if (job.getNumReduceTasks() <= 1) {
			System.err.println("Sampler requires more than one reducer");
			return printUsage();
		}
		if (otherArgs.size() < 2) {
			System.out.println("ERROR: Wrong number of parameters: ");
			return printUsage();
		}
		if (null == sampler) {
			sampler = new SplitSampler<K, V>(1000, 10);
		}

		Path outf = new Path(otherArgs.remove(otherArgs.size() - 1));
		TotalOrderPartitioner.setPartitionFile(getConf(), outf);
		for (String s : otherArgs) {
			FileInputFormat.addInputPath(job, new Path(s));
		}
		InputSampler.<K, V> writePartitionFile(job, sampler);

		return 0;
	}

    public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new sampler<LongQuadWritable, NullWritable>(new Configuration()), args);
		System.exit(res);
	}

}
