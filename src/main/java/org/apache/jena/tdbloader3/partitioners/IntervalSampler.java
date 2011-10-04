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

package org.apache.jena.tdbloader3.partitioners;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Sample from s splits at regular intervals. Useful for sorted data.
 */
public class IntervalSampler<K, V> implements Sampler<K, V> {

	private final double freq;
	private final int maxSplitsSampled;

	/**
	 * Create a new IntervalSampler sampling <em>all</em> splits.
	 * 
	 * @param freq
	 *            The frequency with which records will be emitted.
	 */
	public IntervalSampler(double freq) {
		this(freq, Integer.MAX_VALUE);
	}

	/**
	 * Create a new IntervalSampler.
	 * 
	 * @param freq
	 *            The frequency with which records will be emitted.
	 * @param maxSplitsSampled
	 *            The maximum number of splits to examine.
	 * @see #getSample
	 */
	public IntervalSampler(double freq, int maxSplitsSampled) {
		this.freq = freq;
		this.maxSplitsSampled = maxSplitsSampled;
	}

	/**
	 * For each split sampled, emit when the ratio of the number of records
	 * retained to the total record count is less than the specified frequency.
	 */
	@SuppressWarnings("unchecked")
	// ArrayList::toArray doesn't preserve type
	public K[] getSample(InputFormat<K, V> inf, Job job) throws IOException, InterruptedException {
		List<InputSplit> splits = inf.getSplits(job);
		ArrayList<K> samples = new ArrayList<K>();
		int splitsToSample = Math.min(maxSplitsSampled, splits.size());
		long records = 0;
		long kept = 0;
		for (int i = 0; i < splitsToSample; ++i) {
			TaskAttemptContext samplingContext = new TaskAttemptContext(job.getConfiguration(), new TaskAttemptID());
			RecordReader<K, V> reader = inf.createRecordReader(splits.get(i), samplingContext);
			reader.initialize(splits.get(i), samplingContext);
			while (reader.nextKeyValue()) {
				++records;
				if ((double) kept / records < freq) {
					samples.add(ReflectionUtils.copy(job.getConfiguration(), reader.getCurrentKey(), null));
					++kept;
				}
			}
			reader.close();
		}
		return (K[]) samples.toArray();
	}

}
