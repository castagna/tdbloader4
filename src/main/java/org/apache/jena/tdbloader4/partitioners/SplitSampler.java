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

package org.apache.jena.tdbloader4.partitioners;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.jena.tdbloader4.io.LongQuadWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Samples the first n records from s splits. 
 * Inexpensive way to sample random data.
 */
public class SplitSampler<K, V> implements Sampler<K, V> {

    private static final Logger log = LoggerFactory.getLogger(SplitSampler.class);
	
	private final int numSamples;
	private final int maxSplitsSampled;

	/**
	 * Create a SplitSampler sampling <em>all</em> splits. Takes the first
	 * numSamples / numSplits records from each split.
	 * 
	 * @param numSamples Total number of samples to obtain from all selected splits.
	 */
	public SplitSampler(int numSamples) {
		this(numSamples, Integer.MAX_VALUE);
	}

	/**
	 * Create a new SplitSampler.
	 * 
	 * @param numSamples Total number of samples to obtain from all selected splits.
	 * @param maxSplitsSampled The maximum number of splits to examine.
	 */
	public SplitSampler(int numSamples, int maxSplitsSampled) {
		log.debug("SplitSampler({}, {})", numSamples, maxSplitsSampled);
		this.numSamples = numSamples;
		this.maxSplitsSampled = maxSplitsSampled;
	}

	/**
	 * From each split sampled, take the first numSamples / numSplits records.
	 */
	@SuppressWarnings("unchecked")
	// ArrayList::toArray doesn't preserve type
	public K[] getSample(InputFormat<K, V> inf, Job job) throws IOException, InterruptedException {
		List<InputSplit> splits = inf.getSplits(job);
		ArrayList<K> samples = new ArrayList<K>(numSamples);
		int splitsToSample = Math.min(maxSplitsSampled, splits.size());
		int samplesPerSplit = numSamples / splitsToSample;
		log.debug("Sampling {} splits, taking {} samples per split", splitsToSample, samplesPerSplit);
		long records = 0;
		for (int i = 0; i < splitsToSample; ++i) {
			TaskAttemptContext samplingContext = new TaskAttemptContext(job.getConfiguration(), new TaskAttemptID());
			InputSplit split = splits.get(i);
			log.debug("Sampling {} split", split);
			RecordReader<K, V> reader = inf.createRecordReader(split, samplingContext);
			reader.initialize(split, samplingContext);
			while ( reader.nextKeyValue() ) {
				LongQuadWritable currentKey = (LongQuadWritable)reader.getCurrentKey() ;
				// TODO: why do we need to do that? Why on earth we have -1 in subject, predicate or object position???
				if ( ( currentKey.get(0) > 0 ) && ( currentKey.get(1) > 0 ) && ( currentKey.get(2) > 0 ) ) {
					LongQuadWritable key = new LongQuadWritable(currentKey.get(0), currentKey.get(1), currentKey.get(2), currentKey.get(3));
					log.debug("Sampled {}", key);
					samples.add((K)key);
					++records;
					if ( records >= (i + 1) * samplesPerSplit ) {
						log.debug("Records is {} and (i + 1) * samplesPerSplit is {}", records, (i + 1) * samplesPerSplit);
						break;
					}					
				}
			}
			reader.close();
		}
		return (K[]) samples.toArray();
	}

}