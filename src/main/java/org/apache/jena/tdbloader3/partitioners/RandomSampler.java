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
import java.util.Random;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sample from random points in the input. General-purpose sampler. Takes
 * numSamples / maxSplitsSampled inputs from each split.
 */
public class RandomSampler<K, V> implements Sampler<K, V> {
	private double freq;
	private final int numSamples;
	private final int maxSplitsSampled;

	private static final Logger log = LoggerFactory.getLogger(RandomSampler.class);

	/**
	 * Create a new RandomSampler sampling <em>all</em> splits. This will read
	 * every split at the client, which is very expensive.
	 * 
	 * @param freq
	 *            Probability with which a key will be chosen.
	 * @param numSamples
	 *            Total number of samples to obtain from all selected splits.
	 */
	public RandomSampler(double freq, int numSamples) {
		this(freq, numSamples, Integer.MAX_VALUE);
	}

	/**
	 * Create a new RandomSampler.
	 * 
	 * @param freq
	 *            Probability with which a key will be chosen.
	 * @param numSamples
	 *            Total number of samples to obtain from all selected splits.
	 * @param maxSplitsSampled
	 *            The maximum number of splits to examine.
	 */
	public RandomSampler(double freq, int numSamples, int maxSplitsSampled) {
		this.freq = freq;
		this.numSamples = numSamples;
		this.maxSplitsSampled = maxSplitsSampled;
	}

	/**
	 * Randomize the split order, then take the specified number of keys from
	 * each split sampled, where each key is selected with the specified
	 * probability and possibly replaced by a subsequently selected key when the
	 * quota of keys from that split is satisfied.
	 */
	@SuppressWarnings("unchecked")
	// ArrayList::toArray doesn't preserve type
	public K[] getSample(InputFormat<K, V> inf, Job job) throws IOException, InterruptedException {
		List<InputSplit> splits = inf.getSplits(job);
		ArrayList<K> samples = new ArrayList<K>(numSamples);
		int splitsToSample = Math.min(maxSplitsSampled, splits.size());

		Random r = new Random();
		long seed = r.nextLong();
		r.setSeed(seed);
		log.debug("seed: {}", seed);
		// shuffle splits
		for (int i = 0; i < splits.size(); ++i) {
			InputSplit tmp = splits.get(i);
			int j = r.nextInt(splits.size());
			splits.set(i, splits.get(j));
			splits.set(j, tmp);
		}
		// our target rate is in terms of the maximum number of sample splits,
		// but we accept the possibility of sampling additional splits to hit
		// the target sample keyset
		for (int i = 0; i < splitsToSample || (i < splits.size() && samples.size() < numSamples); ++i) {
			TaskAttemptContext samplingContext = new TaskAttemptContext(job.getConfiguration(), new TaskAttemptID());
			RecordReader<K, V> reader = inf.createRecordReader(splits.get(i), samplingContext);
			reader.initialize(splits.get(i), samplingContext);
			while (reader.nextKeyValue()) {
				if (r.nextDouble() <= freq) {
					if (samples.size() < numSamples) {
						samples.add(ReflectionUtils.copy(job.getConfiguration(), reader.getCurrentKey(), null));
					} else {
						// When exceeding the maximum number of samples, replace
						// a
						// random element with this one, then adjust the
						// frequency
						// to reflect the possibility of existing elements being
						// pushed out
						int ind = r.nextInt(numSamples);
						if (ind != numSamples) {
							samples.set(ind, ReflectionUtils.copy(job.getConfiguration(), reader.getCurrentKey(), null));
						}
						freq *= (numSamples - 1) / (double) numSamples;
					}
				}
			}
			reader.close();
		}
		return (K[]) samples.toArray();
	}
}
