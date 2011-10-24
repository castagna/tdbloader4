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
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.jena.tdbloader3.io.LongQuadWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility for collecting samples and writing a partition file for
 * {@link TotalOrderPartitioner}.
 */
public class InputSampler<K, V> {

	private static final Logger log = LoggerFactory.getLogger(InputSampler.class);
	
	/**
	 * Write a partition file for the given job, using the Sampler provided.
	 * Queries the sampler for a sample keyset, sorts by the output key
	 * comparator, selects the keys for each rank, and writes to the destination
	 * returned from {@link TotalOrderPartitioner#getPartitionFile}.
	 */
	@SuppressWarnings("unchecked")
	// getInputFormat, getOutputKeyComparator
	public static <K, V> void writePartitionFile(Job job, Sampler<K, V> sampler) throws IOException, ClassNotFoundException, InterruptedException {
		log.debug("writePartitionFile({},{})", job, sampler);
		Configuration conf = job.getConfiguration();
		@SuppressWarnings("rawtypes")
		final InputFormat inf = ReflectionUtils.newInstance(job.getInputFormatClass(), conf);
		int numPartitions = job.getNumReduceTasks() / 9;
		log.debug("Number of partitions is {} for each index", numPartitions);
		K[] samples = sampler.getSample(inf, job);
		log.info("Using " + samples.length + " samples");
		writePartitionFile(samples, "GSPO", job, conf, numPartitions);
		writePartitionFile(samples, "GPOS", job, conf, numPartitions);
		writePartitionFile(samples, "GOSP", job, conf, numPartitions);
		writePartitionFile(samples, "SPOG", job, conf, numPartitions);
		writePartitionFile(samples, "POSG", job, conf, numPartitions);
		writePartitionFile(samples, "OSPG", job, conf, numPartitions);
		writePartitionFile(samples, "SPO", job, conf, numPartitions);
		writePartitionFile(samples, "POS", job, conf, numPartitions);
		writePartitionFile(samples, "OSP", job, conf, numPartitions);
	}

	private static <K> void writePartitionFile(K[] samples, String indexName, Job job, Configuration conf, int numPartitions) throws IOException {
		@SuppressWarnings("unchecked")
		RawComparator<K> comparator = (RawComparator<K>) job.getSortComparator();
		K[] shuffledSamples = reshuffleSamples(samples, indexName, comparator, numPartitions);
		log.debug("Size of permutated samples is {}", shuffledSamples.length);
		Path dst = new Path(TotalOrderPartitioner.getPartitionFile(conf) + "_" + indexName);
		log.debug("Writing to {}", dst);
		FileSystem fs = dst.getFileSystem(conf);
		if (fs.exists(dst)) {
			fs.delete(dst, false);
		}
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, dst, job.getMapOutputKeyClass(), NullWritable.class);
		NullWritable nullValue = NullWritable.get();
		float stepSize = shuffledSamples.length / (float) numPartitions;
		log.debug("Step size is {}", stepSize);
		int last = -1;
		for (int i = 1; i < numPartitions; ++i) {
			int k = Math.round(stepSize * i);
			while (last >= k && comparator.compare(shuffledSamples[last], shuffledSamples[k]) == 0) {
				++k;
			}
			log.debug("Writing ({},{})", shuffledSamples[k], nullValue);
			writer.append(shuffledSamples[k], nullValue);
			last = k;
		}
		log.debug("Closing {}", dst);
		writer.close();
	}
	
	@SuppressWarnings("unchecked")
	private static <K> K[] reshuffleSamples(K[] samples, String indexName, RawComparator<K> comparator, int numPartitions) {
		ArrayList<K> shuffledSamples = new ArrayList<K>(samples.length);
		for (K sample : samples) {
			if ( sample instanceof LongQuadWritable ) {
				LongQuadWritable q = (LongQuadWritable)sample;
				
				if ( (( q.get(3) != -1L ) && (indexName.length() == 4)) || (( q.get(3) == -1L ) && (indexName.length() == 3)) ) {
					LongQuadWritable shuffledQuad = null;
					if ( indexName.equals("GSPO") ) shuffledQuad = new LongQuadWritable(q.get(3), q.get(0), q.get(1), q.get(2));
					else if ( indexName.equals("GPOS") ) shuffledQuad = new LongQuadWritable(q.get(3), q.get(1), q.get(2), q.get(0));
					else if ( indexName.equals("GOSP") ) shuffledQuad = new LongQuadWritable(q.get(3), q.get(2), q.get(0), q.get(1));
					else if ( indexName.equals("SPOG") ) shuffledQuad = new LongQuadWritable(q.get(0), q.get(1), q.get(2), q.get(3));
					else if ( indexName.equals("POSG") ) shuffledQuad = new LongQuadWritable(q.get(1), q.get(2), q.get(0), q.get(3));
					else if ( indexName.equals("OSPG") ) shuffledQuad = new LongQuadWritable(q.get(2), q.get(0), q.get(1), q.get(3));
					else if ( indexName.equals("SPO") ) shuffledQuad = new LongQuadWritable(q.get(0), q.get(1), q.get(2), -1L);
					else if ( indexName.equals("POS") ) shuffledQuad = new LongQuadWritable(q.get(1), q.get(2), q.get(0), -1L);
					else if ( indexName.equals("OSP") ) shuffledQuad = new LongQuadWritable(q.get(2), q.get(0), q.get(1), -1L);
					shuffledSamples.add((K)shuffledQuad);
				}
				
			}
		}

		// This is to ensure we always have the same amount of split points
		int size = shuffledSamples.size();
		if ( size < numPartitions ) {
			log.debug("Found only {} samples for {} partitions...", shuffledSamples.size(), numPartitions);
			for (int i = 0; i < numPartitions - size; i++) {
				long id = 100L * (i + 1);
				K key = (K)new LongQuadWritable (id, id, id, indexName.length() == 3 ? -1L : id );
				log.debug("Added fake sample: {}", key);
				shuffledSamples.add(key);
			}
		}

		K[] result = (K[]) shuffledSamples.toArray();
		Arrays.sort(result, comparator);

		return result;
	}

}