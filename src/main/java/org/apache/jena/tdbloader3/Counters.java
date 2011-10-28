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

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

public class Counters {

	private final TaskInputOutputContext<?,?,?,?> context;
	
    private Counter counterTriples = null;
    private Counter counterQuads = null;
    private Counter counterDuplicates = null;
    private Counter counterMalformed = null;
    private Counter counterRdfNodes = null;
    private Counter counterRecords = null;
    
    private long numTriples = 0L;
    private long numQuads = 0L;
    private long numDuplicates = 0L;
    private long numMalformed = 0L;
    private long numRdfNodes = 0L;
    private long numRecords = 0L;
    private long n = 0L;

	public Counters (TaskInputOutputContext<?,?,?,?> context) {
		this.context = context;
	}

	public void incrementTriples() {
		if ( counterTriples == null ) counterTriples = context.getCounter(Constants.TDBLOADER3_COUNTER_GROUPNAME, Constants.TDBLOADER3_COUNTER_TRIPLES);
		numTriples++;
		n++;
		report();
	}
	
	public void incrementQuads() {
		if ( counterQuads == null ) counterQuads = context.getCounter(Constants.TDBLOADER3_COUNTER_GROUPNAME, Constants.TDBLOADER3_COUNTER_QUADS);
		numQuads++;
		n++;
		report();
	}
	
	public void incrementDuplicates() {
		if ( counterDuplicates == null ) counterDuplicates = context.getCounter(Constants.TDBLOADER3_COUNTER_GROUPNAME, Constants.TDBLOADER3_COUNTER_DUPLICATES);
		numDuplicates++;
		n++;
		report();
	}
	
	public void incrementMalformed() {
		if ( counterMalformed == null ) counterMalformed = context.getCounter(Constants.TDBLOADER3_COUNTER_GROUPNAME, Constants.TDBLOADER3_COUNTER_MALFORMED);
		numMalformed++;
		n++;
		report();
	}

	public void incrementRdfNodes() {
		if ( counterRdfNodes == null ) counterRdfNodes = context.getCounter(Constants.TDBLOADER3_COUNTER_GROUPNAME, Constants.TDBLOADER3_COUNTER_RDFNODES);
		numRdfNodes++;
		n++;
		report();
	}

	public void incrementRecords() {
		if ( counterRecords == null ) counterRecords = context.getCounter(Constants.TDBLOADER3_COUNTER_GROUPNAME, Constants.TDBLOADER3_COUNTER_RECORDS);
		numRecords++;
		n++;
		report();
	}

	public void close() {
		increment();
	}
	
	private void report() {
		if ( n > 1000 ) {
			increment();
		}
	}
	
	private void increment() {
		if ( ( numTriples != 0L ) && ( counterTriples != null ) ) counterTriples.increment(numTriples);
		if ( ( numQuads != 0L ) && ( counterQuads != null ) ) counterQuads.increment(numQuads);
		if ( ( numMalformed != 0L ) && ( counterMalformed != null ) ) counterMalformed.increment(numMalformed);
		if ( ( numDuplicates != 0L ) && ( counterDuplicates != null ) ) counterDuplicates.increment(numDuplicates);
		if ( ( numRdfNodes != 0L ) && ( counterRdfNodes != null ) ) counterRdfNodes.increment(numRdfNodes);
		if ( ( numRecords != 0L ) && ( counterRecords != null ) ) counterRecords.increment(numRecords);

	    numTriples = 0L;
	    numQuads = 0L;
	    numDuplicates = 0L;
	    numMalformed = 0L;
	    numRdfNodes = 0L;
	    numRecords = 0L;

		n = 0L;
	}
	
}
