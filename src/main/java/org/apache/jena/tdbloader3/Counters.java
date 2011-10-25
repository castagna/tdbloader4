package org.apache.jena.tdbloader3;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

public class Counters {

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
    	counterTriples = context.getCounter(Constants.TDBLOADER3_COUNTER_GROUPNAME, Constants.TDBLOADER3_COUNTER_TRIPLES);
        counterQuads = context.getCounter(Constants.TDBLOADER3_COUNTER_GROUPNAME, Constants.TDBLOADER3_COUNTER_QUADS);
    	counterMalformed = context.getCounter(Constants.TDBLOADER3_COUNTER_GROUPNAME, Constants.TDBLOADER3_COUNTER_MALFORMED);
    	counterDuplicates = context.getCounter(Constants.TDBLOADER3_COUNTER_GROUPNAME, Constants.TDBLOADER3_COUNTER_DUPLICATES);
    	counterRdfNodes = context.getCounter(Constants.TDBLOADER3_COUNTER_GROUPNAME, Constants.TDBLOADER3_COUNTER_RDFNODES);
    	counterRecords = context.getCounter(Constants.TDBLOADER3_COUNTER_GROUPNAME, Constants.TDBLOADER3_COUNTER_RECORDS);
	}

	public void incrementTriples() {
		numTriples++;
		n++;
		report();
	}
	
	public void incrementQuads() {
		numQuads++;
		n++;
		report();
	}
	
	public void incrementDuplicates() {
		numDuplicates++;
		n++;
		report();
	}
	
	public void incrementMalformed() {
		numMalformed++;
		n++;
		report();
	}

	public void incrementRdfNodes() {
		numRdfNodes++;
		n++;
		report();
	}

	public void incrementRecords() {
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
		if ( numTriples != 0L ) counterTriples.increment(numTriples);
		if ( numQuads != 0L ) counterQuads.increment(numQuads);
		if ( numMalformed != 0L ) counterMalformed.increment(numMalformed);
		if ( numDuplicates != 0L ) counterDuplicates.increment(numDuplicates);
		if ( numRdfNodes != 0L ) counterRdfNodes.increment(numRdfNodes);
		if ( numRecords != 0L ) counterRecords.increment(numRecords);

	    numTriples = 0L;
	    numQuads = 0L;
	    numDuplicates = 0L;
	    numMalformed = 0L;
	    numRdfNodes = 0L;
	    numRecords = 0L;

		n = 0L;
	}
	
}
