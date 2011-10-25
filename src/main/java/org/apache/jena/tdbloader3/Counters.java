package org.apache.jena.tdbloader3;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

public class Counters {

    private Counter counterTriples = null;
    private Counter counterQuads = null;
    private Counter counterDuplicates = null;
    private Counter counterMalformed = null;
    private Counter counterRdfNodes = null;
    
    private long numTriples = 0L;
    private long numQuads = 0L;
    private long numDuplicates = 0L;
    private long numMalformed = 0L;
    private long numRdfNodes = 0L;
    private long n = 0L;

	public Counters (TaskInputOutputContext<?,?,?,?> context) {
    	counterTriples = context.getCounter(Constants.TDBLOADER3_COUNTER_GROUPNAME, Constants.TDBLOADER3_COUNTER_TRIPLES);
        counterQuads = context.getCounter(Constants.TDBLOADER3_COUNTER_GROUPNAME, Constants.TDBLOADER3_COUNTER_QUADS);
    	counterMalformed = context.getCounter(Constants.TDBLOADER3_COUNTER_GROUPNAME, Constants.TDBLOADER3_COUNTER_MALFORMED);
    	counterDuplicates = context.getCounter(Constants.TDBLOADER3_COUNTER_GROUPNAME, Constants.TDBLOADER3_COUNTER_DUPLICATES);
    	counterRdfNodes = context.getCounter(Constants.TDBLOADER3_COUNTER_GROUPNAME, Constants.TDBLOADER3_COUNTER_RDFNODES);

    	counterTriples.increment(0L);
    	counterQuads.increment(0L);
    	counterMalformed.increment(0L);
    	counterDuplicates.increment(0L);
    	counterRdfNodes.increment(0L);
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

	public void close() {
		increment();
	}
	
	private void report() {
		if ( n > 1000 ) {
			increment();
		}
	}
	
	private void increment() {
		counterTriples.increment(numTriples);
		counterQuads.increment(numQuads);
		counterMalformed.increment(numMalformed);
		counterDuplicates.increment(numDuplicates);
		counterRdfNodes.increment(numRdfNodes);

	    numTriples = 0L;
	    numQuads = 0L;
	    numDuplicates = 0L;
	    numMalformed = 0L;
	    numRdfNodes = 0L;

		n = 0L;
	}
	
}
