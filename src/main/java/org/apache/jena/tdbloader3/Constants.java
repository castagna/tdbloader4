package org.apache.jena.tdbloader3;

public class Constants {

	public static final String TDBLOADER3_COUNTER_GROUPNAME = "TDBLoader3 Counters";
	public static final String TDBLOADER3_COUNTER_MALFORMED = "Malformed";
	public static final String TDBLOADER3_COUNTER_QUADS = "Quads (including duplicates)";
	public static final String TDBLOADER3_COUNTER_TRIPLES = "Triples (including duplicates)";
	public static final String TDBLOADER3_COUNTER_DUPLICATES = "Duplicates (quads or triples)";
	public static final String TDBLOADER3_COUNTER_RDFNODES = "RDF nodes";
	public static final String TDBLOADER3_COUNTER_RECORDS = "Index records";

	public static final int DEFAULT_NUM_REDUCERS = 10;

	public static String[] indexNames = new String[] {
	    "SPO",
	    "POS",
	    "OSP",
	    "GSPO",
	    "GPOS",
	    "GOSP",
	    "SPOG",
	    "POSG",
	    "OSPG"
	};

}
