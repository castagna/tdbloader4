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

import org.openjena.atlas.event.EventType;

public class Constants {

	// Job names
	
	public static final String NAME_INFER = "tdbloader3-inference";
	public static final String NAME_FIRST = "tdbloader3-first";
	public static final String NAME_SECOND = "tdbloader3-second";
	public static final String NAME_THIRD = "tdbloader3-third";
	public static final String NAME_FOURTH = "tdbloader3-fourth";

	// Generic options and their default values
	
	public static final String OPTION_USE_COMPRESSION = "useCompression";
	public static final boolean OPTION_USE_COMPRESSION_DEFAULT = false;

	public static final String OPTION_OVERRIDE_OUTPUT = "overrideOutput";
	public static final boolean OPTION_OVERRIDE_OUTPUT_DEFAULT = false;

	public static final String OPTION_COPY_TO_LOCAL = "copyToLocal";
	public static final boolean OPTION_COPY_TO_LOCAL_DEFAULT = false;

	public static final String OPTION_VERIFY = "verify";
	public static final boolean OPTION_VERIFY_DEFAULT = false;

	public static final String OPTION_RUN_LOCAL = "runLocal";
	public static final boolean OPTION_RUN_LOCAL_DEFAULT = false;

	public static final String OPTION_NUM_REDUCERS = "numReducers";
	public static final int OPTION_NUM_REDUCERS_DEFAULT = 20;

	public static final String OPTION_NUM_SAMPLES = "numSamples";
	public static final int OPTION_NUM_SAMPLES_DEFAULT = 1000;

	public static final String OPTION_MAX_SPLITS_SAMPLED = "maxSplitsSampled";
	public static final int OPTION_MAX_SPLITS_SAMPLED_DEFAULT = 10;

	// Non configurable options

	public static final String RUN_ID = "runId";
	public static final String OUTPUT_PATH_POSTFIX_1 = "_1";
	public static final String OUTPUT_PATH_POSTFIX_2 = "_2";
	public static final String OUTPUT_PATH_POSTFIX_3 = "_3";
	public static final String OUTPUT_PATH_POSTFIX_4 = "_4";
	public static final String OFFSETS_FILENAME = "offsets.txt";

	// MapReduce counters
	
	public static final String TDBLOADER3_COUNTER_GROUPNAME = "TDBLoader3 Counters";
	public static final String TDBLOADER3_COUNTER_MALFORMED = "Malformed";
	public static final String TDBLOADER3_COUNTER_QUADS = "Quads (including duplicates)";
	public static final String TDBLOADER3_COUNTER_TRIPLES = "Triples (including duplicates)";
	public static final String TDBLOADER3_COUNTER_DUPLICATES = "Duplicates (quads or triples)";
	public static final String TDBLOADER3_COUNTER_RDFNODES = "RDF nodes";
	public static final String TDBLOADER3_COUNTER_RECORDS = "Index records";

	// TDB index names
	
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

	// Event types
	
	public static EventType eventQuad = new EventType("quad");
	public static EventType eventTriple = new EventType("triple");
	public static EventType eventDuplicate = new EventType("duplicate");
	public static EventType eventMalformed = new EventType("malformed");
	public static EventType eventRdfNode = new EventType("RDF node");
	public static EventType eventRecord = new EventType("record");

	public static EventType eventTick = new EventType("tick");

}
