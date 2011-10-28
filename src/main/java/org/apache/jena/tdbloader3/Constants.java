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
