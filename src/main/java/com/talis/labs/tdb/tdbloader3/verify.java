/*
 * Copyright 2010,2011 Talis Systems Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.talis.labs.tdb.tdbloader3;

import static com.talis.labs.tdb.tdbloader3.tdbloader3.dump;
import static com.talis.labs.tdb.tdbloader3.tdbloader3.isomorphic;
import static com.talis.labs.tdb.tdbloader3.tdbloader3.load;

import org.apache.hadoop.util.ToolRunner;

import com.hp.hpl.jena.tdb.base.file.Location;
import com.hp.hpl.jena.tdb.store.DatasetGraphTDB;
import com.hp.hpl.jena.tdb.sys.SetupTDB;

public class verify {

	public static void main(String[] args) throws Exception {
		if ( args.length != 2 ) {
			System.err.printf("Usage: verify <input> <input>\n");
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(1);
		}
		
        DatasetGraphTDB dsgMem = load(args[0]);
        Location location = new Location(args[1]);
        DatasetGraphTDB dsgDisk = SetupTDB.buildDataset(location) ;
        boolean isomorphic = isomorphic ( dsgMem, dsgDisk );
        System.out.println ("> " + isomorphic);
        
        if ( !isomorphic ) {
        	System.out.println( dump ( dsgMem, dsgDisk ) );
        }

		System.exit(0);
	}
	
}
