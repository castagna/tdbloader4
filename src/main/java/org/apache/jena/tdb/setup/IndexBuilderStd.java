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

package org.apache.jena.tdb.setup;

import com.hp.hpl.jena.tdb.base.file.FileSet;
import com.hp.hpl.jena.tdb.base.record.RecordFactory;
import com.hp.hpl.jena.tdb.index.Index;

public class IndexBuilderStd implements IndexBuilder
{
    private RangeIndexBuilderStd other ;

    public IndexBuilderStd(BlockMgrBuilder bMgr1, BlockMgrBuilder bMgr2)
    {
        this.other = new RangeIndexBuilderStd(bMgr1, bMgr2) ;
    }
    
    public Index buildIndex(FileSet fileSet, RecordFactory recordFactory)
    {
        // Cheap.
        return other.buildRangeIndex(fileSet, recordFactory) ;
    }
}
