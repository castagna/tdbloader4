package com.talis.labs.tdb.setup;

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
