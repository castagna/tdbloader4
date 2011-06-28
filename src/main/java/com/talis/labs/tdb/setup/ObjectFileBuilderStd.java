package com.talis.labs.tdb.setup;

import com.hp.hpl.jena.tdb.base.file.FileFactory;
import com.hp.hpl.jena.tdb.base.file.FileSet;
import com.hp.hpl.jena.tdb.base.objectfile.ObjectFile;

public class ObjectFileBuilderStd implements ObjectFileBuilder
{
    public ObjectFile buildObjectFile(FileSet fileSet, String ext)
    {
        if ( fileSet.isMem() )
            return FileFactory.createObjectFileMem() ;
        String filename = fileSet.filename(ext) ;
        return FileFactory.createObjectFileDisk(filename) ;
    }
}