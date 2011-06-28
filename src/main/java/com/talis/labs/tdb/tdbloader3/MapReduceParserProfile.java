package com.talis.labs.tdb.tdbloader3;

import org.openjena.riot.ErrorHandler;
import org.openjena.riot.lang.LabelToNode;
import org.openjena.riot.system.ParserProfileBase;
import org.openjena.riot.system.Prologue;

public class MapReduceParserProfile extends ParserProfileBase {

    public MapReduceParserProfile(Prologue prologue, ErrorHandler errorHandler, LabelToNode labelMapping) {
        super (prologue, errorHandler, labelMapping);
    }

}
