package com.talis.labs.tdb.tdbloader3;

import java.io.StringWriter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobID;
import org.openjena.atlas.lib.Hex;
import org.openjena.riot.ErrorHandlerFactory;
import org.openjena.riot.lang.LabelToNode;
import org.openjena.riot.out.NodeToLabel;
import org.openjena.riot.out.OutputLangUtils;
import org.openjena.riot.system.IRIResolver;
import org.openjena.riot.system.ParserProfile;
import org.openjena.riot.system.ParserProfileBase;
import org.openjena.riot.system.Prologue;

import com.hp.hpl.jena.graph.Node;
import com.talis.labs.tdb.tdbloader3.io.MapReduceLabelToNode;

public class Utils {

	public static String toString(String[] args) {
	    StringBuffer sb = new StringBuffer();
	    sb.append("{");
	    for ( String arg : args ) sb.append(arg).append(", ");
	    if ( sb.length() > 2 ) sb.delete(sb.length()-2, sb.length());
	    sb.append("}");
	    return sb.toString();
	}

	public static final NodeToLabel nodeToLabel = NodeToLabel.createBNodeByLabelRaw();
    public static String serialize(Node node) {
        StringWriter out = new StringWriter();
        OutputLangUtils.output(out, node, null, nodeToLabel);
        return out.toString();
    }

    public static ParserProfile createParserProfile(JobID jobId, Path path) {
        Prologue prologue = new Prologue(null, IRIResolver.createNoResolve()); 
        LabelToNode labelMapping = new MapReduceLabelToNode(jobId, path);
        return new ParserProfileBase(prologue, ErrorHandlerFactory.errorHandlerStd, labelMapping);
    }

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
	public static byte[] toHex(long id) {
	    byte[] b = new byte[16];
	    Hex.formatUnsignedLongHex(b, 0, id, 16);
	    return b;
	}
}
