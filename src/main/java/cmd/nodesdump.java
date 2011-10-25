package cmd;

import java.nio.ByteBuffer;
import java.util.Iterator;

import org.openjena.atlas.lib.Pair;

import com.hp.hpl.jena.tdb.base.file.FileFactory;
import com.hp.hpl.jena.tdb.base.objectfile.ObjectFile;

public class nodesdump {
	public static void main(String[] args) {
		if ( args.length != 1 ) { print_usage(); }
		
		ObjectFile objects = FileFactory.createObjectFileDisk(args[0]);
		Iterator<Pair<Long, ByteBuffer>> iter = objects.all();
		while ( iter.hasNext() ) {
			Pair<Long, ByteBuffer> pair = iter.next();
			System.out.println(pair.getLeft() + " : " + pair.getRight());
		}
	}

	private static void print_usage() {
		System.out.println("Usage: nodesdump <nodes.dat>");
		System.exit(0);		
	}

}
