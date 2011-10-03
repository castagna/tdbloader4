package cmd;

import static com.hp.hpl.jena.sparql.util.Utils.nowAsString;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.jena.tdbloader3.FirstDriver;
import org.apache.jena.tdbloader3.FourthDriver;
import org.apache.jena.tdbloader3.NodeTableRewriter;
import org.apache.jena.tdbloader3.SecondDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.tdb.base.file.Location;
import com.hp.hpl.jena.tdb.store.DatasetGraphTDB;
import com.hp.hpl.jena.tdb.store.bulkloader.BulkLoader;
import com.hp.hpl.jena.tdb.store.bulkloader2.ProgressLogger;
import com.hp.hpl.jena.tdb.sys.Names;
import com.hp.hpl.jena.tdb.sys.SetupTDB;

public class download extends Configured implements Tool {

    private static final Logger log = LoggerFactory.getLogger(download.class);

    public download() {
		super();
        log.debug("constructed with no configuration.");
	}
    
    public download(Configuration configuration) {
		super(configuration);
        log.debug("constructed with configuration.");
	}    

    @Override
	public int run(String[] args) throws Exception {
		if ( args.length != 3 ) {
			System.err.printf("Usage: %s [generic options] <input node table> <input b+tree indexes> <output>\n", getClass().getName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		Configuration configuration = getConf();

        Location location = new Location(args[2]);
        DatasetGraphTDB dsgDisk = SetupTDB.buildDataset(location) ;
        dsgDisk.sync(); 
        dsgDisk.close();

        FileSystem fs = FileSystem.get(configuration);

        new File(args[1], "nodes.dat").delete() ;
        mergeToLocalFile(fs, new Path(args[0]), args[2], configuration);
        copyToLocalFile(fs, new Path(args[1]), new Path(args[2]));

        // TODO: this is a sort of a cheat and it could go away (if it turns out to be too slow)!
        fixNodeTable2(location);

		return 0;
	}
	
	private void mergeToLocalFile ( FileSystem fs, Path src, String outPath, Configuration configuration ) throws FileNotFoundException, IOException {
		FileStatus[] status = fs.listStatus(src);
		Map<String, Path> paths = new TreeMap<String, Path>();
		for ( FileStatus fileStatus : status ) {
            Path path = fileStatus.getPath();
            String pathName = path.getName();
            if ( pathName.startsWith(SecondDriver.NAME) ) {
            	paths.put(pathName, path);
            }
        }

		File outFile = new File(outPath, Names.indexId2Node + ".dat");
        OutputStream out = new FileOutputStream(outFile);
        for (String pathName : paths.keySet()) {
        	Path path = new Path(src, paths.get(pathName));
        	log.debug("Concatenating {} into {}...", path.toUri(), outFile.getAbsoluteFile());
        	InputStream in = fs.open(new Path(path, Names.indexId2Node + ".dat"));
        	IOUtils.copyBytes(in, out, configuration, false);
        	in.close();			
		}
		out.close();
	}
	
	private void copyToLocalFile ( FileSystem fs, Path src, Path dst ) throws FileNotFoundException, IOException {
		FileStatus[] status = fs.listStatus(src);
		for ( FileStatus fileStatus : status ) {
            Path path = fileStatus.getPath();
            String pathName = path.getName();
            if ( pathName.startsWith(FirstDriver.NAME) || pathName.startsWith(FourthDriver.NAME) ) {
            	copyToLocalFile(fs, path, dst);
            }
            if ( pathName.endsWith(".idn") || pathName.endsWith(".dat") ) {
                fs.copyToLocalFile(path, new Path(dst, path.getName()));
            }
        }
	}

	public static void fixNodeTable2(Location location) throws IOException {
	    ProgressLogger monitor = new ProgressLogger(log, "Data (1/2)", BulkLoader.DataTickPoint,BulkLoader.superTick) ;
	    log.info("Data (1/2)...");
	    monitor.start();
	    NodeTableRewriter.fixNodeTable2(location, log, monitor);
	    long time = monitor.finish() ;
        long total = monitor.getTicks() ;
        float elapsedSecs = time/1000F ;
        float rate = (elapsedSecs!=0) ? total/elapsedSecs : 0 ;
        String str =  String.format("Total: %,d RDF nodes : %,.2f seconds : %,.2f nodes/sec [%s]", total, elapsedSecs, rate, nowAsString()) ;
        log.info(str);
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new download(), args);
		System.exit(exitCode);
	}

}
