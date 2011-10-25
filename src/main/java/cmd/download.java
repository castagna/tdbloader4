package cmd;

import static com.hp.hpl.jena.sparql.util.Utils.nowAsString;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.jena.tdbloader3.Constants;
import org.apache.jena.tdbloader3.FourthDriver;
import org.apache.jena.tdbloader3.NodeTableRewriter;
import org.apache.jena.tdbloader3.SecondDriver;
import org.openjena.atlas.io.IO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.tdb.base.file.Location;
import com.hp.hpl.jena.tdb.store.DatasetGraphTDB;
import com.hp.hpl.jena.tdb.store.bulkloader.BulkLoader;
import com.hp.hpl.jena.tdb.store.bulkloader2.CmdIndexBuild;
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

        // Node table
        new File(args[1], "nodes.dat").delete() ;
        mergeToLocalFile(fs, new Path(args[0]), args[2], configuration);
        // TODO: this is a sort of a cheat and it could go away (if it turns out to be too slow)!
        fixNodeTable2(location);

        // B+Tree indexes
        mergeToLocalFile2(fs, new Path(args[1]), args[2], configuration);

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

	private void mergeToLocalFile2 ( FileSystem fs, Path src, String outPath, Configuration configuration ) throws FileNotFoundException, IOException {
		// Find all the right paths and copy .gz files locally
		FileStatus[] status = fs.listStatus(src);
		Map<String, Path> paths = new TreeMap<String, Path>();
		for ( FileStatus fileStatus : status ) {
            Path path = fileStatus.getPath();
            String pathName = path.getName();
            if ( pathName.startsWith(FourthDriver.NAME) ) {
            	paths.put(pathName, path);
            }
        }

        for (String pathName : paths.keySet()) {
        	Path path = new Path(src, paths.get(pathName));
        	status = fs.listStatus(path);
        	for ( FileStatus fileStatus : status ) {
        		Path p = fileStatus.getPath();
        		log.debug("Copying {} to {}...", p.toUri(), outPath);
        		fs.copyToLocalFile(p, new Path(outPath, p.getName()));
        	}
		}

        // Merge .gz files into indexName.gz
        File fileOutputPath = new File(outPath);
        File[] files = fileOutputPath.listFiles(new FileFilter() {
			@Override 
			public boolean accept(File pathname) { return pathname.getName().endsWith(".gz"); }}
        );
        Arrays.sort(files);
        String prevIndexName = null;
        OutputStream out = null;
        for (File file : files) {
        	log.debug("Processing {}... ", file.getName());
        	String indexName = file.getName().substring(0, file.getName().indexOf("_"));
        	if ( prevIndexName == null ) prevIndexName = indexName;
        	if ( out == null ) out = new GZIPOutputStream(new FileOutputStream(new File(outPath, indexName + ".gz")));
			if ( !prevIndexName.equals(indexName) ) {
				if ( out != null ) IO.close(out);
				log.debug("Index name set to {}", indexName);
				out = new GZIPOutputStream(new FileOutputStream(new File(outPath, indexName + ".gz")));
			}
			InputStream in = new GZIPInputStream(new FileInputStream(file));
			log.debug("Copying {} into {}.gz ...", file.getName(), indexName);
			IOUtils.copyBytes(in, out, 8192);
			in.close();
			file.delete();
			prevIndexName = indexName;
		}
        if ( out != null ) IO.close(out);
        
        // build B+Tree indexes
		Location location = new Location(outPath);
		for ( String idxName : Constants.indexNames ) {
			log.debug("Creating {} index...", idxName);
		    String indexFilename = location.absolute(idxName, "gz");
		    if ( new File(indexFilename).exists() ) {
		    	new File(outPath, idxName + ".dat").delete() ;
		    	new File(outPath, idxName + ".idn").delete() ;
		        CmdIndexBuild.main(location.getDirectoryPath(), idxName, indexFilename);
	            // To save some disk space
	            new File (indexFilename).delete();
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
