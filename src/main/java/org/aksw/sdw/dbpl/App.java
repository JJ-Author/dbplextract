package org.aksw.sdw.dbpl;

import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.jena.ext.com.google.common.collect.Lists;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.util.FileManager;
import org.apache.log4j.Logger;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.exc.ReqlError;
import com.rethinkdb.gen.exc.ReqlQueryLogicError;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Cursor;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.lang.System;



/**
 *
 */
public class App 
{
	public static String DIFFS_DIRECTORY ;
	public static int RETHINK_PORT ;
	public static int NUMBER_THREADS ;
	public static int LOGGING ;
	public static String RESUME_FILE;
	
	public static final RethinkDB r = RethinkDB.r;
	public static Connection conn;
	final static Logger logger = Logger.getLogger(App.class);
	
    public static void main( String[] args )
    {
    	
    	List<Path> file_list = new ArrayList<Path>();
    	
    //read config file and try to connect to rethinkdb database
		try {
			new App().initConstantsFromPropValues();
			conn = r.connection().hostname("localhost").port(RETHINK_PORT).connect();
		} /*catch (TimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			conn =null;
		} */catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	//check if database exists and delete it if so and (re)create the tables and indeces
		if (RESUME_FILE.equals(""))
		{
			if( ((List<String>) r.dbList().run(conn)).contains("dbplextract") )
			{
				r.db("dbplextract").tableDrop("dbpl16").run(conn); 
				r.dbDrop("dbplextract").run(conn);
			}	
			r.dbCreate("dbplextract").run(conn);
			r.db("dbplextract").tableCreate("dbpl16").run(conn);
			
			//r.db("dbplextract").table("dbpl16").indexCreate("hash").run(conn);
			//r.db("dbplextract").table("dbpl16").indexWait("hash").run(conn);
			r.db("dbplextract").table("dbpl16").indexCreate("spo",
				    row -> r.array(row.g("s"), row.g("p"), row.g("o")) ).run(conn);
			r.db("dbplextract").table("dbpl16").indexWait("spo").run(conn);
		}

    
    	
	//process all added.nt and removed.nt files recursively in specified directory (tree)
	    Path p = Paths.get(DIFFS_DIRECTORY);
	    FileVisitor<Path> fv = new SimpleFileVisitor<Path>() {
			      @Override
			      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
			    	  	file_list.add(file); // add all files to a list for later retrieval (cannot use it directly because there is no guaranteed order)
			    	  	return FileVisitResult.CONTINUE;
			      }
	    };

	    try 
	    {
			  Files.walkFileTree(p, fv);
			  java.util.Collections.sort(file_list); // sort the list to process the files in the right (time) order 
			  Path last_month=null; Path last_day=null;
			  boolean skip =  (!RESUME_FILE.equals("")) ? true : false;
				  
			  for (Path file : file_list) 
			  {
				  if (skip)
				  {
					  if (file.toAbsolutePath().toString().equals(RESUME_FILE))
						  skip=false;
					  continue;
				  }
				  Path  day = file.toAbsolutePath().getParent().getParent(); Path month  = day.getParent();
				  if (last_month==null || !last_month.equals(month))
					  System.out.print("\nprocessing files in month "+month+"\n\tday"); 
				  if (last_day==null || !last_day.equals(day))
					  System.out.print(" "+day.getFileName());
				  last_month = month;last_day=day;
				  if(LOGGING>0){
						logger.info(file.toAbsolutePath().toString());
					} 	
				  parseNTFile(file);
			  }
			  System.out.println("finish");
	    } catch (IOException e) {
	      e.printStackTrace();
	    }

    }
   
    
    public static void handleStatement(Statement st, String date, boolean adding)
    {
    //parse Triple line
//    	RethinkDB r = RethinkDB.r;
//    	Connection conn=null; 
//    	try {
//			conn = r.connection().hostname("localhost").port(RETHINK_PORT).connect();
//		} /*catch (TimeoutException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//			conn =null;
//		} */ finally {}
    	String s = st.asTriple().getSubject().toString();
		String p = st.asTriple().getPredicate().toString();
		String o = st.asTriple().getObject().toString();
		
		String hash = ""+(s+p+o).hashCode();
		
		boolean statement_exists = false;
		
	//check if statement already exists and fetch it if so
		Cursor<HashMap> cursor = r.db("dbplextract").table("dbpl16").getAll(r.array(s,p,o)).optArg("index", "spo").run(conn);//filter(row -> row.g("hash").eq(hash)).run(conn); 
		HashMap doc=null; 
		for ( HashMap doc2 : cursor) {
			statement_exists = true; doc=doc2;		    
		}
		MapObject m;
	//parse statement data from db and update it 
		if (statement_exists)
		{
			
			ArrayList validity_list = (ArrayList) ((List) doc.get("validity"));
		    HashMap last_valid_entry = (HashMap) validity_list.get(validity_list.size()-1); //get last validEntry
		    String last_valid_from = (String) last_valid_entry.get("validFrom"); 
		    String last_valid_until = (String) last_valid_entry.get("validUntil");
		    ArrayList<String> added = (ArrayList) ((List) doc.get("added"));
		    ArrayList<String> removed = (ArrayList) ((List) doc.get("deleted"));
		    if (adding)
			{
		    	added.add(date);
				if (last_valid_until.equals("")) // triple is re-added  without being removed 
					;
				else
				{
					validity_list.add(r.hashMap("validFrom", date).with("validUntil", ""));
				}
			}
		    else
		    {
		    	removed.add(date);
		    	if (last_valid_from.equals("#UNKNOWN#")) // triple is removed  without being added first 
					;
				else if (last_valid_until.equals("")) //do note remove already removed triples
				{
					last_valid_entry.put("validUntil", date);
					validity_list.set(validity_list.size()-1, last_valid_entry);
				}
		    }

		    r.db("dbplextract").table("dbpl16").replace(doc).run(conn);
		   
		}
	//insert statement information for the first time
		else
		{	
			 m = r.hashMap("hash", hash)
		     .with("s", s)
		     .with("p", p)
		     .with("o", o); 

			 if (adding)
				 m = m.with("validity", r.array(r.hashMap("validFrom", date).with("validUntil", ""))).with("added", r.array(date)).with("deleted", r.array());
			 else
				 m = m.with("validity", r.array(r.hashMap("validFrom", "#UNKNOWN#").with("validUntil", date))).with("added", r.array()).with("deleted", r.array(date));
			 
			 r.db("dbplextract").table("dbpl16").insert(m).run(conn);
		} 
		//conn.close();
		
    }
    
    
    
    
    public static void parseNTFile(Path file)
    {
    //check whether it's an adding or removing file	
    	boolean tmp;
    	if (file.getFileName().toString().endsWith("added.nt"))
    		tmp=true;
    	else if (file.getFileName().toString().endsWith("removed.nt"))
    		tmp=false;
    	else 
    		return;
    	final boolean added = tmp;
    	
    //read the timestamp comment from the first line of the nt file	
    	String  date_tmp = null;
    	try(BufferedReader reader = new BufferedReader(new FileReader(file.toString()))) {
            date_tmp = reader.readLine();
            date_tmp = date_tmp.replace("# started ", "");
            
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
    	final String date = date_tmp;
    	
		Model model = ModelFactory.createDefaultModel();
		    java.io.InputStream is = FileManager.get().open(file.toAbsolutePath().toString());
		    
		if (is != null) {
			
		    Model m = model.read(is, null, "N-TRIPLE"); //parse nt file
		    
		    //prepare stuff for parallel processing of the statements
		    if (NUMBER_THREADS>0)
		    	System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", ""+NUMBER_THREADS);
		    StmtIterator it = m.listStatements();
		    Stream<Statement> targetStream = StreamSupport.stream(
		    		Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED),false);

		    //process the statements in parallel
		    /*ForkJoinPool forkJoinPool = new ForkJoinPool(NUMBER_THREADS);
			
				try {
					forkJoinPool.submit(() -> targetStream.parallel().forEach( s -> handleStatement(s, date, added))
					    //parallel task here, for example
					    
					).get();
				} catch (InterruptedException | ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}*/
			targetStream.parallel().forEach( s -> handleStatement(s, date, added));
		}
	

		
	}
    
    public void initConstantsFromPropValues() throws IOException {
    	InputStream inputStream = null;
		try {
			Properties prop = new Properties();
			String propFileName = "config.properties";
 
			inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
 
			if (inputStream != null) {
				prop.load(inputStream);
			} else {
				throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
			}
			DIFFS_DIRECTORY = prop.getProperty("diffs_directory");
			RETHINK_PORT = Integer.parseInt(prop.getProperty("rethink_port"));
			NUMBER_THREADS = Integer.parseInt(prop.getProperty("number_threads"));
			LOGGING = Integer.parseInt(prop.getProperty("logging"));
			RESUME_FILE = prop.getProperty("resume_file");
			
					
		} catch (Exception e) {
			System.out.println("Exception: " + e);
		} finally {
			inputStream.close();
		}
	}

    
}


