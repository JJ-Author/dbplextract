package org.aksw.sdw.dbpl;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.Writer;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.ParsePosition;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.aksw.sdw.dbpl.Metadata.Validity;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.internal.bind.util.ISO8601Utils;

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
	
	final static Logger logger = Logger.getLogger(App.class);
	final static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
	static DataSet<Tuple2<String,String>> d;
	
    public static void main( String[] args )
    {
    	List<Path> file_list = new ArrayList<Path>();
    //read config file 
		try {
			new App().initConstantsFromPropValues();
			logger.info("started with resuming at "+RESUME_FILE);
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
			
			  System.out.println("Processing all DBPedia live diff files and creating Flink Map Jobs");
			  
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
				  handleNTFile(file);
			  }
			  System.out.println("\ncreating Flink Map Jobs finished");
		
		// REDUCE PHASE 
			  DataSet<String> result =  
					  d.groupBy(0).reduceGroup( new RichGroupReduceFunction<Tuple2<String, String>, String>() {
						  
						 // Reduce Function
						  	public void reduce(Iterable<Tuple2<String, String>> it,Collector<String> out) 
						  	{ 						  		  
								  Iterator<Tuple2<String, String>> itt = it.iterator();
								  List<String> list = new ArrayList<>();
								  Tuple2<String, String> t = itt.next();
								  String tmp = t.f0+"\t";list.add(t.f1);
								  while(itt.hasNext()){
								    list.add(itt.next().f1);
								  }
								  
								  Collections.sort(list);
								  out.collect(tmp+retrieveMetadataString(list,this.histograms));
							} 
						  	
						  	public void open(Configuration parameters) {	//register the histograms
						  		getRuntimeContext().addAccumulator("deletes"			,histograms.deletes); 
						  		getRuntimeContext().addAccumulator("adds"				,histograms.adds);
						  		getRuntimeContext().addAccumulator("delAnnomalies"		,histograms.delAnnomalies);
						  		getRuntimeContext().addAccumulator("addAnnomalies"		,histograms.addAnnomalies);
						  		getRuntimeContext().addAccumulator("durations"			,histograms.durations);
						  		
						    }
						  	App.Histograms histograms = new App.Histograms();

						  }
					  ).returns(String.class);
			  try {

				result.writeAsText("test.txt",WriteMode.OVERWRITE);//.setParallelism(1);
				JobExecutionResult job = env.execute();
				
				
				Gson gson = new Gson(); 
				TreeMap deletes = job.getAccumulatorResult("deletes");
				TreeMap adds = job.getAccumulatorResult("adds");
				TreeMap addAnnomalies = job.getAccumulatorResult("addAnnomalies");
				TreeMap delAnnomalies = job.getAccumulatorResult("delAnnomalies");
				TreeMap durations = job.getAccumulatorResult("durations");
				try (Writer writer = new FileWriter("stats.json")) {
				    writer.write("{ \"deletes\":");gson.toJson(deletes, writer);
				    writer.write("\n,\"adds\":"); gson.toJson(adds, writer); 
				    writer.write("\n,\"addAnnomalies\":"); gson.toJson(addAnnomalies, writer); 
				    writer.write("\n,\"delAnnomalies\":"); gson.toJson(delAnnomalies, writer); 
				    writer.write("\n,\"durations\":"); gson.toJson(durations, writer);
				    writer.write("\n}");				}

				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				System.out.println("ERROR:");
				e.printStackTrace();
			}
	    } catch (IOException e) {
	      e.printStackTrace();
	    }

    }
    
    
    public static String retrieveMetadataString(List<String> list,App.Histograms h)
    {
    	Metadata m = new Metadata();
    	for (String s : list) 
    	{
			boolean adding = s.charAt(s.length()-1) == 'a';
			String date = s.substring(0, s.length()-1);
			
			if(m.validities.size()==0) // if validity list is empty
			{
				if (adding)
				{
					m.validities.add(m.new Validity(date,""));
					m.added.add(date);
				}	 
				else
				{
					 m.validities.add(m.new Validity("#UNKNOWN#",date));
					 m.deleted.add(date);
				}
			}
			else
			{
				if (adding)
				{
			    	m.added.add(date);
					if (m.validities.peekLast().validUntil.equals("")) // triple is re-added  without being removed 
						m.add_anomalies.add(date);
					else
					{
						m.validities.add(m.new Validity(date, ""));
					}
				}
				else
			    {
			    	m.deleted.add(date);
			    	if (m.validities.peekLast().validFrom.equals("#UNKNOWN#")) // triple is removed without being added first 
						m.delete_anomalies.add(date);
					else if (m.validities.peekLast().validUntil.equals("")) //do note remove already removed triples
					{
						m.validities.peekLast().validUntil=date;
					}
			    }
			}
		}
    	
    	//calculate the validity duration  for each validity entry
    	for ( Validity v : m.validities) 
    	{
    		int days = -1;
    		try {
    			long diff = ISO8601Utils.parse(v.validUntil,new ParsePosition(0)).getTime() - ISO8601Utils.parse(v.validFrom,new ParsePosition(0)).getTime();
    			days = (int) TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS); // no leap years, daylight time and so on ...
			} catch (Exception e) {
			}
    		finally {
				h.durations.add(days);
				m.durations.add(days);
			}
		}
    	
    	
    	 h.deletes.add(m.deleted.size());
    	 h.adds.add(m.added.size());
    	 h.delAnnomalies.add(m.delete_anomalies.size());
    	 h.addAnnomalies.add(m.add_anomalies.size());
    	
    	Gson gson = new Gson(); 
    	return gson.toJson(m);
    	
     }
    
    protected static class Histograms implements Serializable
    {
    	Histogram deletes = new Histogram();
    	Histogram adds = new Histogram();
    	Histogram delAnnomalies = new Histogram();
    	Histogram addAnnomalies = new Histogram();
    	Histogram durations = new Histogram();
    }
   
    public static void handleNTFile(Path file)
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
    	
    //MAP PHASE(S) HERE	
    	DataSource<String> d1 = env.readTextFile(file.toString()); // mark that file as input for later evaluation phase
    	DataSet<Tuple2<String,String>> d2 = d1.flatMap( //define the MAP FUNCTION
    			(String s,Collector<Tuple2<String, String>> o) -> {
    				if(s.charAt(0)!='#')
    					o.collect(new Tuple2<String,String>(s,(added)?date+'a': date+'d')); 
    			}
    		).returns("Tuple2<String,String>");
   
    	d = (d==null) ? d2 : d.union(d2); // "copy" the data of that file to the global dataset
 
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


