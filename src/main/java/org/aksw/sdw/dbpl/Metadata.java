package org.aksw.sdw.dbpl;

import java.util.LinkedList;
import java.util.List;

public class Metadata {
	
	class Validity {
		public Validity(String validFrom, String validUntil) 
		{
			this.validFrom=validFrom;
			this.validUntil=validUntil;
		}
		public String validFrom; 
		public String validUntil;
		
	};
	public LinkedList<Validity> validities 			= new LinkedList<>();
	public LinkedList<String> added 				= new LinkedList<>();
	public LinkedList<String> deleted 				= new LinkedList<>();
	public LinkedList<String> delete_anomalies 	= new LinkedList<>();
	public LinkedList<String> add_anomalies 	= new LinkedList<>();
	public LinkedList<Integer> durations 		= new LinkedList<>();
};
