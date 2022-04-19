package com.ericsson.dm.transform.implementation;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;

public class Accumulator {
	String msisdn;
	final static Logger LOG = Logger.getLogger(DedicatedAccount.class);
	
	Set<String> rejectAndLog;
	Set<String> onlyLog;
	Set<String> trackLog;
		
	public Accumulator()
	{
		
	}
	public Accumulator(Set<String> rejectAndLog, Set<String> onlyLog) {
		
		// TODO Auto-generated constructor stub
		this.rejectAndLog=rejectAndLog;
		this.onlyLog=onlyLog;
		this.trackLog = trackLog;
		
	}
	
	public Map<String,String> execute() {
		// TODO Auto-generated method stub
		Map<String, String> ACMmap = new HashMap<String, String>();
		
		
		return ACMmap;
	}
	
}
