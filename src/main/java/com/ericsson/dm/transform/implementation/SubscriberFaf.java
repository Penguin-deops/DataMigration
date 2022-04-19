package com.ericsson.dm.transform.implementation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public class SubscriberFaf {
	String msisdn;
	Set<String> rejectAndLog;
	Set<String> onlyLog;
	
	public SubscriberFaf(Set<String> rejectAndLog, Set<String> onlyLog) {
		// TODO Auto-generated constructor stub
		this.rejectAndLog=rejectAndLog;
		this.onlyLog=onlyLog;		
	}
	public Collection<? extends String> execute() {
		// TODO Auto-generated method stub
		List<String> result = new ArrayList<String>();
		
		return result;
	}
	
}
