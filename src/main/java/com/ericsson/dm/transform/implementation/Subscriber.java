package com.ericsson.dm.transform.implementation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.ericsson.dm.inititialization.LoadSubscriberMapping;
import com.ericsson.dm.transformation.JsonInputFields;

public class Subscriber {
	Set<String> rejectAndLog;
	Set<String> onlyLog;
	CommonFunctions commonfunction;
	JsonInputFields inputObj;
	
	public Subscriber(Set<String> rejectAndLog, Set<String> onlyLog, JsonInputFields inputObj) {
		// TODO Auto-generated constructor stub
		this.rejectAndLog=rejectAndLog;
		this.onlyLog=onlyLog;
		this.commonfunction = new CommonFunctions();
		this.inputObj = inputObj;
	}
	public Collection<? extends String> execute() {
		// TODO Auto-generated method stub
		
		return applyRulesSubscribers();
	}
	
	private List<String> applyRulesSubscribers() {
		List<String> subsList =  new ArrayList<>();
		
		String msisdn = inputObj.MSISDN;
		String serviceClass = "";
		
		String INITIAL_ACTIVATION_DATE_FLAG;
		
		String subsStatus = "";
		String first_ivr_call_done = "";
		long first_call_done = 0;
		String Targetlanguage = "";
		
	
		StringBuffer sb = new StringBuffer();
		sb.append(msisdn).append(",");
		sb.append(msisdn).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_0")).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_0")).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_0")).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_0")).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_0")).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_1")).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_0")).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_1")).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_0")).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_0")).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Block_status")).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_31")).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_1")).append(",");
		sb.append("").append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_0"));
		
		subsList.add(sb.toString());
		sb = null;
		return subsList;
	}	
}
