package com.ericsson.dm.transform.implementation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.ericsson.dm.inititialization.LoadSubscriberMapping;
import com.ericsson.dm.transformation.JsonInputFields;

public class ServiceClass {
	Set<String> rejectAndLog;
	Set<String> onlyLog;
	CommonFunctions commonfunction;
	JsonInputFields inputObj;
	Map<String, String> ProductIDLookUpMap;
	
	
	public ServiceClass(Set<String> rejectAndLog, Set<String> onlyLog,JsonInputFields inputObj, Map<String, String> ProductIDLookUpMap) {
		// TODO Auto-generated constructor stub
		this.rejectAndLog = rejectAndLog;
		this.onlyLog = onlyLog;
		this.inputObj = inputObj;
		commonfunction = new CommonFunctions();		
		this.ProductIDLookUpMap = ProductIDLookUpMap;		
	}
	
	public List<String> executePhase2() {
		// TODO Auto-generated method stub
		List<String> SCValue = new ArrayList<String>();
		
		SCValue.addAll(GenerateFromServiceClass());
		LoadSubscriberMapping.counterForSC++;
		//System.out.println("Service Class went inside once only");
		
		return SCValue;
	}

	private Collection<? extends String> GenerateFromServiceClass() {
		// TODO Auto-generated method stub
		List<String> SCList = new ArrayList<String>();
		
		for(Entry<String, String> etr : LoadSubscriberMapping.mainCreditLimitInput.entrySet())
		{
			if(LoadSubscriberMapping.CompleteListToMigrate.contains(etr.getKey()))
			{
				String MSISDN = etr.getValue().split(",",-1)[0];
				String SC = etr.getValue().split(",",-1)[1];
				List<String> RejectedSC = Arrays.asList(LoadSubscriberMapping.CommonConfigMapping.get("Rejected_SC").split(","));
				if(RejectedSC.contains(SC))
					//if( SC.equals("4001")) //LoadSubscriberMapping.CommonConfigMapping.get("Rejected_SC")
					onlyLog.add("INC42:MSISDN=" + etr.getValue().split(",",-1)[0] + ":SERVICE_CLASS="+ SC +":DESCRIPTION=SERVICE_CLASS IS NOT SUPPOSE TO MIGRATED:ACTION=DISCARD & LOG" );
				else
					SCList.add(populateSC(MSISDN,SC));
			}
			else
				onlyLog.add("INC41:MSISDN=" + etr.getValue().split(",",-1)[0] + ":DESCRIPTION=SERVICE_CLASS NOT PRESENT IN ACCOUNT, DISCARED & LOGGED:ACTION=DISCARD & LOG" );
		}		
		return SCList;
	}
	
	private String populateSC(String MSISDN, String SC)
	{
		// TODO Auto-generated method stub
		StringBuffer sb = new StringBuffer();
		if(LoadSubscriberMapping.CompleteListToMigrate.contains(MSISDN))
		{
			if(LoadSubscriberMapping.AlreadyMigratedMsisdn.contains(MSISDN))
			{
										
			}
			else
			{				
				sb.append("GENERAL").append(",");
				sb.append(MSISDN).append(",");
				sb.append("").append(",");
				sb.append("").append(",");
				sb.append("").append(",");
				sb.append("").append(",");
				sb.append("").append(",");
				sb.append("").append(",");
				sb.append("").append(",");
				sb.append(SC).append(",");
				sb.append("").append(",");
				sb.append("").append(",");
				sb.append("");
			}
		}
		return sb.toString();
	}
}
