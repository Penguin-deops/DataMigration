package com.ericsson.dm.transform.implementation;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import com.ericsson.dm.inititialization.LoadSubscriberMapping;

public class Credit_Limit_UC {
	Set<String> rejectAndLog;
	Set<String> onlyLog;
	CommonFunctions commonfunction;
	
	public Credit_Limit_UC(Set<String> rejectAndLog, Set<String> onlyLog) {
		// TODO Auto-generated constructor stub
		this.rejectAndLog = rejectAndLog;
		this.onlyLog = onlyLog;
		commonfunction = new CommonFunctions();		
	}
	
	public List<String> executePhase2() {
		// TODO Auto-generated method stub
		List<String> UCValue = new ArrayList<String>();
		UCValue.addAll(GenerateFromCreditLimit());
		return UCValue;
	}

	private Collection<? extends String> GenerateFromCreditLimit() {
		// TODO Auto-generated method stub
		List<String> UCValue = new ArrayList<String>();
		for(Entry<String, String> etr : LoadSubscriberMapping.mainCreditLimitInput.entrySet())
		{
			if(LoadSubscriberMapping.CompleteListToMigrate.contains(etr.getKey()))
			{
				if(LoadSubscriberMapping.AlreadyMigratedMsisdn.contains(etr.getValue().split(",",-1)[0]))
				{
					rejectAndLog.add("INC03:MSISDN=" + etr.getValue().split(",",-1)[0] + ":REFERENCE_CREDIT_LIMIT=" + etr.getValue().split(",",-1)[2] 
							+ ":SUBSCRIBER_BALANCE=" + etr.getValue().split(",",-1)[3] + ":AVAILABLE_CREDIT_LIMIT=" + etr.getValue().split(",",-1)[4]  
							+ ":Notification_70=" + etr.getValue().split(",",-1)[6] + ":Notification_80=" + etr.getValue().split(",",-1)[7] 
							+ ":Notification_90=" + etr.getValue().split(",",-1)[8] + ":UsageCounter=" + etr.getValue().split(",",-1)[9] + 
							":ServiceClass=" + etr.getValue().split(",",-1)[1] + ":DESCRIPTION=MSISDN ALREADY MIGRATED:ACTION=DISCARD & LOG");
				}
				else
				{
					String MSISDN = etr.getValue().split(",",-1)[0];
					String UsageValue = etr.getValue().split(",",-1)[9];
					NumberFormat formatter = new DecimalFormat("#0"); 
					if(UsageValue.length() !=0 && !MSISDN.startsWith("100"))
					{
						String UC_ID = LoadSubscriberMapping.CommonConfigMapping.get("UC_ID");
						String Balance =  formatter.format(Double.parseDouble(LoadSubscriberMapping.CommonConfigMapping.get("Monetory_Factor")) * 
								Double.parseDouble(UsageValue)); 
						UCValue.add(PopulateUsageCounter(UC_ID, MSISDN, Balance));
					}
					else
					{
						rejectAndLog.add("INC03:MSISDN=" + etr.getValue().split(",",-1)[0] + ":REFERENCE_CREDIT_LIMIT=" + etr.getValue().split(",",-1)[2] 
								+ ":SUBSCRIBER_BALANCE=" + etr.getValue().split(",",-1)[3] + ":AVAILABLE_CREDIT_LIMIT=" + etr.getValue().split(",",-1)[4]  
								+ ":Notification_70=" + etr.getValue().split(",",-1)[6] + ":Notification_80=" + etr.getValue().split(",",-1)[7] 
								+ ":Notification_90=" + etr.getValue().split(",",-1)[8] + ":UsageCounter=" + etr.getValue().split(",",-1)[9] + 
								":ServiceClass=" + etr.getValue().split(",",-1)[1] + ":DESCRIPTION=MSISDN NOT PRESENT IN CS:ACTION=DISCARD & LOG");
							
					}
				}
			}
		}
		
		return UCValue;
	}
	
	public String PopulateUsageCounter(String UC_ID, String Msisdn, String Balance)
	{
		StringBuilder sb = new StringBuilder();
		
		sb.append("SeqNo").append(",");
		sb.append(Msisdn).append(",");
		sb.append(UC_ID).append(",");	
		sb.append(Balance);
			
		
		return sb.toString();
	}
}
