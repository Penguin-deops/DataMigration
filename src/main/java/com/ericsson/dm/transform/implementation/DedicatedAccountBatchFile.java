package com.ericsson.dm.transform.implementation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ericsson.dm.inititialization.LoadSubscriberMapping;
import com.ericsson.dm.transformation.JsonInputFields;
import com.ericsson.dm.transformation.JsonInputFields.IMPLICIT_SHARED_OFFER;

public class DedicatedAccountBatchFile {

	Set<String> rejectAndLog;
	Set<String> onlyLog;
	Set<String> trackLog;
	CommonFunctions commonfunction;
	JsonInputFields inputObj;
	Set<String> CompletedMSISDN;
	public DedicatedAccountBatchFile()
	{
		
	}
	
	public DedicatedAccountBatchFile(Set<String> rejectAndLog, Set<String> onlyLog, Set<String> CompletedMSISDN) {		
		// TODO Auto-generated constructor stub
		this.rejectAndLog=rejectAndLog;
		this.onlyLog=onlyLog;
		this.CompletedMSISDN = CompletedMSISDN;		
	}
	
	public DedicatedAccountBatchFile(Set<String> rejectAndLog, Set<String> onlyLog, JsonInputFields inputObj) {		
		// TODO Auto-generated constructor stub
		this.rejectAndLog=rejectAndLog;
		this.onlyLog=onlyLog;
		this.inputObj = inputObj;		
	}
	
	public List<String> execute() {
		// TODO Auto-generated method stub
		List<String> DAList = new ArrayList<>();
		DAList.addAll(PopulateDAFromImplict());
		return DAList;

	}

	private List<String> PopulateDAFromImplict() {
		List<String> UTList = new ArrayList<>();
		for(Map.Entry<String,Object> entry : inputObj.IMPLICIT_SHARED_OFFER.entrySet())
		{
			IMPLICIT_SHARED_OFFER mo = inputObj.new IMPLICIT_SHARED_OFFER();
			mo = (IMPLICIT_SHARED_OFFER)entry.getValue();
			
			if(mo.Shared_Offer_ID.length() > 0 && LoadSubscriberMapping.sharedOfferMapping.containsKey((mo.Shared_Offer_ID)) 
					&& !mo.Usage_Threshold.isEmpty() && Integer.valueOf(mo.Usage_Threshold) > 0)
			{
				String UTFlag = LoadSubscriberMapping.sharedOfferMapping.get(mo.Shared_Offer_ID).split(",")[1].split("=")[1];
				if(UTFlag.equals("Y"))
					UTList.add(PopulateDedicatedAccount(mo.Shared_Offer_ID, inputObj.MSISDN, mo.Usage_Value ,mo.Usage_Threshold));
			}
		}	
		
		return UTList;
	}

	private String PopulateDedicatedAccount(String shared_Offer_ID, String mSISDN,String usage_Value, String usage_Threshold) {
		// TODO Auto-generated method stub
		StringBuilder sb = new StringBuilder();
		
		String[] dedicated_account = new String[3];
		HashMap<String, String> dedicated_account_id = new HashMap<String, String>();
		if(LoadSubscriberMapping.daMapping.containsKey(shared_Offer_ID))
		{
			dedicated_account = (LoadSubscriberMapping.daMapping.get(shared_Offer_ID).split(","));
		}
		for(String str: dedicated_account)
		{
			dedicated_account_id.put(str.split("=")[0].trim(), str.split("=")[1].trim());
		}
		
		sb.append("DA_INSTALL").append(",");
		sb.append(mSISDN).append(",");
		sb.append("1").append(",");
		sb.append(dedicated_account_id.get("DA_ID")).append(",");
		sb.append(Integer.parseInt(usage_Threshold) - Integer.parseInt(usage_Value)).append(",");
		sb.append("").append(",");
		sb.append("").append(",");
		sb.append(dedicated_account_id.get("DA_TYPE")).append(",");
		sb.append("").append(",");
		sb.append("").append(",");
		sb.append("").append(",");
		sb.append("");
		
		return sb.toString();
	}

	
	//&&&&&&&&&&&&&&&&&&&&&&&&Added code to Phase1...For extra records&&&&&&&&&&&&&&&&&
	
	public List<String> executeAdditionalRecord() {
		// TODO Auto-generated method stub
		List<String> DAList = new ArrayList<>();
		DAList.addAll(PopulateDAFromImplictAdditionalRecord());
		return DAList;

	}
	
	private List<String> PopulateDAFromImplictAdditionalRecord() {
		List<String> UTList = new ArrayList<>();
		
		for(Map.Entry<String, Set<String>> entry : LoadSubscriberMapping.Implicit_Shared_OfferExtraInput.entrySet())
		{
			if(!LoadSubscriberMapping.CompleteListToMigrate.contains(entry.getKey()))
			{
				//do nothing
			}
			else
			{
				if(LoadSubscriberMapping.AlreadyMigratedMsisdn.contains(entry.getKey()))
				{
									
				}
				else if(!CompletedMSISDN.contains(entry.getKey()))
				{
					//if("10005560".equals(entry.getKey()))
					//	System.out.println("Vipin");
					Set<String> InputSet = new HashSet<String>(entry.getValue());
					for(String str : InputSet)
					{
						String MSISDN = str.split(",",-1)[0];
						String Shared_Offer_ID = str.split(",",-1)[1];
						String Usage_Threshold = str.split(",",-1)[5];
						String Usage_Value = str.split(",",-1)[4];
						if(Shared_Offer_ID.length() > 0 && LoadSubscriberMapping.sharedOfferMapping.containsKey((Shared_Offer_ID)) 
								&& !Usage_Threshold.isEmpty() && Integer.valueOf(Usage_Threshold) > 0)
						{
							String UTFlag = LoadSubscriberMapping.sharedOfferMapping.get(Shared_Offer_ID).split(",")[1].split("=")[1];
							if(UTFlag.equals("Y"))
								UTList.add(PopulateDedicatedAccount(Shared_Offer_ID, MSISDN, Usage_Value ,Usage_Threshold));
						}
					}
				}
			}
		}		
		
		return UTList;
	}

}
