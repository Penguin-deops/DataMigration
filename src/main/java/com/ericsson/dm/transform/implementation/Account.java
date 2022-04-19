package com.ericsson.dm.transform.implementation;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.ericsson.dm.Utils.CommonUtilities;
import com.ericsson.dm.inititialization.LoadSubscriberMapping;
import com.ericsson.dm.transformation.JsonInputFields;
import com.ericsson.dm.transformation.JsonInputFields.ADDON_OFFER;
import com.ericsson.dm.transformation.JsonInputFields.MAIN_OFFER;

public class Account {
	Set<String> rejectAndLog;
	Set<String> onlyLog;
	JsonInputFields inputObj;
	
	public Account(Set<String> rejectAndLog, Set<String> onlyLog, JsonInputFields inputObj) {
		this.rejectAndLog=rejectAndLog;
		this.onlyLog=onlyLog;		
		this.inputObj = inputObj;
	}

	public Collection<? extends String> execute() {
		// TODO Auto-generated method stub
		List<String> result = new ArrayList<String>();
		result = applyRulesAccount();		
		
		
		return result;
	}
	
	private List<String> applyRulesAccount() {
		
		List<String> accountList = new ArrayList<>();
		StringBuffer sb = new StringBuffer();
		
		//String msisdn = subscriber.getSubscriberInfoMSISDN();
		String AccountClass = "";
		
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd dd:mm:ss");
		Date dateobj = new Date();
	    
		TreeMap<Long, String> OfferStartDate = new TreeMap<Long,String>();
		for(Map.Entry<String,Object> entry : inputObj.MAIN_OFFER.entrySet())
		{
			MAIN_OFFER mo = inputObj.new MAIN_OFFER();
			mo = (MAIN_OFFER)entry.getValue();
			String OfferID = mo.Offer_ID;
			//Long Expirydays = CommonUtilities.convertDateToEpoch(CommonUtilities.getEndOfDay(mo.End_Date_Time));
			//Long startdays = CommonUtilities.convertDateToEpoch(CommonUtilities.getEndOfDay(mo.Start_Date_Time));
			
			OfferStartDate.put(CommonUtilities.convertDateToEpoch(mo.Start_Date_Time), mo.Start_Date_Time);
				
		}
		
		long actDate = CommonUtilities.convertDateToEpoch(inputObj.OfferStartDateTime);
		
		//OfferStartDate.get(mo.Offer_ID).first();
		//System.out.println(df.format(dateobj));
		long sfeedate = 0;
		long supdate = 0;

		String units = "0";
		String sfeeStatus = "0";
		String supStatus = "0";
		
		
				// validTo = ruleB(validTo);
		sb.append(inputObj.MSISDN).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_3001")).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_3001")).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_NULL")).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_0")).append(",");
		sb.append(actDate +1 ).append(",");
		sb.append(CommonUtilities.convertDateToEpoch(CommonUtilities.getEndOfDay(df.format(dateobj))) + Long.parseLong(LoadSubscriberMapping.CommonConfigMapping.get("Default_1023")) + 1).append(",");
		sb.append(CommonUtilities.convertDateToEpoch(CommonUtilities.getEndOfDay(df.format(dateobj))) + Long.parseLong(LoadSubscriberMapping.CommonConfigMapping.get("Default_1023")) + 1).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_0")).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_0")).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_1023")).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_1023")).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_0")).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_0")).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_0")).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_1")).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_0")).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_0")).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_0")).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_0")).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_0")).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_0")).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_0")).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_NULL")).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_NULL")).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_NULL"));
		
		
		accountList.add(sb.toString());
		
		sb = null;
		return accountList;
	}
	
}