package com.ericsson.dm.transform.implementation;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import com.ericsson.dm.Utils.CommonUtilities;

public class OfferAttribute {
	String msisdn;
	Set<String> rejectAndLog;
	Set<String> onlyLog;
	CommonFunctions commonfunction;
	
	public OfferAttribute()
	{
		
	}
	
	public OfferAttribute(Set<String> rejectAndLog, Set<String> onlyLog) {
		// TODO Auto-generated constructor stub
		this.rejectAndLog=rejectAndLog;
		this.onlyLog=onlyLog;
		commonfunction = new CommonFunctions();
	}
	
	public Map<String, List<String>> execute() {
		// TODO Auto-generated method stub
		Map<String,List<String>> map = new HashMap<>();
		return map;
	}
	
	public String PopulateOfferAttribute(String OffAttr_Type,String OffAttr_Name, String OffAttr_ID, String Balance_ID, String Balance_Value)
	{		
		StringBuilder sb = new StringBuilder();
		sb.append(msisdn).append(",");
		sb.append(OffAttr_ID).append(",");
		sb.append("OfferAttr_Defination").append(",");
		try {
			sb.append(CommonUtilities.toHexadecimal(Balance_Value)).append(",");
		} 
		catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		sb.append("0");
		
		return sb.toString();
	}

}