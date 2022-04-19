package com.ericsson.dm.transform.implementation;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.Map; 
import java.util.HashMap;

import com.ericsson.dm.Utils.CommonUtilities;
import com.ericsson.dm.inititialization.LoadSubscriberMapping;
import com.ericsson.dm.transformation.JsonInputFields;
import com.ericsson.dm.transformation.JsonInputFields.MAIN_OFFER;


public class PamAccount {
	Set<String> rejectAndLog;
	Set<String> onlyLog;
	JsonInputFields inputObj;
	
	public PamAccount(Set<String> rejectAndLog, Set<String> onlyLog, JsonInputFields inputObj) {
		this.rejectAndLog=rejectAndLog;
		this.onlyLog=onlyLog;		
		this.inputObj = inputObj;
	}
	
	public Collection<? extends String> execute() {
		// TODO Auto-generated method stub
		List<String> result = new ArrayList<String>();
		result.addAll(PamFromDefaultMapping());		
		result.addAll(PamFromBillCycle());	
		
		return result;
	}
	
	private Collection<? extends String> PamFromBillCycle() {
		// TODO Auto-generated method stub
		List<String> pamList = new ArrayList<>();
		for(Map.Entry<String,Object> entry : inputObj.MAIN_OFFER.entrySet())
		{
			MAIN_OFFER mo = inputObj.new MAIN_OFFER();
			mo = (MAIN_OFFER)entry.getValue();
			
			if(mo.Bill_Cycle.length() > 0)
			{
				LocalDate todaydate = LocalDate.now();
				String Current_PAM_Period = "";
				String Last_Evaluation_Date = "";
				if(todaydate.getDayOfMonth() > Integer.parseInt(mo.Bill_Cycle))
				{
					int newDay =  Integer.parseInt(mo.Bill_Cycle);
					//Date NewDate = CommonUtilities.addDays(dateobj, diff);
					
					if(mo.Bill_Cycle.equals("11"))
					{
						//PP_BB_11_2020-05-31
						String month = String.valueOf(todaydate.getMonthValue());
						if(month.length() == 1)
							month = "0" + month;
						
						Current_PAM_Period = "PP_BB_1" + "_" + todaydate.getYear() + "-" + month + "-" + todaydate.lengthOfMonth()  ;
						Last_Evaluation_Date = String.valueOf(CommonUtilities.convertDateToEpoch(todaydate.withDayOfMonth(1)
							.toString() + " 00:00:00") + 1);
					}
					else
					{
						
						Current_PAM_Period = "PP_BB_" + mo.Bill_Cycle + "_" + todaydate.minusMonths(-1).withDayOfMonth(newDay-1);
						//System.out.println((todaydate.withDayOfMonth(newDay).toString()));
						Last_Evaluation_Date = String.valueOf(CommonUtilities.convertDateToEpoch(todaydate.withDayOfMonth(newDay+1)
							.toString() + " 00:00:00"));
					}
				}
				else
				{
					if(mo.Bill_Cycle.equals("11"))
					{
						//PP_BB_11_2020-05-31
						String month = String.valueOf(todaydate.getMonthValue());
						if(month.length() == 1)
							month = "0" + month;
						
						Current_PAM_Period = "PP_BB_1" + "_" + todaydate.getYear() + "-" + month + "-" + todaydate.lengthOfMonth()  ;
						Last_Evaluation_Date = String.valueOf(CommonUtilities.convertDateToEpoch(todaydate.withDayOfMonth(1)
							.toString() + " 00:00:00") + 1);
					}
					else
					{
						int newDay =  Integer.parseInt(mo.Bill_Cycle) ;
						Current_PAM_Period = "PP_BB_" + mo.Bill_Cycle + "_" + todaydate.withDayOfMonth(newDay -1);
						//Last evaluation date
						Last_Evaluation_Date = String.valueOf(CommonUtilities.convertDateToEpoch(todaydate.minusMonths(1)
							.withDayOfMonth(newDay +1).toString() + " 00:00:00"));
					}
				}
				
				StringBuffer sb = new StringBuffer();
				sb.append(inputObj.MSISDN).append(",");
				sb.append("11").append(",");
				sb.append(mo.Bill_Cycle).append(",");
				if(mo.Bill_Cycle.equals("11"))
					sb.append("1").append(",");
				else
					sb.append(mo.Bill_Cycle).append(",");
				sb.append(Current_PAM_Period).append(",");
				sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_0")).append(",");
				sb.append(Last_Evaluation_Date).append(",");
				sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_0"));
				pamList.add(sb.toString());
			}
		}
		return pamList;
	}

	private List<String> PamFromDefaultMapping() {		
		List<String> pamList = new ArrayList<>();
		for(Map.Entry<String,String> entry : LoadSubscriberMapping.pamMapping.entrySet())
		{
			//System.out.println(entry.getKey());
			//System.out.println(entry.getValue());
			
			StringBuffer sb = new StringBuffer();
			String[] DefaultPAM = entry.getValue().split(",");
			
			String Last_Evaluation_Date = "";
			String Current_PAM_Period = "";
			if(DefaultPAM[4].split("=")[1].toUpperCase().equals("CURRENTDATE"))
			{
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd");
				Date dateobj = new Date();
				Last_Evaluation_Date = String.valueOf(CommonUtilities.convertDateToEpoch(df.format(dateobj)));
				//YYYY-MM-DD
				String DateFormat = df2.format(dateobj);
				Current_PAM_Period = DefaultPAM[2].split("=")[1].replace("YYYY-MM-DD", DateFormat);
			}
			else if(DefaultPAM[4].split("=")[1].toUpperCase().equals("OFFERSTARTDATE"))
			{
				Last_Evaluation_Date = String.valueOf(CommonUtilities.convertDateToEpoch(inputObj.OfferStartDateTime) + 1);
				Current_PAM_Period = DefaultPAM[2].split("=")[1];
			}
			
			sb.append(inputObj.MSISDN).append(",");
			sb.append(DefaultPAM[3].split("=")[1]).append(",");
			sb.append(DefaultPAM[0].split("=")[1]).append(",");
			sb.append(DefaultPAM[1].split("=")[1]).append(",");
			sb.append(Current_PAM_Period).append(",");
			sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_0")).append(",");
			sb.append(Last_Evaluation_Date).append(",");
			sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_0"));
			
			pamList.add(sb.toString());
		}
		
		
		return pamList;
	}
}
