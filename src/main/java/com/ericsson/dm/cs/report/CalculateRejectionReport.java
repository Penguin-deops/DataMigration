package com.ericsson.dm.cs.report;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.LongAdder;

import com.ericsson.dm.inititialization.LoadSubscriberMapping;

import jdk.management.resource.internal.TotalResourceContext;

public class CalculateRejectionReport {
	String migToolPath = "";
	String logDirectoryPath = "";
	String tempDirectoryPath = "";
	HashMap<String, Long> RejectedZeroUCCount;
	HashMap<String, Long> RejectedZeroUTCount;
	
	HashMap<String, Long> ExceptionCreditLimitCount;
	HashMap<String, BigDecimal> ExceptionCreditLimitBalance;
	
	
	public CalculateRejectionReport(String MigtoolPath) {
		// TODO Auto-generated constructor stub
		this.migToolPath = MigtoolPath;
		this.logDirectoryPath = MigtoolPath + "/logs/";
		this.tempDirectoryPath = MigtoolPath + "/Temp/";
		this.RejectedZeroUCCount = new HashMap<String, Long>();
		this.RejectedZeroUTCount = new HashMap<String, Long>();
		this.ExceptionCreditLimitCount = new HashMap<String, Long>();
		this.ExceptionCreditLimitBalance = new HashMap<String, BigDecimal>();
	}
	
	public void execute()
    {
	   //List down all the files from pre snapshot.
	   readExceptionLogs();	
	   readRejectionLogs();
	   
	   WriteOutputFile(RejectedZeroUCCount, "targetRejectionUCZeroCount.csv");
	   WriteOutputFile(RejectedZeroUTCount, "targetRejectionUTZeroCount.csv");
    }
	
	private void readRejectionLogs() {
		// TODO Auto-generated method stub
		HashMap<String, Long> RejectedOffer = new HashMap<String, Long>();
		HashMap<String, Long> RejectedNotInCSOffer = new HashMap<String, Long>();
		HashMap<String, Long> RejectedUCCount = new HashMap<String, Long>();
		HashMap<String, Long> RejectedUTCount = new HashMap<String, Long>();
		HashMap<String, Long> RejectedUCBalance = new HashMap<String, Long>();
		HashMap<String, Long> RejectedUTBalance = new HashMap<String, Long>();
		HashMap<String, Long> RejectedCLCount = new HashMap<String, Long>();
		HashMap<String, BigDecimal> RejectedCLBalance = new HashMap<String, BigDecimal>();
		HashMap<String, Long> RejectedSCCount = new HashMap<String, Long>();
		HashMap<String, Long> RejectedCLUCCount = new HashMap<String, Long>();
		
		BigDecimal RejectedUCTotalBalance = new BigDecimal("0");
		BigDecimal RejectedUTTotalBalance = new BigDecimal("0");
		try (BufferedReader br = new BufferedReader(new FileReader(logDirectoryPath + "Rejected.log"));){
	 	   String line;
		   while ((line = br.readLine()) != null) {
		    	String data[] = line.split(":",-1);
		    	if(data[0].equals("INC03"))
		    	{
		    		if(!data[8].trim().split("=",-1)[1].isEmpty())
		    		{
		    			if(RejectedCLUCCount.containsKey("1000002"))
		    			{
		    				Long temp = RejectedCLUCCount.get("1000002") + 1l;
		    				RejectedCLUCCount.put("1000002",temp);		    				
		    			}
		    			else
		    			{
		    				RejectedCLUCCount.put("1000002",1l);		    				
		    			}
		    		}
		    	}
		    	else if(data[0].equals("INC02"))
		    	{
		    		/*INC02:MSISDN=36039706: 							 			-0,1
		    		REFERENCE_CREDIT_LIMIT=50:SUBSCRIBER_BALANCE=40.870: 			-2,3
		    		AVAILABLE_CREDIT_LIMIT=9.130:Notification_70=:		 			-4,5
		    		Notification_80=7.304:Notification_90=:UsageCounter=14.326: 	-6,7,8
		    		DESCRIPTION=MSISDN NOT PRESENT IN CS:ACTION7=DISCARD & LOG*/
		    		//if(data[1].startsWith("100"))
		    		{
			    		if(!data[2].trim().split("=",-1)[1].isEmpty())
			    		{
			    			if(RejectedCLCount.containsKey("100000202"))
			    			{
			    				Long temp = RejectedCLCount.get("100000202") + 1l;
			    				RejectedCLCount.put("100000202",temp);
			    				BigDecimal tempBalance = new BigDecimal((data[2].trim().split("=")[1])).add(RejectedCLBalance.get("100000202")) ;
			    				RejectedCLBalance.put("100000202",tempBalance);
			    			}
			    			else
			    			{
			    				RejectedCLCount.put("100000202",1l);
			    				RejectedCLBalance.put("100000202",new BigDecimal((data[2].trim().split("=")[1])));
			    			}
			    		}
			    		if(!data[3].trim().split("=",-1)[1].isEmpty())
			    		{
			    			if(RejectedCLCount.containsKey("10000102"))
			    			{
			    				Long temp = RejectedCLCount.get("10000102") + 1l;
			    				RejectedCLCount.put("10000102",temp);
			    				BigDecimal tempBalance = new BigDecimal((data[3].trim().split("=")[1])).add(RejectedCLBalance.get("10000102")) ;
			    				RejectedCLBalance.put("10000102",tempBalance);
			    			}
			    			else
			    			{
			    				RejectedCLCount.put("10000102",1l);
			    				RejectedCLBalance.put("10000102",new BigDecimal((data[3].trim().split("=")[1])));
			    			}
			    		}
			    		if(!data[4].trim().split("=",-1)[1].isEmpty())
			    		{
			    			if(RejectedCLCount.containsKey("100000201"))
			    			{
			    				Long temp = RejectedCLCount.get("100000201") + 1l;
			    				RejectedCLCount.put("100000201",temp);
			    				BigDecimal tempBalance = new BigDecimal((data[4].trim().split("=")[1])).add(RejectedCLBalance.get("100000201")) ;
			    				RejectedCLBalance.put("100000201",tempBalance);
			    			}
			    			else
			    			{
			    				RejectedCLCount.put("100000201",1l);
			    				RejectedCLBalance.put("100000201",new BigDecimal(data[4].trim().split("=")[1]));
			    			}
			    		}
			    		if(!data[5].trim().split("=",-1)[1].isEmpty())
			    		{
			    			if(RejectedCLCount.containsKey("10000104"))
			    			{
			    				Long temp = RejectedCLCount.get("10000104") + 1l;
			    				RejectedCLCount.put("10000104",temp);
			    				BigDecimal tempBalance = new BigDecimal((data[5].trim().split("=")[1])).add(RejectedCLBalance.get("10000104")) ;
			    				RejectedCLBalance.put("10000104",tempBalance);
			    			}
			    			else
			    			{
			    				RejectedCLCount.put("10000104",1l);
			    				RejectedCLBalance.put("10000104",new BigDecimal(data[5].trim().split("=")[1]));
			    			}
			    		}
			    		if(!data[6].trim().split("=",-1)[1].isEmpty())
			    		{
			    			if(RejectedCLCount.containsKey("10000105"))
			    			{
			    				Long temp = RejectedCLCount.get("10000105") + 1l;
			    				RejectedCLCount.put("10000105",temp);
			    				BigDecimal tempBalance = new BigDecimal((data[6].trim().split("=")[1])).add(RejectedCLBalance.get("10000105")) ;
			    				RejectedCLBalance.put("10000105",tempBalance);
			    			}
			    			else
			    			{
			    				RejectedCLCount.put("10000105",1l);
			    				RejectedCLBalance.put("10000105",new BigDecimal(data[6].trim().split("=")[1]));
			    			}
			    		}
			    		if(!data[7].trim().split("=",-1)[1].isEmpty())
			    		{
			    			if(RejectedCLCount.containsKey("10000106"))
			    			{
			    				Long temp = RejectedCLCount.get("10000106") + 1l;
			    				RejectedCLCount.put("10000106",temp);
			    				BigDecimal tempBalance = new BigDecimal((data[7].trim().split("=")[1])).add(RejectedCLBalance.get("10000106")) ;
			    				RejectedCLBalance.put("10000106",tempBalance);
			    			}
			    			else
			    			{
			    				RejectedCLCount.put("10000106",1l);
			    				RejectedCLBalance.put("10000106",new BigDecimal(data[7].trim().split("=")[1]));
			    			}
			    		}
			    		if(!data[8].trim().split("=",-1)[1].isEmpty())
			    		{
			    			if(RejectedCLCount.containsKey("1000002"))
			    			{
			    				Long temp = RejectedCLCount.get("1000002") + 1l;
			    				RejectedCLCount.put("1000002",temp);
			    				BigDecimal tempBalance = new BigDecimal((data[8].trim().split("=")[1])).add(RejectedCLBalance.get("1000002")) ;
			    				RejectedCLBalance.put("1000002",tempBalance);
			    			}
			    			else
			    			{
			    				RejectedCLCount.put("1000002",1l);
			    				RejectedCLBalance.put("1000002",new BigDecimal(data[8].trim().split("=")[1]));
			    			}
			    		}
			    		if(!data[9].trim().split("=",-1)[1].isEmpty())
			    		{
			    			if(RejectedSCCount.containsKey(data[9].trim().split("=",-1)[1]))
			    			{
			    				Long temp = RejectedSCCount.get(data[9].trim().split("=",-1)[1]) + 1l;
			    				RejectedSCCount.put(data[9].trim().split("=",-1)[1],temp);
			    				
			    			}
			    			else
			    			{
			    				RejectedSCCount.put(data[9].trim().split("=",-1)[1],1l);		    				
			    			}
			    		}
		    		}
		    	}
		    	else if(data[0].equals("INC01"))
		    	{
		    		String[] Offer_UV_UT_Values = data[2].split("=")[1].split(",",-1);
		    		for(String s : Offer_UV_UT_Values)
		    		{
		    			if(s.isEmpty())
		    				continue;
		    			String[] Offer_UV_UT = s.split("\\-",-1);
		    			if(Offer_UV_UT.length == 3)
		    			{
		    				if(!Offer_UV_UT[0].isEmpty())
			    			{
			    				String OfferID = Offer_UV_UT[0].trim();
			    				if(RejectedOffer.containsKey(OfferID))
					    		{
					    			Long counter = RejectedOffer.get(OfferID) + 1L;
					    			RejectedOffer.put(OfferID, counter);
					    		}
					    		else
					    		{
					    			RejectedOffer.put(OfferID, 1L);
					    		}
			    			}
			    			if(!Offer_UV_UT[1].isEmpty() )
			    			{
			    				String UCValue = Offer_UV_UT[1];
			    				String OfferID = Offer_UV_UT[0];
			    				if(RejectedUCCount.containsKey(OfferID))
								{
									Long count = RejectedUCCount.get(OfferID);
									RejectedUCCount.put(OfferID,count + 1L);
									
									Long currentBalance = RejectedUCBalance.get(OfferID) + Long.parseLong(UCValue);
									RejectedUCBalance.put(OfferID,currentBalance);					
								}
								else
								{
									RejectedUCCount.put(OfferID, 1L);
									RejectedUCBalance.put(OfferID,Long.parseLong(UCValue));
								}
			    				RejectedUCTotalBalance = RejectedUCTotalBalance.add(new BigDecimal(UCValue));
			    			}
			    			else
			    			{
			    				String OfferID = Offer_UV_UT[0];
			    				if(RejectedZeroUCCount.containsKey(OfferID))
			    				{
			    					Long counter = RejectedZeroUCCount.get(OfferID) + 1L;
			    					RejectedZeroUCCount.put(OfferID, counter);			    			
			    				}
			    				else
			    				{
			    					RejectedZeroUCCount.put(OfferID, 1L);			    			
			    				}
			    			}
			    			if(!Offer_UV_UT[2].isEmpty())
			    			{
			    				String UTValue = Offer_UV_UT[2];
			    				String OfferID = Offer_UV_UT[0];
			    				if(RejectedUTCount.containsKey(OfferID))
								{
									Long count = RejectedUTCount.get(OfferID);
									RejectedUTCount.put(OfferID,count + 1L);
									
									Long currentBalance = RejectedUTBalance.get(OfferID) + Long.parseLong(UTValue);
									RejectedUTBalance.put(OfferID,currentBalance);					
								}
								else
								{
									RejectedUTCount.put(OfferID, 1L);
									RejectedUTBalance.put(OfferID,Long.parseLong(UTValue));
								}
			    				RejectedUTTotalBalance = RejectedUTTotalBalance.add(new BigDecimal(UTValue));
			    			}
			    			else
			    			{
			    				String OfferID = Offer_UV_UT[0];
			    				if(RejectedZeroUTCount.containsKey(OfferID))
			    				{
			    					Long counter = RejectedZeroUTCount.get(OfferID) + 1L;
			    					RejectedZeroUTCount.put(OfferID, counter);			    			
			    				}
			    				else
			    				{
			    					RejectedZeroUTCount.put(OfferID, 1L);			    			
			    				}
			    			}
		    			}
		    			
		    			/*if((Offer_UV_UT.length == 3) && line.startsWith("INC01:"))
		    			{
		    				
		    				if(!Offer_UV_UT[0].isEmpty())
			    			{
			    				String OfferID = Offer_UV_UT[0].trim();
			    				if(RejectedNotInCSOffer.containsKey(OfferID))
					    		{
					    			Long counter = RejectedNotInCSOffer.get(OfferID) + 1L;
					    			RejectedNotInCSOffer.put(OfferID, counter);
					    		}
					    		else
					    		{
					    			RejectedNotInCSOffer.put(OfferID, 1L);
					    		}
			    			}
		    			}*/
		    		}
		    	}		    		
		    }		    
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		
		 //Added Code to check for 
		  
		
		WriteOutputFile(RejectedOffer, "targetOfferRejectionCount.csv");
		WriteOutputFile(RejectedUCCount, "targetUCRejectionCount.csv");
		WriteOutputFile(RejectedUTCount, "targetUTRejectionCount.csv");
		WriteOutputFile(RejectedUCBalance, "targetUCRejectionBalance.csv");
		WriteOutputFile(RejectedUTBalance, "targetUTRejectionBalance.csv");
		WriteOutputFile(RejectedUCTotalBalance, "targetRejectedUCTotalBalance.csv");
		WriteOutputFile(RejectedUTTotalBalance, "targetRejectedUTTotalBalance.csv");
		WriteOutputFile(RejectedCLCount, "targetRejectedCLCount.csv");
		WriteOutputFile(RejectedCLBalance, "targetRejectedCLBalance.csv","");
		//appendOutputFile(RejectedOffer,"targetNotInCSRejection.csv");
		WriteOutputFile(RejectedOffer,"targetNotInCSRejection.csv");
		appendOutputFileReport(RejectedSCCount,"SCOrphanRejectionCount.csv");		
		WriteOutputFile(RejectedCLUCCount, "targetCLDummyRejectionCount.csv");
		//INC002 Append in not in CS log file
	}

	/*
	   TimeOffer - Expiry|Ignore Flag ( INC11, INC12)
	   Implicit_Shared_Offer - Expiry|Ignore Flag (INC21,INC22
	   Addon_Offer - Expiry|Ignore Flag (INC31, INC32)
	 */
	private void readExceptionLogs() {
		// TODO Auto-generated method stub
		HashMap<String, Long> expiryException = new HashMap<String, Long>();
		HashMap<String, Long> expiryUCException = new HashMap<String, Long>();
		HashMap<String, Long> expiryUTException = new HashMap<String, Long>();
		HashMap<String, BigDecimal> expiryUTExceptionBalances = new HashMap<String, BigDecimal>();
		HashMap<String, BigDecimal> expiryUCExceptionBalances = new HashMap<String, BigDecimal>();
		BigDecimal expiryUCTotalBalance = new BigDecimal("0");
		BigDecimal expiryUTTotalBalance = new BigDecimal("0");
		
		HashMap<String, Long> notInScopeException = new HashMap<String, Long>();
		HashMap<String, Long> notInScopeUCException = new HashMap<String, Long>();
		HashMap<String, Long> notInScopeUTException = new HashMap<String, Long>();
		HashMap<String, BigDecimal> notInScopeUTExceptionBalances = new HashMap<String, BigDecimal>();
		HashMap<String, BigDecimal> notInScopeUCExceptionBalances = new HashMap<String, BigDecimal>();
		BigDecimal notInScopeUCTotalBalance = new BigDecimal("0");
		BigDecimal notInScopeUTTotalBalance = new BigDecimal("0");
		
		//Phase2
		BigDecimal REFERENCE_CREDIT_LIMITBalance = new BigDecimal("0");
		BigDecimal AVAILABLE_CREDIT_LIMITBalance = new BigDecimal("0");
		BigDecimal SUBSCRIBER_BALANCEBalance = new BigDecimal("0");
		
		long REFERENCE_CREDIT_LIMIT = 0;
		long AVAILABLE_CREDIT_LIMIT = 0;
		long SUBSCRIBER_BALANCE = 0;
		
		
		try (BufferedReader br = new BufferedReader(new FileReader(logDirectoryPath + "Exception.log"));){
	 	   String line;
		    while ((line = br.readLine()) != null) {
		    	String datas[] = line.split(":",-1);
		    	//System.out.println(line);
		    	if(datas[0].startsWith("INC11") || datas[0].startsWith("INC21"))
		    	{
		    		//Calculation for Offer
		    		String OfferID = datas[2].split("=",-1)[1];
		    		
		    		if(expiryException.containsKey(OfferID))
		    		{
		    			Long counter = expiryException.get(OfferID) + 1L;
		    			expiryException.put(OfferID, counter);
		    		}
		    		else
		    		{
		    			expiryException.put(OfferID, 1L);
		    		}
		    		
		    		//Calculation for UC
		    		String UCBalance = datas[3].split("=",-1)[1];
		    		if(!UCBalance.isEmpty() && !UCBalance.equals("0"))
		    		{
		    			if(UCBalance.isEmpty() || UCBalance.contains("-"))
		    				continue;
		    			expiryUCTotalBalance = expiryUCTotalBalance.add(new BigDecimal(UCBalance));
		    			if(expiryUCException.containsKey(OfferID))
			    		{
			    			Long counter = expiryUCException.get(OfferID) + 1L;
			    			expiryUCException.put(OfferID, counter);
			    			BigDecimal currentBalance = expiryUCExceptionBalances.get(OfferID).add(new BigDecimal(UCBalance));
			    			expiryUCExceptionBalances.put(OfferID, currentBalance);				    			
			    		}
			    		else
			    		{
			    			expiryUCException.put(OfferID, 1L);
			    			expiryUCExceptionBalances.put(OfferID,new BigDecimal(UCBalance));
			    		}
		    		}
		    		else
		    		{
		    			if(RejectedZeroUCCount.containsKey(OfferID))
			    		{
			    			Long counter = RejectedZeroUCCount.get(OfferID) + 1L;
			    			RejectedZeroUCCount.put(OfferID, counter);			    			
			    		}
			    		else
			    		{
			    			RejectedZeroUCCount.put(OfferID, 1L);			    			
			    		}
		    		}
		    		
		    		//Calculation for UT
		    		String UTBalance = datas[4].split("=",-1)[1];
		    		if(!UTBalance.isEmpty() && !UTBalance.equals("0"))
		    		{
		    			expiryUTTotalBalance = expiryUTTotalBalance.add(new BigDecimal(UTBalance));
		    			if(expiryUTException.containsKey(OfferID))
			    		{
			    			Long counter = expiryUTException.get(OfferID) + 1L;
			    			expiryUTException.put(OfferID, counter);
			    			
			    			BigDecimal currentBalance = expiryUTExceptionBalances.get(OfferID).add(new BigDecimal(UTBalance));
			    			expiryUTExceptionBalances.put(OfferID, currentBalance);
			    		}
			    		else
			    		{
			    			expiryUTException.put(OfferID, 1L);
			    			expiryUTExceptionBalances.put(OfferID,new BigDecimal(UTBalance));
			    		}
		    		}
		    		else
		    		{
		    			if(RejectedZeroUTCount.containsKey(OfferID))
			    		{
			    			Long counter = RejectedZeroUTCount.get(OfferID) + 1L;
			    			RejectedZeroUTCount.put(OfferID, counter);			    			
			    		}
			    		else
			    		{
			    			RejectedZeroUTCount.put(OfferID, 1L);			    			
			    		}
		    		}
		    	}
		    	if(datas[0].startsWith("INC12") || datas[0].startsWith("INC22"))
		    	{
		    		//INC42:MSISDN=36044785:OFFER_ID=3000033:DESCRIPTION=Offer OUT OF SCOPE:ACTION=DISCARD & LOG
		    		String OfferID = datas[2].split("=",-1)[1];
		    		//Calculation for Offer
		    		if(notInScopeException.containsKey(OfferID))
		    		{
		    			Long counter = notInScopeException.get(OfferID) + 1L;
		    			notInScopeException.put(OfferID, counter);
		    		}
		    		else
		    		{
		    			notInScopeException.put(OfferID, 1L);
		    		}
		    		
		    		//Calculation for UC
		    		String UCBalance = datas[3].split("=",-1)[1];
		    		if(!UCBalance.isEmpty() && !UCBalance.equals("0"))
		    		{
		    			notInScopeUCTotalBalance = notInScopeUCTotalBalance.add(new BigDecimal(UCBalance));
		    			if(notInScopeUCException.containsKey(OfferID))
			    		{
			    			Long counter = notInScopeUCException.get(OfferID) + 1L;
			    			notInScopeUCException.put(OfferID, counter);
			    			
			    			BigDecimal currentBalance = notInScopeUCExceptionBalances.get(OfferID).add(new BigDecimal(UCBalance));
			    			notInScopeUCExceptionBalances.put(OfferID, currentBalance);
			    		}
			    		else
			    		{
			    			notInScopeUCException.put(OfferID, 1L);
			    			notInScopeUCExceptionBalances.put(OfferID,new BigDecimal(UCBalance));
			    		}
		    		}
		    		else
		    		{
		    			if(RejectedZeroUCCount.containsKey(OfferID))
			    		{
			    			Long counter = RejectedZeroUCCount.get(OfferID) + 1L;
			    			RejectedZeroUCCount.put(OfferID, counter);			    			
			    		}
			    		else
			    		{
			    			RejectedZeroUCCount.put(OfferID, 1L);			    			
			    		}
		    		}
		    		
		    		//Calculation for UT
		    		String UTBalance = datas[4].split("=",-1)[1];
		    		if(!UTBalance.isEmpty() && !UTBalance.equals("0"))
		    		{
		    			notInScopeUTTotalBalance = notInScopeUTTotalBalance.add(new BigDecimal(UTBalance));
		    			if(notInScopeUTException.containsKey(OfferID))
			    		{
			    			Long counter = notInScopeUTException.get(OfferID) + 1L;
			    			notInScopeUTException.put(OfferID, counter);
			    			
			    			BigDecimal currentBalance = notInScopeUTExceptionBalances.get(OfferID).add(new BigDecimal(UTBalance));
			    			notInScopeUTExceptionBalances.put(OfferID, currentBalance);
			    		}
			    		else
			    		{
			    			notInScopeUTException.put(OfferID, 1L);
			    			notInScopeUTExceptionBalances.put(OfferID,new BigDecimal(UTBalance));
			    		}
		    		}
		    		else
		    		{
		    			if(RejectedZeroUTCount.containsKey(OfferID))
			    		{
			    			Long counter = RejectedZeroUTCount.get(OfferID) + 1L;
			    			RejectedZeroUTCount.put(OfferID, counter);			    			
			    		}
			    		else
			    		{
			    			RejectedZeroUTCount.put(OfferID, 1L);			    			
			    		}
		    		}
		    	}
		    	//for Phase2
		    	if(datas[0].startsWith("INC32") || datas[0].startsWith("INC30") || datas[0].startsWith("INC41")|| datas[0].startsWith("INC42"))
		    	{
		    		if(datas[0].startsWith("INC30"))
		    		{
		    			//left it for notification as its not part of rejection
			    		//INC30:MSISDN=36177575:REFERENCE_CREDIT_LIMIT=20:AVAILABLE_CREDIT_LIMIT=0:SUBSCRIBER_BALANCE=25.102:DESCRIPTION=Mismatch in Reference, Available and Subuscriber Credit limit=DISCARD & LOG
		    			REFERENCE_CREDIT_LIMITBalance = REFERENCE_CREDIT_LIMITBalance.add(new BigDecimal(datas[2].split("=")[1]));
		    			AVAILABLE_CREDIT_LIMITBalance = AVAILABLE_CREDIT_LIMITBalance.add(new BigDecimal(datas[3].split("=")[1]));
		    			SUBSCRIBER_BALANCEBalance = SUBSCRIBER_BALANCEBalance.add(new BigDecimal(datas[4].split("=")[1]));
		    		
		    		}
		    		else if(datas[0].startsWith("INC32"))
		    		{
		    			//INC32:MSISDN=36611877:SUBSCRIBER_BALANCE=-10.424:DESCRIPTION=SUBSCRIBER_BALANCE is negative=DISCARD & LOG
		    			SUBSCRIBER_BALANCEBalance = SUBSCRIBER_BALANCEBalance.add(new BigDecimal(datas[2].split("=")[1]));
		    		}		    		
		    		REFERENCE_CREDIT_LIMIT +=1;
	    			AVAILABLE_CREDIT_LIMIT +=1;
	    			SUBSCRIBER_BALANCE +=1;	 
		    	}
		    }
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		
		//For Offer Write to output file
		WriteOutputFile(expiryException, "targetExpiryOfferException.csv");
		WriteOutputFile(notInScopeException, "targetOutOfScopeOfferException.csv");
		
		//For UC
		WriteOutputFile(expiryUCException, "targetExpiryUCException.csv");
		WriteOutputFile(notInScopeUCException, "targetOutOfScopeUCException.csv");
		WriteOutputFile(expiryUCTotalBalance, "targetExpiryUCExceptionTotalBalance.csv");
		WriteOutputFile(notInScopeUCTotalBalance, "targetOutOfScopeUCExceptionTotalBalance.csv");
		WriteOutputFile(expiryUCExceptionBalances, "targetExpiryUCExceptionBalance.csv","JustDummy");
		WriteOutputFile(notInScopeUCExceptionBalances, "targetOutOfScopeUCExceptionBalance.csv","JustDummy");
		
		//For UT
		WriteOutputFile(expiryUTException, "targetExpiryUTException.csv");
		WriteOutputFile(notInScopeUTException, "targetOutOfScopeUTException.csv");
		WriteOutputFile(expiryUTTotalBalance, "targetExpiryUTExceptionTotalBalance.csv");
		WriteOutputFile(notInScopeUTTotalBalance, "targetOutOfScopeUTExceptionTotalBalance.csv");	
		WriteOutputFile(expiryUTExceptionBalances, "targetExpiryUTExceptionBalance.csv","JustDummy");
		WriteOutputFile(notInScopeUTExceptionBalances, "targetOutOfScopeUTExceptionBalance.csv","JustDummy");
		
		
		//For phase2
		//write phase2 output
		try(BufferedWriter bw = new BufferedWriter(new FileWriter(tempDirectoryPath +  "Phase2RejectedCountlog.csv")))
		{
			bw.append("REFERENCE_CREDIT_LIMIT:" + REFERENCE_CREDIT_LIMIT);
			bw.append(System.lineSeparator());
			bw.append("AVAILABLE_CREDIT_LIMIT:" + AVAILABLE_CREDIT_LIMIT);
			bw.append(System.lineSeparator());
			bw.append("SUBSCRIBER_BALANCE:" + SUBSCRIBER_BALANCE);
			bw.append(System.lineSeparator());
			bw.flush();
			bw.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();  
		}
		try(BufferedWriter bw = new BufferedWriter(new FileWriter(tempDirectoryPath +  "Phase2RejectedBalancelog.csv")))
		{
			bw.append("REFERENCE_CREDIT_LIMIT:" + REFERENCE_CREDIT_LIMITBalance);
			bw.append(System.lineSeparator());
			bw.append("AVAILABLE_CREDIT_LIMIT:" + AVAILABLE_CREDIT_LIMITBalance);
			bw.append(System.lineSeparator());
			bw.append("SUBSCRIBER_BALANCE:" + SUBSCRIBER_BALANCEBalance);
			bw.append(System.lineSeparator());
			bw.flush();
			bw.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();  
		}
		
	}
	
	public void WriteOutputFile(BigDecimal OutputValue, String outputFileName)
	{
		try(BufferedWriter bw = new BufferedWriter(new FileWriter(tempDirectoryPath +  outputFileName)))
		{
			bw.append(OutputValue.toString());
			bw.append(System.lineSeparator());
			bw.flush();
			bw.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();  
		}		
	}
	
	public void appendOutputFile(BigDecimal OutputValue, String outputFileName)
	{
		try(BufferedWriter bw = new BufferedWriter(new FileWriter(tempDirectoryPath +  outputFileName)))
		{
			bw.append(OutputValue.toString());
			bw.append(System.lineSeparator());
			bw.flush();
			bw.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();  
		}		
	}
	
	public void WriteOutputFile(HashMap<String,Long> OutputMap, String outputFileName)
	{
		try(BufferedWriter bw = new BufferedWriter(new FileWriter(tempDirectoryPath +  outputFileName)))
		{
			for(Entry<String,Long> str : OutputMap.entrySet())
			{
			  bw.append(str.getKey() + ":" + str.getValue());
			  bw.append(System.lineSeparator());
			}
			bw.flush();
			bw.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();  
		}		
	}
	
	public void appendOutputFile(HashMap<String,Long> OutputMap, String outputFileName)
	{
		try(BufferedWriter bw = new BufferedWriter(new FileWriter(tempDirectoryPath +  outputFileName, true)))
		{
			for(Entry<String,Long> str : OutputMap.entrySet())
			{
			  bw.append(str.getKey() + ":" + str.getValue());
			  bw.append(System.lineSeparator());
			}
			bw.flush();
			bw.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();  
		}		
	}
	
	public void appendOutputFileReport(HashMap<String,Long> OutputMap, String outputFileName)
	{
		try(BufferedWriter bw = new BufferedWriter(new FileWriter(tempDirectoryPath +  outputFileName, true)))
		{
			for(Entry<String,Long> str : OutputMap.entrySet())
			{
			  bw.append(str.getKey() + ":" + str.getValue());
			  bw.append(System.lineSeparator());
			}
			bw.flush();
			bw.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();  
		}		
	}
	
	public void WriteOutputFile(HashMap<String,BigDecimal> OutputMap, String outputFileName, String justDummy)
	{
		try(BufferedWriter bw = new BufferedWriter(new FileWriter(tempDirectoryPath +  outputFileName)))
		{
			for(Entry<String,BigDecimal> str : OutputMap.entrySet())
			{
			  bw.append(str.getKey() + ":" + str.getValue());
			  bw.append(System.lineSeparator());
			}
			bw.flush();
			bw.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();  
		}		
	}
	
	
	public static void main(String[] args) {
		String MigtoolPath = "C:\\Ericsson\\MyWorkingProject\\Charging_System\\dm_cs_2020_zain_bahrain\\dev\\src\\";
		CalculateRejectionReport crr = new CalculateRejectionReport(MigtoolPath);
		crr.execute();
	}

}
