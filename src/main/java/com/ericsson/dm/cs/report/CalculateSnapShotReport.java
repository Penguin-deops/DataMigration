package com.ericsson.dm.cs.report;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.FileWriter;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.filefilter.WildcardFileFilter;

public class CalculateSnapShotReport {
   String SnapShotPrePath = "";
   String SnapShotPostPath = "";
   String migToolPath = "";
   String tempDirectoryPath = "";
   String SortedOutputPath = "";
   String OutputPath = "";
      
   Map<String,Set<String>> OfferMSISDN;
   Map<String,Set<String>> UCMSISDN;
   Map<String,Set<String>> DAMSISDN;
   Set<String> SUBUSCRIBERMSISDN;
   Map<String,Set<String>> UTMSISDN;
   Map<String,Set<String>> SubOfferMSISDN;
   
   
   /*String preOfferSnapShotName = "";
   String postOfferSnapShotName = "";
   String preUCSnapShotName = "";
   String posUCfferSnapShotName = "";
   String preUTSnapShotName = "";
   String postUTSnapShotName = "";
   String preSubsOfferSnapShotName = "";
   String postSubsOfferSnapShotName = "";
   //String preSnapShotName = "";
   //String postOfferSnapShotName = "";
   
   public final Map<String, Long> KPIPreOfferSnapShot = new HashMap<String, Long>();
   public final Map<String, Long> KPIPreUsageThresholdSnapShot = new HashMap<String, Long>();
   public final Map<String, Long> KPIPreSubuscriberOfferSnapShot = new HashMap<String, Long>();
   public final Map<String, Long> KPIPreUsageCounterSnapShot = new HashMap<String, Long>();
   
   public final Map<String, Long> KPIPostOfferSnapShot = new HashMap<String, Long>();
   public final Map<String, Long> KPIPostUsageThresholdSnapShot = new HashMap<String, Long>();
   public final Map<String, Long> KPIPostSubuscriberOfferSnapShot = new HashMap<String, Long>();
   public final Map<String, Long> KPIPostUsageCounterSnapShot = new HashMap<String, Long>();*/
   
   public CalculateSnapShotReport(String MigtoolPath) {
	  // TODO Auto-generated constructor stub
	   this.migToolPath = MigtoolPath;
	   this.SnapShotPostPath = MigtoolPath + "/PostSnapshot/";
	   this.SnapShotPrePath = MigtoolPath + "/PreSnapshot/";
	   this.tempDirectoryPath = MigtoolPath + "/Temp/";
	   this.SortedOutputPath = MigtoolPath + "/Output/Sorted/";
	   this.OutputPath = MigtoolPath + "/Output/";
	   
	   
	   this.OfferMSISDN = new HashMap<String,Set<String>>();
	   this.UCMSISDN = new HashMap<String,Set<String>>();
	   this.UTMSISDN = new HashMap<String,Set<String>>();
	   this.DAMSISDN = new HashMap<String,Set<String>>();
	   this.SubOfferMSISDN = new HashMap<String,Set<String>>();
	   this.SUBUSCRIBERMSISDN = new HashSet<String>();
   }
   
   public void execute()
   {
	   //Get all the MSISDN
	   BuildMSISDNList();
	   //List down all the files from pre snapshot.
	   GetpreSnapshot();
	   GetpostSnapshot();	   
   }
  
   
	 private void BuildMSISDNList() {
	   // TODO Auto-generated method stub
	   //for Offer.csv
	   try (BufferedReader br = new BufferedReader(new FileReader(this.SortedOutputPath + "Offer.csv"));){
	 	   String line;
		    while ((line = br.readLine()) != null) {
		    	String datas[] = line.split(",",-1);
		    	
		    	if(OfferMSISDN.containsKey(datas[1]))
		    	{
		    		Set<String> temp = OfferMSISDN.get(datas[1]);
		    		temp.add(datas[0]);
		    		OfferMSISDN.put(datas[1], temp);
		    	}
		    	else
		    	{
		    		Set<String> temp = new HashSet<String>();
		    		temp.add(datas[0]);
		    		OfferMSISDN.put(datas[1], temp);
		    	}	    	
			}
		    br.close();			  
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	   
	   try (BufferedReader br = new BufferedReader(new FileReader(this.OutputPath + "DedicatedAccount"));){
	 	   String line;
		    while ((line = br.readLine()) != null) {
		    	String datas[] = line.split(",",-1);
		    	
		    	if(DAMSISDN.containsKey(datas[3]))
		    	{
		    		Set<String> temp = DAMSISDN.get(datas[3]);
		    		temp.add(datas[1]);
		    		DAMSISDN.put(datas[3], temp);
		    	}
		    	else
		    	{
		    		Set<String> temp = new HashSet<String>();
		    		temp.add(datas[1]);
		    		DAMSISDN.put(datas[3], temp);
		    	}	    	
			}
		    br.close();			  
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	   
	   	try (BufferedReader br = new BufferedReader(new FileReader(this.SortedOutputPath + "UsageThreshold.csv"));){
	 	   String line;
		    while ((line = br.readLine()) != null) {
		    	String datas[] = line.split(",",-1);
				if(UTMSISDN.containsKey(datas[1]))
		    	{
		    		Set<String> temp = UTMSISDN.get(datas[1]);
		    		temp.add(datas[0]);
		    		UTMSISDN.put(datas[1], temp);
		    	}
		    	else
		    	{
		    		Set<String> temp = new HashSet<String>();
		    		temp.add(datas[0]);
		    		UTMSISDN.put(datas[1], temp);
		    	}
			}
		    br.close();			  
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	   
	    try (BufferedReader br = new BufferedReader(new FileReader(this.SortedOutputPath + "UsageCounter.csv"));){
	 	   String line;
		    while ((line = br.readLine()) != null) {
		    	String datas[] = line.split(",",-1);
				
				if(UCMSISDN.containsKey(datas[1]))
		    	{
		    		Set<String> temp = UCMSISDN.get(datas[1]);
		    		temp.add(datas[0]);
		    		UCMSISDN.put(datas[1], temp);
		    	}
		    	else
		    	{
		    		Set<String> temp = new HashSet<String>();
		    		temp.add(datas[0]);
		    		UCMSISDN.put(datas[1], temp);
		    	}
			}
		    br.close();			  
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	    
	    try (BufferedReader br = new BufferedReader(new FileReader(this.SortedOutputPath + "Credit_Limit_UC.csv"));){
	 	   String line;
		    while ((line = br.readLine()) != null) {
		    	String datas[] = line.split(",",-1);
				
				if(UCMSISDN.containsKey(datas[2]))
		    	{
		    		Set<String> temp = UCMSISDN.get(datas[2]);
		    		temp.add(datas[1]);
		    		UCMSISDN.put(datas[2], temp);
		    	}
		    	else
		    	{
		    		Set<String> temp = new HashSet<String>();
		    		temp.add(datas[1]);
		    		UCMSISDN.put(datas[2], temp);
		    	}
			}
		    br.close();			  
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	   

	    try (BufferedReader br = new BufferedReader(new FileReader(this.SortedOutputPath + "SubscriberOffer.csv"));){
	 	   String line;
		    while ((line = br.readLine()) != null) {
		    	String datas[] = line.split(",",-1);
				if(SubOfferMSISDN.containsKey(datas[1]))
		    	{
		    		Set<String> temp = SubOfferMSISDN.get(datas[1]);
		    		temp.add(datas[0]);
		    		SubOfferMSISDN.put(datas[1], temp);
		    	}
		    	else
		    	{
		    		Set<String> temp = new HashSet<String>();
		    		temp.add(datas[0]);
		    		SubOfferMSISDN.put(datas[1], temp);
		    	}
			}
		    br.close();			  
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	    
	    try (BufferedReader br = new BufferedReader(new FileReader(this.OutputPath + "ServiceClass"));){
	 	   String line;
		   while ((line = br.readLine()) != null) {
		    	String datas[] = line.split(",",-1);
		    	SUBUSCRIBERMSISDN.add(datas[1]+ ":" + datas[9]);		    		    	
			}
		    br.close();			  
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		   

   }
   private void GetpreSnapshot() {
	// TODO Auto-generated method stub
	   File dir = new File(this.SnapShotPrePath);
	   FileFilter fileFilter = new WildcardFileFilter("*.v4.csv");
	   File[] files = dir.listFiles(fileFilter);
	   for (int i = 0; i < files.length; i++) {
		   //System.out.println(files[i]);
		   if(files[i].getName().endsWith("DUMP_offer.v4.csv"))
		   {
			   CalculateSnapShortReport(files[i].toString(), "preSnapShotOfferCount.csv","OFFER");
		   }
		   else if(files[i].getName().endsWith("DUMP_usage_threshold.v4.csv"))
		   {
			   CalculateSnapShortReport(files[i].toString(), "preSnapShotUTCount.csv","UT");
			   CalculateSnapShortBalanceReport(files[i].toString(), "preSnapShotUTBalance.csv","UT");
		   }
		   else if(files[i].getName().endsWith("DUMP_usage_counter.v4.csv"))
		   {
			   CalculateSnapShortReport(files[i].toString(), "preSnapShotUCCount.csv","UC");
			   CalculateSnapShortBalanceReport(files[i].toString(), "preSnapShotUCBalance.csv","UC");
		   }
		   else if(files[i].getName().endsWith("DUMP_subscriber_offer.v4.csv"))
		   {
			   CalculateSnapShortReport(files[i].toString(), "preSnapShotSubOfferCount.csv","SUBSOFFER");
		   }
		   else if(files[i].getName().endsWith("DUMP_dedicatedaccount.v4.csv"))
		   {
			   CalculateSnapShortReport(files[i].toString(), "preSnapShotDACount.csv","DACHECK");
			   CalculateSnapShortDedicatedBalanceReport(files[i].toString(), "preSnapShotDABalance.csv");
		   }
		   else if(files[i].getName().endsWith("DUMP_subscriber.v4.csv"))
		   {
			   CalculateSnapShortReport(files[i].toString(), "preSubuscriberCount.csv","SCCheck");
		   }
	   }	
   }

   private void GetpostSnapshot() {
	   // TODO Auto-generated method stub
	   File dir = new File(this.SnapShotPostPath);
	   FileFilter fileFilter = new WildcardFileFilter("*.v4.csv");
	   File[] files = dir.listFiles(fileFilter);
	   for (int i = 0; i < files.length; i++) {
		   //System.out.println(files[i]);
		   if(files[i].getName().endsWith("DUMP_offer.v4.csv"))
		   {
			   CalculateSnapShortReport(files[i].toString(), "postSnapShotOfferCount.csv","OFFER");
		   }
		   else if(files[i].getName().endsWith("DUMP_usage_threshold.v4.csv"))
		   {
			   CalculateSnapShortReport(files[i].toString(), "postSnapShotUTCount.csv","UT");
			   CalculateSnapShortBalanceReport(files[i].toString(), "postSnapShotUTBalance.csv","UT");
		   }
		   else if(files[i].getName().endsWith("DUMP_usage_counter.v4.csv"))
		   {
			   CalculateSnapShortReport(files[i].toString(), "postSnapShotUCCount.csv","UC");
			   CalculateSnapShortBalanceReport(files[i].toString(), "postSnapShotUCBalance.csv","UC");
		   }
		   else if(files[i].getName().endsWith("DUMP_subscriber_offer.v4.csv"))
		   {
			   CalculateSnapShortReport(files[i].toString(), "postSnapShotSubOfferCount.csv","SUBSOFFER");
		   }
		   else if(files[i].getName().endsWith("DUMP_dedicatedaccount.v4.csv"))
		   {
			   CalculateSnapShortReport(files[i].toString(), "postSnapShotDACount.csv","DACHECK");
			   CalculateSnapShortDedicatedBalanceReport(files[i].toString(), "postSnapShotDABalance.csv");
		   }
		   else if(files[i].getName().endsWith("DUMP_subscriber.v4.csv"))
		   {
			   CalculateSnapShortReport(files[i].toString(), "postSubuscriberCount.csv","SCCheck");
		   }
	   }
	}

   
   public void CalculateSnapShortReport(String path, String outputFileName, String FileName)
   {
	   Map<String, Long> KPISnapShot = new HashMap<String, Long>();
	   try (BufferedReader br = new BufferedReader(new FileReader(path));){
	 	   String line;
		    while ((line = br.readLine()) != null) {
		    	String datas[] = line.split(",",-1);
		    	
		    	if(FileName.equals("OFFER"))
		    	{
		    		if(OfferMSISDN.containsKey(datas[1]))
		    		{
		    			Set<String> msisdnlist = OfferMSISDN.get(datas[1]);
		    			if(msisdnlist != null && msisdnlist.contains(datas[0]))
		    			{
		    				if(KPISnapShot.containsKey(datas[1]))
							{
								Long temp = KPISnapShot.get(datas[1]);
								KPISnapShot.put(datas[1], temp+1);
							}
							else
							{
								KPISnapShot.put(datas[1], 1L);
							}
		    			}
		    			else
		    			{
		    				//KPISnapShot.put(datas[1], 0L);
		    			}
		    		}
		    	}
		    	else if(FileName.equals("UC"))
		    	{
		    		if(UCMSISDN.containsKey(datas[1]))
		    		{
		    			Set<String> msisdnlist = UCMSISDN.get(datas[1]);
		    			//if(datas[1].equals("1000001"))
		    			//System.out.println();
		    			if(msisdnlist != null && msisdnlist.contains(datas[0]))
		    			{
		    				if(KPISnapShot.containsKey(datas[1]))
							{
								Long temp = KPISnapShot.get(datas[1]);
								KPISnapShot.put(datas[1], temp+1);
							}
							else
							{
								KPISnapShot.put(datas[1], 1L);
							}
		    			}
		    			else
		    			{
		    				//KPISnapShot.put(datas[1], 0L);
		    			}
		    		}
		    	}
		    	else if(FileName.equals("UT"))
		    	{
		    		if(UTMSISDN.containsKey(datas[1]))
		    		{
		    			Set<String> msisdnlist = UTMSISDN.get(datas[1]);
		    			if(msisdnlist != null &&  msisdnlist.contains(datas[0]))
		    			{
		    				if(KPISnapShot.containsKey(datas[1]))
							{
								Long temp = KPISnapShot.get(datas[1]);
								KPISnapShot.put(datas[1], temp+1);
							}
							else
							{
								KPISnapShot.put(datas[1], 1L);
							}
		    			}
		    			else
		    			{
		    				//KPISnapShot.put(datas[1], 0L);
		    			}
		    		}
		    	}
		    	else if(FileName.equals("SUBSOFFER"))
		    	{
		    		if(SubOfferMSISDN.containsKey(datas[1]))
		    		{
		    			Set<String> msisdnlist = SubOfferMSISDN.get(datas[1]);
		    			if(msisdnlist != null && msisdnlist.contains(datas[0]))
		    			{
		    				if(KPISnapShot.containsKey(datas[1]))
							{
								Long temp = KPISnapShot.get(datas[1]);
								KPISnapShot.put(datas[1], temp+1);
							}
							else
							{
								KPISnapShot.put(datas[1], 1L);
							}
		    			}
		    			else
		    			{
		    				//KPISnapShot.put(datas[1], 0L);
		    			}
		    		}
		    	}
		    	else if(FileName.equals("DACHECK"))
		    	{
		    		Set<String> msisdnlist = DAMSISDN.get(datas[1]);
	    			if(msisdnlist != null && msisdnlist.contains(datas[0]))
	    			{
	    				if(KPISnapShot.containsKey(datas[1]))
						{
							Long temp = KPISnapShot.get(datas[1]);
							KPISnapShot.put(datas[1], temp+1);
						}
						else
						{
							KPISnapShot.put(datas[1], 1L);
						}
	    			}		    		
		    	}
		    	else if(FileName.equals("SCCheck"))
		    	{
		    		if(SUBUSCRIBERMSISDN.contains(datas[1] + ":" + datas[18]))
		    		{
		    			if(KPISnapShot.containsKey(datas[18]))
						{
							Long temp = KPISnapShot.get(datas[18]);
							KPISnapShot.put(datas[18], temp+1);
						}
						else
						{
							KPISnapShot.put(datas[18], 1L);
						}
		    		}
		    	}
			}
		    br.close();			  
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	   
	    //now Write the file to temp directory
	    try(BufferedWriter bw = new BufferedWriter(new FileWriter(tempDirectoryPath +  outputFileName)))
		{
			for(Entry<String,Long> str : KPISnapShot.entrySet())
			{
			  bw.append(str.getKey() + ":" + str.getValue());
			  bw.append(System.lineSeparator());
			}
			bw.flush();
			bw.close();
		}
		catch(Exception e)
		{
			e.getStackTrace();  
		}
   }
   
   public void CalculateSnapShortBalanceReport(String path, String outputFileName,String FileName)
   {
	   Map<String, BigDecimal> KPISnapShot = new HashMap<String, BigDecimal>();
	   try (BufferedReader br = new BufferedReader(new FileReader(path));){
	 	   String line;
		    while ((line = br.readLine()) != null) {
		    	String datas[] = line.split(",",-1);
		    	
		    	if(FileName.equals("UC"))
		    	{
		    		if(UCMSISDN.containsKey(datas[1]))
		    		{
		    			Set<String> msisdnlist = UCMSISDN.get(datas[1]);
		    			if(msisdnlist != null && msisdnlist.contains(datas[0]))
		    			{
		    				if(!datas[2].isEmpty() && datas[2].length() > 0) {
					    		if(KPISnapShot.containsKey(datas[1]))
								{
					    			BigDecimal temp = KPISnapShot.get(datas[1]).add(new BigDecimal(datas[3]));
									KPISnapShot.put(datas[1], temp);
								}
								else
								{
									KPISnapShot.put(datas[1], new BigDecimal(datas[3]));
								}
					    	}
		    				else
			    			{
			    				//KPISnapShot.put(datas[1], new BigDecimal("0"));
			    			}
		    			}
		    		}		    		
		    	}		    
		    	else if(FileName.equals("UT"))
		    	{
		    		if(UTMSISDN.containsKey(datas[1]))
		    		{
		    			Set<String> msisdnlist = UTMSISDN.get(datas[1]);
		    			if(msisdnlist != null && msisdnlist.contains(datas[0]))
		    			{
		    				if(!datas[2].isEmpty() && datas[2].length() > 0) {
					    		if(KPISnapShot.containsKey(datas[1]))
								{
					    			BigDecimal temp = KPISnapShot.get(datas[1]).add(new BigDecimal(datas[3]));
									KPISnapShot.put(datas[1], temp);
								}
								else
								{
									KPISnapShot.put(datas[1], new BigDecimal(datas[3]));
								}
					    	}
		    				else
			    			{
			    				//KPISnapShot.put(datas[1], new BigDecimal("0"));
			    			}
		    			}		    			
		    		}
		    	}
		    		
			}
		    br.close();			  
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	   
	   //now Write the file to temp directory
	   try(BufferedWriter bw = new BufferedWriter(new FileWriter(tempDirectoryPath +  outputFileName)))
		{
			for(Entry<String,BigDecimal> str : KPISnapShot.entrySet())
			{
			  bw.append(str.getKey() + ":" + str.getValue());
			  bw.append(System.lineSeparator());
			}
			bw.flush();
			bw.close();
		}
		catch(Exception e)
		{
			e.getStackTrace();  
		}
    }
    
   public void CalculateSnapShortDedicatedBalanceReport(String path, String outputFileName)
   {
	   Map<String, BigDecimal> KPISnapShot = new HashMap<String, BigDecimal>();
	   try (BufferedReader br = new BufferedReader(new FileReader(path));){
	 	   String line;
		    while ((line = br.readLine()) != null) {
		    	String datas[] = line.split(",",-1);
		    	if(!datas[10].isEmpty() && datas[10].length() > 0) {
		    		if(DAMSISDN.containsKey(datas[1]))
		    		{
		    			Set<String> msisdnlist = DAMSISDN.get(datas[1]);
		    			if(msisdnlist != null && msisdnlist.contains(datas[0]))
		    			{
		    				if(KPISnapShot.containsKey(datas[1]))
							{
				    			BigDecimal temp = KPISnapShot.get(datas[1]).add(new BigDecimal(datas[10]));
								KPISnapShot.put(datas[1], temp);
							}
							else
							{
								KPISnapShot.put(datas[1], new BigDecimal(datas[10]));
							}
		    			}
		    		}		    		
		    	}				
			}
		    br.close();			  
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	   
	   //now Write the file to temp directory
	   try(BufferedWriter bw = new BufferedWriter(new FileWriter(tempDirectoryPath +  outputFileName)))
		{
			for(Entry<String,BigDecimal> str : KPISnapShot.entrySet())
			{
			  bw.append(str.getKey() + ":" + str.getValue());
			  bw.append(System.lineSeparator());
			}
			bw.flush();
			bw.close();
		}
		catch(Exception e)
		{
			e.getStackTrace();  
		}
    }
   
	public static void main(String[] args) {
	
		String MigtoolPath = "C:\\Ericsson\\MyWorkingProject\\Charging_System\\dm_cs_2020_zain_bahrain\\dev\\src\\";
		CalculateSnapShotReport csr = new CalculateSnapShotReport(MigtoolPath);
		csr.execute();
		
	}
}


