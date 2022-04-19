package com.ericsson.dm.cs.report;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.FileWriter;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.filefilter.WildcardFileFilter;

import com.ericsson.dm.inititialization.LoadSubscriberMapping;

public class CalculateTargetReport {

	String migToolPath = "";
	String outputDirectoryPath = "";
	String tempDirectoryPath = "";
	String opDirectoryPath = "";
	
	public CalculateTargetReport(String MigtoolPath) {
		// TODO Auto-generated constructor stub
		this.migToolPath = MigtoolPath;
		this.outputDirectoryPath = MigtoolPath + "/Output/Sorted/";
		this.opDirectoryPath = MigtoolPath + "/Output/";
		this.tempDirectoryPath = MigtoolPath + "/Temp/";
	}
	
	public void execute()
    {
	    //List down all the files from pre snapshot.
		generateTargetReport(outputDirectoryPath + "Offer.csv", "targetOfferCount.csv");
		generateTargetReport(outputDirectoryPath + "ProviderOffer.csv", "targetProviderOfferCount.csv");
		generateTargetReport(outputDirectoryPath + "SubscriberOffer.csv", "targetSubscriberOfferCount.csv");
		generateTargetReport(outputDirectoryPath + "UsageCounter.csv", "targetUsageCounterCount.csv");
		generateTargetReportCreditLimitUC(outputDirectoryPath + "Credit_Limit_UC.csv", "targetCreditLimitUCCount.csv");
		generateTargetReportBalance(outputDirectoryPath + "UsageCounter.csv", "targetUsageCounterBalance.csv","targetUsageCounterTotalBalance.csv");
		generateTargetReport(outputDirectoryPath + "UsageThreshold.csv", "targetUsageThresholdCount.csv");
		generateTargetReportBalance(outputDirectoryPath + "UsageThreshold.csv", "targetUsageThresholdBalance.csv","targetUsageThresholdTotalBalance.csv");
		generateTargetReportforDA(outputDirectoryPath + "DedicatedAccount", "targetDedicatedAccountDatCount.csv");
		generateTargetReportforSubuscriberDat(outputDirectoryPath , "targetSUBSCRPTDATADatBalance.csv");
	
		//generateKPI for Phase2
		generateTargetReportPhase2(outputDirectoryPath + "Credit_Limit_UC.csv", "Phase2targetCreditLimitUCCount.csv");
		generateTargetReportBalancePhase2(outputDirectoryPath + "Credit_Limit_UC.csv", "Phase2targetCreditLimitUCBalance.csv","Phase2targetCreditLimitUCTotalBalance.csv");
		generateTargetReportSC(opDirectoryPath + "ServiceClass", "Phase2TargetServiceClassCount.csv");
		//generateTargetReportBalancePhase2(outputDirectoryPath + "UsageThreshold.csv", "Phase2targetUsageThresholdBalance.csv","targetUsageThresholdTotalBalance.csv");
    
    }
	
	private void generateTargetReportforSubuscriberDat(String outputDirectoryPath, String FileName) {
		// TODO Auto-generated method stub
		Map<String, BigDecimal> KPISnapShot = new HashMap<String, BigDecimal>();
		File dir = new File(outputDirectoryPath);
		FileFilter fileFilter = new WildcardFileFilter("SUBSCRPT_DATA_*.DAT");
		File[] files = dir.listFiles(fileFilter);
		for (int i = 0; i < files.length; i++) {
		   System.out.println(files[i]);		   
		   //BigDecimal KPITotalBalance = new BigDecimal("0"); 
		   try (BufferedReader br = new BufferedReader(new FileReader(files[i]));){
		 	   String line;
		 	   br.readLine();
			    while ((line = br.readLine()) != null) {
			    	String datas[] = line.split(",",-1);
			    	if(!datas[4].isEmpty())
			    	{
				    	if(KPISnapShot.containsKey(datas[3]))
						{
			    			BigDecimal temp = KPISnapShot.get(datas[3]).add(new BigDecimal(datas[4]));
							KPISnapShot.put(datas[3], temp);
						}
						else
						{
							KPISnapShot.put(datas[3], new BigDecimal(datas[4]));
						}
			    	}
			    	//KPITotalBalance = KPITotalBalance.add(new BigDecimal(datas[3].toString()));
				}
			    br.close();			  
			}
			catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}
		}
	   //now Write the file to temp directory
	    try(BufferedWriter bw = new BufferedWriter(new FileWriter(tempDirectoryPath +  FileName)))
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

	public void generateTargetReport(String path, String outputFileName)
    {
	   Map<String, Long> KPISnapShot = new HashMap<String, Long>();
	   try (BufferedReader br = new BufferedReader(new FileReader(path));){
	 	   String line;
		    while ((line = br.readLine()) != null) {
		    	String datas[] = line.split(",",-1);
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
	
	public void generateTargetReportCreditLimitUC(String path, String outputFileName)
    {
	   Map<String, Long> KPISnapShot = new HashMap<String, Long>();
	   BigDecimal KPISnapShotBalance = new BigDecimal("0");
	   try (BufferedReader br = new BufferedReader(new FileReader(path));){
	 	   String line;
		    while ((line = br.readLine()) != null) {
		    	String datas[] = line.split(",",-1);
				if(KPISnapShot.containsKey(datas[2]))
				{
					Long temp = KPISnapShot.get(datas[2]);
					KPISnapShot.put(datas[2], temp+1);
				}
				else
				{
					KPISnapShot.put(datas[2], 1L);
				}
				KPISnapShotBalance = KPISnapShotBalance.add(new BigDecimal(datas[3]));
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
	   
	   	try(BufferedWriter bw = new BufferedWriter(new FileWriter(tempDirectoryPath +  "targetCreditLimitUCBalance.csv")))
		{
			bw.append("1000001:" + KPISnapShotBalance);
			bw.append(System.lineSeparator());
			bw.flush();
			bw.close();
		}
		catch(Exception e)
		{
			e.getStackTrace();  
		}
    }
	
	public void generateTargetReportBalance(String path, String outputFileName, String TotalBalanceFileName)
	   {
		   Map<String, BigDecimal> KPISnapShot = new HashMap<String, BigDecimal>();
		   Map<String, BigDecimal> UTBalanceForRohit = new HashMap<String, BigDecimal>();
		   BigDecimal KPITotalBalance = new BigDecimal("0"); 
		   try (BufferedReader br = new BufferedReader(new FileReader(path));){
		 	   String line;
			    while ((line = br.readLine()) != null) {
			    	String datas[] = line.split(",",-1);
			    	if(datas[1].equals("10000103"))
			    		UTBalanceForRohit.put(datas[0],new BigDecimal(datas[3]));
			    	if(KPISnapShot.containsKey(datas[1]))
					{
		    			BigDecimal temp = KPISnapShot.get(datas[1]).add(new BigDecimal(datas[3]));
						KPISnapShot.put(datas[1], temp);
					}
					else
					{
						KPISnapShot.put(datas[1], new BigDecimal(datas[3]));
					}
			    	KPITotalBalance = KPITotalBalance.add(new BigDecimal(datas[3].toString()));
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
		   
		   try(BufferedWriter bw = new BufferedWriter(new FileWriter(tempDirectoryPath +  TotalBalanceFileName)))
			{
				bw.append(KPITotalBalance.toString());
				bw.append(System.lineSeparator());
				bw.flush();
				bw.close();
			}
			catch(Exception e)
			{
				e.getStackTrace();  
			}
		   
		   /*try(BufferedWriter bw = new BufferedWriter(new FileWriter(tempDirectoryPath +  "ForRohit.csv")))
			{
				for(Entry<String,BigDecimal> str : UTBalanceForRohit.entrySet())
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
			}*/
		   
	   }
	
	public void generateTargetReportforDA(String path, String outputFileName)
    {
	   Map<String, Long> KPISnapShot = new HashMap<String, Long>();
	   try (BufferedReader br = new BufferedReader(new FileReader(path));){
	 	   String line;
		    while ((line = br.readLine()) != null) {
		    	String datas[] = line.split(",",-1);
				if(KPISnapShot.containsKey(datas[3]))
				{
					Long temp = KPISnapShot.get(datas[3]);
					KPISnapShot.put(datas[3], temp+1);
				}
				else
				{
					KPISnapShot.put(datas[3], 1L);
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
	
	public void generateTargetReportPhase2(String path, String outputFileName)
    {
	   Map<String, Long> KPISnapShot = new HashMap<String, Long>();
	   try (BufferedReader br = new BufferedReader(new FileReader(path));){
	 	   String line;
		    while ((line = br.readLine()) != null) {
		    	String datas[] = line.split(",",-1);
				if(KPISnapShot.containsKey(datas[2]))
				{
					Long temp = KPISnapShot.get(datas[2]);
					KPISnapShot.put(datas[2], temp+1);
				}
				else
				{
					KPISnapShot.put(datas[2], 1L);
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
	
	public void generateTargetReportSC(String path, String outputFileName)
    {
	   Map<String, Long> KPISnapShot = new HashMap<String, Long>();
	   try (BufferedReader br = new BufferedReader(new FileReader(path));){
	 	   String line;
		    while ((line = br.readLine()) != null) {
		    	String datas[] = line.split(",",-1);
		    	if(KPISnapShot.containsKey(datas[9]))
				{
					Long temp = KPISnapShot.get(datas[9]);
					KPISnapShot.put(datas[9], temp+1);
				}
				else
				{
					KPISnapShot.put(datas[9], 1L);
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
	
	public void generateTargetReportBalancePhase2(String path, String outputFileName, String TotalBalanceFileName)
    {
	   Map<String, BigDecimal> KPISnapShot = new HashMap<String, BigDecimal>();
	   BigDecimal KPITotalBalance = new BigDecimal("0"); 
	   try (BufferedReader br = new BufferedReader(new FileReader(path));){
	 	   String line;
		    while ((line = br.readLine()) != null) {
		    	String datas[] = line.split(",",-1);
		    	if(KPISnapShot.containsKey(datas[2]))
				{
	    			BigDecimal temp = KPISnapShot.get(datas[2]).add(new BigDecimal(datas[3]));
					KPISnapShot.put(datas[2], temp);
				}
				else
				{
					KPISnapShot.put(datas[2], new BigDecimal(datas[3]));
				}
		    	KPITotalBalance = KPITotalBalance.add(new BigDecimal(datas[3].toString()));
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
	   
	    try(BufferedWriter bw = new BufferedWriter(new FileWriter(tempDirectoryPath +  TotalBalanceFileName)))
		{
			bw.append(KPITotalBalance.toString());
			bw.append(System.lineSeparator());
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
		CalculateTargetReport crr = new CalculateTargetReport(MigtoolPath);
		crr.execute();
	}
}
