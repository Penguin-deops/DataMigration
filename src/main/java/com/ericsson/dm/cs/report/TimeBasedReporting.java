package com.ericsson.dm.cs.report;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.FileWriter;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.commons.io.filefilter.WildcardFileFilter;

import com.ericsson.dm.Utils.CommonUtilities;
import com.ericsson.dm.inititialization.LoadSubscriberMapping;
import com.ericsson.dm.transformation.JsonInputFields.ADDON_OFFER;

public class TimeBasedReporting {

	private String OfferID;
	private String StartDate;
	private String ExpiryDate;
	private String OfferSequence;
	private String migToolPath = "";
	private String inputDirectoryPath = "";
	private String SnapShotPostPath = "";
	private String SortedOutputPath = "";
	private String reportDirectoryPath = "";
	
	private Map<String, List<TimeBasedReporting>> SourceOffer = new HashMap<String, List<TimeBasedReporting>>();
	private Map<String, List<TimeBasedReporting>> TargetOffer = new HashMap<String, List<TimeBasedReporting>>();
	Map<String,Set<String>> OfferMSISDN;
	Map<String,Set<String>> SubOfferMSISDN;
	Set<String> AddonOfferMSISDN;
	
	public TimeBasedReporting(String OfferID,String StartDate,String ExpiryDate, String OfferSequence) {
		// TODO Auto-generated constructor stub
		this.OfferID = OfferID;
		this.StartDate = StartDate;
		this.ExpiryDate = ExpiryDate;	
		this.OfferSequence = OfferSequence;
	}
	
	public TimeBasedReporting(String MigtoolPath) {
		// TODO Auto-generated constructor stub
		this.migToolPath = MigtoolPath;
		this.inputDirectoryPath = MigtoolPath + "/Input/";
		this.SnapShotPostPath = MigtoolPath + "/PostSnapshot/";
		this.SortedOutputPath = MigtoolPath + "/Output/Sorted/";
		this.reportDirectoryPath = MigtoolPath + "/Reports/";
		this.AddonOfferMSISDN = new HashSet<String>();
		
		this.OfferMSISDN = new HashMap<String,Set<String>>();
		this.SubOfferMSISDN = new HashMap<String,Set<String>>();
	}
	
	public void execute()
	{
		BuildMSISDNList();
		CalulateSourceOffer();
		CalulateTargetOffer();
		//write into the file both source and target offer start and expiry
		populateReport();
		consolidateReport();
	}
	
	private void consolidateReport() {
		// TODO Auto-generated method stub
		
	}

	private void populateReport() {
		// TODO Auto-generated method stub
		Map<String,List<String>> DetailReport = new HashMap<String, List<String>>();
		Map<String, Long> MatchCount = new HashMap<String, Long>();
		Map<String, Long> UnMatchCount = new HashMap<String, Long>();
		for(Entry<String, List<TimeBasedReporting>> entry : SourceOffer.entrySet())
		{
			if(TargetOffer.containsKey(entry.getKey()))
			{
				List<TimeBasedReporting> tempSource = new ArrayList<TimeBasedReporting>(entry.getValue());
				List<TimeBasedReporting> tempTarget = new ArrayList<TimeBasedReporting>(TargetOffer.get(entry.getKey()));
				
				if(AddonOfferMSISDN.contains(entry.getKey()))
				{
					String SourceOfferSequence = "10001";
					for(TimeBasedReporting tbrSource: tempSource)
					{
						String SourceOfferID = tbrSource.OfferID.trim();
						String SourceStart = tbrSource.StartDate.trim();
						String SourceExpiry = tbrSource.ExpiryDate.trim();
						for(TimeBasedReporting tbrTarget: tempTarget)
						{
							String TargetOfferID = "";
							if(tbrTarget.OfferID.trim().equals("35046") || tbrTarget.OfferID.trim().equals("35047") || tbrTarget.OfferID.trim().equals("35048"))
								continue;
							
							if(tbrTarget.OfferID.trim().equals("3504609") || tbrTarget.OfferID.trim().equals("3504709") || tbrTarget.OfferID.trim().equals("3504809"))
							{
								TargetOfferID = tbrTarget.OfferID.trim().replace("09","");
								if(SourceOfferID.equals(TargetOfferID))
								{
									String TargetStart = tbrTarget.StartDate.trim();
									String TargetExpiry = tbrTarget.ExpiryDate.trim();
									String TargetOfferSequence = tbrTarget.OfferSequence.trim();
									if(SourceOfferSequence.equals(TargetOfferSequence))
									{
										if(SourceStart.equals(TargetStart) && SourceExpiry.equals(TargetExpiry))
										{
											//this is for match count
											if(MatchCount.containsKey(SourceOfferID))
											{
												long temp = MatchCount.get(SourceOfferID) + 1;
												MatchCount.put(SourceOfferID, temp);
											}
											else
											{
												MatchCount.put(SourceOfferID, 1L);
											}
											
											//this is for detail report
											if(DetailReport.containsKey(entry.getKey()))
											{
												List<String> tempOfferList = new ArrayList<String>(DetailReport.get(entry.getKey()));
												tempOfferList.add(entry.getKey() + "," + SourceOfferID + "," + SourceStart + "," +
														SourceExpiry + "," + TargetStart + "," + TargetExpiry + "," + 0);
												DetailReport.put(entry.getKey(), tempOfferList);
											}
											else
											{
												List<String> tempOfferList = new ArrayList<String>();
												tempOfferList.add(entry.getKey() + "," + SourceOfferID + "," + SourceStart + "," +
														SourceExpiry + "," + TargetStart + "," + TargetExpiry + "," + 0);
												DetailReport.put(entry.getKey(), tempOfferList);
											}
											SourceOfferSequence = String.valueOf(Integer.parseInt(SourceOfferSequence) +1);
											break;
										}
										else
										{
											//this is for unMatch count
											if(UnMatchCount.containsKey(SourceOfferID))
											{
												long temp = UnMatchCount.get(SourceOfferID) + 1;
												UnMatchCount.put(SourceOfferID, temp);
											}
											else
											{
												UnMatchCount.put(SourceOfferID, 1L);
											}
											//this is for detail report
											if(DetailReport.containsKey(entry.getKey()))
											{
												List<String> tempOfferList = new ArrayList<String>(DetailReport.get(entry.getKey()));
												tempOfferList.add(entry.getKey() + "," + SourceOfferID + "," + SourceStart + "," +
														SourceExpiry + "," + TargetStart + "," + TargetExpiry + "," + 1);
												DetailReport.put(entry.getKey(), tempOfferList);
											}
											else
											{
												List<String> tempOfferList = new ArrayList<String>();
												tempOfferList.add(entry.getKey() + "," + SourceOfferID + "," + SourceStart + "," +
														SourceExpiry + "," + TargetStart + "," + TargetExpiry + "," + 1);
												DetailReport.put(entry.getKey(), tempOfferList);
											}
											SourceOfferSequence = String.valueOf(Integer.parseInt(SourceOfferSequence) +1);
											break;
										}
									}								
								}			
							}
							else 
							{
								TargetOfferID = tbrTarget.OfferID.trim();
								if(SourceOfferID.equals(TargetOfferID))
								{
									String TargetStart = tbrTarget.StartDate.trim();
									String TargetExpiry = tbrTarget.ExpiryDate.trim();
									String TargetOfferSequence = tbrTarget.OfferSequence.trim();
									if(SourceStart.equals(TargetStart) && SourceExpiry.equals(TargetExpiry))
									{
										//this is for match count
										if(MatchCount.containsKey(SourceOfferID))
										{
											long temp = MatchCount.get(SourceOfferID) + 1;
											MatchCount.put(SourceOfferID, temp);
										}
										else
										{
											MatchCount.put(SourceOfferID, 1L);
										}
										
										//this is for detail report
										if(DetailReport.containsKey(entry.getKey()))
										{
											List<String> tempOfferList = new ArrayList<String>(DetailReport.get(entry.getKey()));
											tempOfferList.add(entry.getKey() + "," + SourceOfferID + "," + SourceStart + "," +
													SourceExpiry + "," + TargetStart + "," + TargetExpiry + "," + 0);
											DetailReport.put(entry.getKey(), tempOfferList);
										}
										else
										{
											List<String> tempOfferList = new ArrayList<String>();
											tempOfferList.add(entry.getKey() + "," + SourceOfferID + "," + SourceStart + "," +
													SourceExpiry + "," + TargetStart + "," + TargetExpiry + "," + 0);
											DetailReport.put(entry.getKey(), tempOfferList);
										}
										break;
									}
									else
									{
										//this is for unMatch count
										if(UnMatchCount.containsKey(SourceOfferID))
										{
											long temp = UnMatchCount.get(SourceOfferID) + 1;
											UnMatchCount.put(SourceOfferID, temp);
										}
										else
										{
											UnMatchCount.put(SourceOfferID, 1L);
										}
										//this is for detail report
										if(DetailReport.containsKey(entry.getKey()))
										{
											List<String> tempOfferList = new ArrayList<String>(DetailReport.get(entry.getKey()));
											tempOfferList.add(entry.getKey() + "," + SourceOfferID + "," + SourceStart + "," +
													SourceExpiry + "," + TargetStart + "," + TargetExpiry + "," + 1);
											DetailReport.put(entry.getKey(), tempOfferList);
										}
										else
										{
											List<String> tempOfferList = new ArrayList<String>();
											tempOfferList.add(entry.getKey() + "," + SourceOfferID + "," + SourceStart + "," +
													SourceExpiry + "," + TargetStart + "," + TargetExpiry + "," + 1);
											DetailReport.put(entry.getKey(), tempOfferList);
										}
										break;
									}								
								}			
							}			
						}
					}
				}
				else
				{
					for(TimeBasedReporting tbrSource: tempSource)
					{
						String SourceOfferID = tbrSource.OfferID.trim();
						String SourceStart = tbrSource.StartDate.trim();
						String SourceExpiry = tbrSource.ExpiryDate.trim();
						for(TimeBasedReporting tbrTarget: tempTarget)
						{
							String TargetOfferID = "";
							if(tbrTarget.OfferID.trim().equals("3504609") || tbrTarget.OfferID.trim().equals("3504709") || tbrTarget.OfferID.trim().equals("3504809"))
							{
								TargetOfferID = tbrTarget.OfferID.trim().replace("09","");
							}
							else
							{
								TargetOfferID = tbrTarget.OfferID.trim();
							}
							
							if(SourceOfferID.equals(TargetOfferID))
							{
								String TargetStart = tbrTarget.StartDate.trim();
								String TargetExpiry = tbrTarget.ExpiryDate.trim();
								if(SourceStart.equals(TargetStart) && SourceExpiry.equals(TargetExpiry))
								{
									//this is for match count
									if(MatchCount.containsKey(SourceOfferID))
									{
										long temp = MatchCount.get(SourceOfferID) + 1;
										MatchCount.put(SourceOfferID, temp);
									}
									else
									{
										MatchCount.put(SourceOfferID, 1L);
									}
									
									//this is for detail report
									if(DetailReport.containsKey(entry.getKey()))
									{
										List<String> tempOfferList = new ArrayList<String>(DetailReport.get(entry.getKey()));
										tempOfferList.add(entry.getKey() + "," + SourceOfferID + "," + SourceStart + "," +
												SourceExpiry + "," + TargetStart + "," + TargetExpiry + "," + 0);
										DetailReport.put(entry.getKey(), tempOfferList);
									}
									else
									{
										List<String> tempOfferList = new ArrayList<String>();
										tempOfferList.add(entry.getKey() + "," + SourceOfferID + "," + SourceStart + "," +
												SourceExpiry + "," + TargetStart + "," + TargetExpiry + "," + 0);
										DetailReport.put(entry.getKey(), tempOfferList);
									}
								}
								else
								{
									//this is for unMatch count
									if(UnMatchCount.containsKey(SourceOfferID))
									{
										long temp = UnMatchCount.get(SourceOfferID) + 1;
										UnMatchCount.put(SourceOfferID, temp);
									}
									else
									{
										UnMatchCount.put(SourceOfferID, 1L);
									}
									//this is for detail report
									if(DetailReport.containsKey(entry.getKey()))
									{
										List<String> tempOfferList = new ArrayList<String>(DetailReport.get(entry.getKey()));
										tempOfferList.add(entry.getKey() + "," + SourceOfferID + "," + SourceStart + "," +
												SourceExpiry + "," + TargetStart + "," + TargetExpiry + "," + 1);
										DetailReport.put(entry.getKey(), tempOfferList);
									}
									else
									{
										List<String> tempOfferList = new ArrayList<String>();
										tempOfferList.add(entry.getKey() + "," + SourceOfferID + "," + SourceStart + "," +
												SourceExpiry + "," + TargetStart + "," + TargetExpiry + "," + 1);
										DetailReport.put(entry.getKey(), tempOfferList);
									}
								}
							}						
						}
					}
				}								
			}
		}
		
		//Now write it into file
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "OfferDateTimeDetails.csv"))){
			//bw.append(UCSourceBalance + "," + UCSourceZeroBalance);
			for(Entry<String, List<String>> entry : DetailReport.entrySet())
			{
				List<String> tempValue = new ArrayList<String>(entry.getValue());
				for(String str : tempValue)
				{
					bw.append(str);
					bw.append(System.lineSeparator());
				}				
			}
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		Long TotalMatchCount = 0L;
		Long TotalUnMatchCount = 0L;
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "OfferDateTimeCount.csv"))){
			//bw.append(UCSourceBalance + "," + UCSourceZeroBalance);
			
			for(Entry<String, Long> entryMatch : MatchCount.entrySet())
			{
				long unValue = 0;
				if(UnMatchCount.containsKey((entryMatch.getKey())))
					unValue = UnMatchCount.get((entryMatch.getKey()));
				
				bw.append(entryMatch.getKey() + "," + entryMatch.getValue() +  "," + unValue);
				bw.append(System.lineSeparator());
				TotalMatchCount = TotalMatchCount + entryMatch.getValue();
				TotalUnMatchCount = TotalUnMatchCount + unValue;
			}
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "OfferDateTimeCountSummary.csv"))){
			//bw.append(UCSourceBalance + "," + UCSourceZeroBalance);
			
			bw.append("MatchCount" + "," + TotalMatchCount);
			bw.append(System.lineSeparator());
			bw.append("UnMatchCount" + "," + TotalUnMatchCount);
			bw.append(System.lineSeparator());
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

	public void CalulateSourceOffer()
	{
		generateSourceReport(inputDirectoryPath + "Main_Offer.csv");
		//generateSourceReport(inputDirectoryPath + "Addon_Offer.csv");
		generateSourceReportAddon(inputDirectoryPath + "Addon_Offer.csv");
		generateSourceReport(inputDirectoryPath + "Implicit_Shared_Offer.csv");
	}
	
	public void CalulateTargetOffer()
	{
		File dir = new File(this.SnapShotPostPath);
	    FileFilter fileFilter = new WildcardFileFilter("*.v4.csv");
	    File[] files = dir.listFiles(fileFilter);
	    for (int i = 0; i < files.length; i++) {
		   //System.out.println(files[i]);
		   if(files[i].getName().endsWith("DUMP_offer.v4.csv"))
		   {
			   generateSnapShortReport(files[i].toString(), "postSnapShotOfferCount.csv","OFFER");
		   }
		   else if(files[i].getName().endsWith("DUMP_subscriber_offer.v4.csv"))
		   {
			   generateSnapShortReport(files[i].toString(), "postSnapShotSubOfferCount.csv","SUBSOFFER");
		   }		
	   }
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
	}
	
	public void generateSnapShortReport(String path, String outputFileName, String FileName)
    {
	   //Map<String, Long> KPISnapShot = new HashMap<String, Long>();
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
		    				String mMSISDN = datas[0];
		    				if(TargetOffer.containsKey(mMSISDN))
		    				{
		    					List<TimeBasedReporting> temp = new ArrayList<TimeBasedReporting>();
		    					temp.addAll(TargetOffer.get(mMSISDN));
		    					TimeBasedReporting tbr = new TimeBasedReporting(line.split(",",-1)[1], line.split(",",-1)[2] + " " + line.split(",",-1)[4].split("\\+",-1)[0] , line.split(",",-1)[3] + " " + line.split(",",-1)[5].split("\\+",-1)[0], line.split(",",-1)[8]);
		    					temp.add(tbr);
		    					TargetOffer.put(mMSISDN, temp);
		    				}
		    				else
		    				{
		    					List<TimeBasedReporting> temp = new ArrayList<TimeBasedReporting>();
		    					TimeBasedReporting tbr = new TimeBasedReporting(line.split(",",-1)[1], line.split(",",-1)[2] + " " + line.split(",",-1)[4].split("\\+",-1)[0] , line.split(",",-1)[3] + " " + line.split(",",-1)[5].split("\\+",-1)[0], line.split(",",-1)[8]);
		    					temp.add(tbr);
		    					TargetOffer.put(mMSISDN, temp);
		    				}		    				
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
		    				String mMSISDN = datas[0];
		    				if(TargetOffer.containsKey(mMSISDN))
		    				{
		    					List<TimeBasedReporting> temp = new ArrayList<TimeBasedReporting>();
		    					temp.addAll(TargetOffer.get(mMSISDN));
		    					TimeBasedReporting tbr = new TimeBasedReporting(line.split(",",-1)[1], line.split(",",-1)[2] + " " + line.split(",",-1)[8].split("\\+",-1)[0] , line.split(",",-1)[3] + " " + line.split(",",-1)[9].split("\\+",-1)[0], line.split(",",-1)[8]);
		    					temp.add(tbr);
		    					TargetOffer.put(mMSISDN, temp);
		    				}
		    				else
		    				{
		    					List<TimeBasedReporting> temp = new ArrayList<TimeBasedReporting>();
		    					TimeBasedReporting tbr = new TimeBasedReporting(line.split(",",-1)[1], line.split(",",-1)[2] + " " + line.split(",",-1)[8].split("\\+",-1)[0] , line.split(",",-1)[3] + " " + line.split(",",-1)[9].split("\\+",-1)[0], line.split(",",-1)[8]);
		    					temp.add(tbr);
		    					TargetOffer.put(mMSISDN, temp);
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
   }
		
	public void generateSourceReport(String path)
    {
	   //Map<String, Long> KPISnapShot = new HashMap<String, Long>();
		try 
		{
		  BufferedReader br = new BufferedReader(new FileReader(path));
		  String line = "";
		  br.readLine();
		  while ((line = br.readLine()) != null) {
			String mMSISDN = line.split(",",-1)[0];
			
			if(SourceOffer.containsKey(mMSISDN))
			{
				List<TimeBasedReporting> temp = new ArrayList<TimeBasedReporting>();
				temp.addAll(SourceOffer.get(mMSISDN));
				String OfferStart = line.split(",",-1)[2];
				String OfferExpiry = line.split(",",-1)[3];
				if(OfferStart.length() == 10)
					OfferStart = line.split(",",-1)[2] + " 00:00:00";
				
				if(OfferExpiry.length() == 10)
					OfferExpiry = line.split(",",-1)[2] + " 00:00:00";
				
				TimeBasedReporting tbr = new TimeBasedReporting(line.split(",",-1)[1], OfferStart, OfferExpiry,"0");
				temp.add(tbr);
				SourceOffer.put(mMSISDN, temp);
			}
			else
			{
				List<TimeBasedReporting> temp = new ArrayList<TimeBasedReporting>();
				String OfferStart = line.split(",",-1)[2];
				String OfferExpiry = line.split(",",-1)[3];
				if(OfferStart.length() == 10)
					OfferStart = line.split(",",-1)[2] + " 00:00:00";
				
				if(OfferExpiry.length() == 10)
					OfferExpiry = line.split(",",-1)[2] + " 00:00:00";
				
				TimeBasedReporting tbr = new TimeBasedReporting(line.split(",",-1)[1], OfferStart, OfferExpiry,"0");
				temp.add(tbr);
				SourceOffer.put(mMSISDN, temp);
			}			
		  }
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}	    
   }
	public void generateSourceReportAddon(String path)
    {
	   //Map<String, Long> KPISnapShot = new HashMap<String, Long>();
		try 
		{
		  BufferedReader br = new BufferedReader(new FileReader(path));
		  String line = "";
		  br.readLine();
		  while ((line = br.readLine()) != null) {
			String mMSISDN = line.split(",",-1)[0];
			AddonOfferMSISDN.add(mMSISDN);
			if(SourceOffer.containsKey(mMSISDN))
			{
				List<TimeBasedReporting> temp = new ArrayList<TimeBasedReporting>();
				temp.addAll(SourceOffer.get(mMSISDN));
				String OfferStart = line.split(",",-1)[2];
				String OfferExpiry = line.split(",",-1)[3];
				String Offer_Sequence =  line.split(",",-1)[6];
				if(OfferStart.length() == 10)
					OfferStart = line.split(",",-1)[2] + " 00:00:00";
				
				if(OfferExpiry.length() == 10)
					OfferExpiry = line.split(",",-1)[2] + " 00:00:00";
				
				TimeBasedReporting tbr = new TimeBasedReporting(line.split(",",-1)[1], OfferStart, OfferExpiry,Offer_Sequence);
				temp.add(tbr);
				SourceOffer.put(mMSISDN, temp);
			}
			else
			{
				List<TimeBasedReporting> temp = new ArrayList<TimeBasedReporting>();
				String OfferStart = line.split(",",-1)[2];
				String OfferExpiry = line.split(",",-1)[3];
				String Offer_Sequence =  line.split(",",-1)[6];
				if(OfferStart.length() == 10)
					OfferStart = line.split(",",-1)[2] + " 00:00:00";
				
				if(OfferExpiry.length() == 10)
					OfferExpiry = line.split(",",-1)[2] + " 00:00:00";
				
				TimeBasedReporting tbr = new TimeBasedReporting(line.split(",",-1)[1], OfferStart, OfferExpiry,Offer_Sequence);
				temp.add(tbr);
				SourceOffer.put(mMSISDN, temp);
			}			
		  }
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}	    
   }
	
	public void generateSourceReportAddon_old(String path)
    {
		Map<String, HashMap<String, TreeSet<String>>> OfferStartDate = new HashMap<String,HashMap<String, TreeSet<String>>>();
		Map<String, HashMap<String, TreeSet<String>>> OfferExpiryDate = new HashMap<String,HashMap<String, TreeSet<String>>>();
		 
		try 
		{
		  BufferedReader br = new BufferedReader(new FileReader(path));
		  
		  String line = "";
		  br.readLine();
		  while ((line = br.readLine()) != null) {
			String mMSISDN = line.split(",",-1)[0];
			String OfferID = line.split(",",-1)[1];
			String Start_Date_Time = line.split(",",-1)[2];
			String End_Date_Time = line.split(",",-1)[3];
			
			if(OfferStartDate.containsKey(mMSISDN))
			{
				HashMap<String, TreeSet<String>> addonValue = new HashMap<String, TreeSet<String>>(OfferStartDate.get(mMSISDN));
				TreeSet<String> startEpoch = new TreeSet<String>();
				if(OfferStartDate.get(mMSISDN).containsKey(OfferID))
				{
					startEpoch.addAll(addonValue.get(OfferID));
					startEpoch.add(Start_Date_Time);
					addonValue.put(OfferID, startEpoch);
				}
				else
				{
					startEpoch.add(Start_Date_Time);
					addonValue.put(OfferID, startEpoch);
				}
				OfferStartDate.put(mMSISDN, addonValue);
			}
			else
			{
				HashMap<String, TreeSet<String>> addonValue = new HashMap<String, TreeSet<String>>();
				TreeSet<String> startEpoch = new TreeSet<String>();
				startEpoch.add(Start_Date_Time);
				addonValue.put(OfferID, startEpoch);				
				OfferStartDate.put(mMSISDN, addonValue);
			}
			
			if(OfferExpiryDate.containsKey(mMSISDN) )
			{
				HashMap<String, TreeSet<String>> addonValue = new HashMap<String, TreeSet<String>>(OfferExpiryDate.get(mMSISDN));
				TreeSet<String> expiryEpoch = new TreeSet<String>();
				if(OfferExpiryDate.get(mMSISDN).containsKey(OfferID))
				{
					expiryEpoch.addAll(addonValue.get(OfferID));
					expiryEpoch.add(End_Date_Time);
					addonValue.put(OfferID, expiryEpoch);
				}
				else
				{
					expiryEpoch.add(End_Date_Time);
					addonValue.put(OfferID, expiryEpoch);
				}				
				OfferExpiryDate.put(mMSISDN, addonValue);				
			}
			else
			{
				HashMap<String, TreeSet<String>> addonValue = new HashMap<String, TreeSet<String>>();
				TreeSet<String> expiryEpoch = new TreeSet<String>();
				expiryEpoch.add(End_Date_Time);
				addonValue.put(OfferID, expiryEpoch);
				OfferExpiryDate.put(mMSISDN, addonValue);
			}				
		  }
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		
	    //Map<String, Long> KPISnapShot = new HashMap<String, Long>();
		try 
		{
		  BufferedReader br = new BufferedReader(new FileReader(path));
		  String line = "";
		  br.readLine();
		  while ((line = br.readLine()) != null) {
			String mMSISDN = line.split(",",-1)[0];
			String sOfferID = line.split(",",-1)[1];
			
			if(SourceOffer.containsKey(mMSISDN))
			{
				List<TimeBasedReporting> temp = new ArrayList<TimeBasedReporting>();
				temp.addAll(SourceOffer.get(mMSISDN));
				//get the start and expiry from map above
				HashMap<String, TreeSet<String>> addonStartValue = new HashMap<String, TreeSet<String>>(OfferStartDate.get(mMSISDN));
				TreeSet<String> tempStartSet = new TreeSet<String>(addonStartValue.get(sOfferID));
				String minOfferStart = (tempStartSet.first());
				HashMap<String, TreeSet<String>> addonExpiryValue = new HashMap<String, TreeSet<String>>(OfferExpiryDate.get(mMSISDN));
				TreeSet<String> tempExpirySet = new TreeSet<String>(addonExpiryValue.get(sOfferID));
				String MaxOfferExpiry = (tempExpirySet.last());
				
				TimeBasedReporting tbr = new TimeBasedReporting(line.split(",",-1)[1], minOfferStart, MaxOfferExpiry,"");
				temp.add(tbr);
				SourceOffer.put(mMSISDN, temp);
			}
			else
			{
				List<TimeBasedReporting> temp = new ArrayList<TimeBasedReporting>();
				HashMap<String, TreeSet<String>> addonStartValue = new HashMap<String, TreeSet<String>>(OfferStartDate.get(mMSISDN));
				TreeSet<String> tempStartSet = new TreeSet<String>(addonStartValue.get(sOfferID));
				String minOfferStart = (tempStartSet.first());
				HashMap<String, TreeSet<String>> addonExpiryValue = new HashMap<String, TreeSet<String>>(OfferExpiryDate.get(mMSISDN));
				TreeSet<String> tempExpirySet = new TreeSet<String>(addonExpiryValue.get(sOfferID));
				String MaxOfferExpiry = (tempExpirySet.last());
				
				TimeBasedReporting tbr = new TimeBasedReporting(line.split(",",-1)[1], minOfferStart, MaxOfferExpiry,"");
				temp.add(tbr);
				SourceOffer.put(mMSISDN, temp);
			}			
		  }
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		String MigtoolPath = "C:\\Ericsson\\MyWorkingProject\\Charging_System\\dm_cs_2020_zain_bahrain\\dev\\src\\";
		TimeBasedReporting tbf = new TimeBasedReporting(MigtoolPath);
		tbf.execute();
		System.out.println("VipinSingh");
	}
}
