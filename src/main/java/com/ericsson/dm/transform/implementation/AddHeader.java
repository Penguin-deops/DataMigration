package com.ericsson.dm.transform.implementation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;

import com.ericsson.dm.inititialization.LoadSubscriberMapping;

import sun.awt.image.PixelConverter.Bgrx;

public class AddHeader {
	private String OutputFilePath;
	private String SnapShotPrePath;
	private int maxCount;
	public int counter;
	public int Offercounter;
	public Set<String> tempOfferDelete;
	public List<String> OfferDelete;
	public List<String> UCDelete;
	public List<String> CreditLimitUCDelete;
	public List<String> UTDelete;
	public List<String> DADelete;
	public List<String> OfferExclude = new ArrayList<String>(Arrays.asList("3504609","3504709","3504809"));
	Map<String, Map<String, BigDecimal>> UCSnapShotBalance; 
	public AddHeader()
	{
		
	}

	public AddHeader(String WorkingFilePath, String maxCount)
	{
		this.OutputFilePath = WorkingFilePath + "/Output/";
		this.SnapShotPrePath = WorkingFilePath + "/PreSnapshot/";
		this.UCSnapShotBalance = new HashMap<String, Map<String,BigDecimal>>();
		this.counter = 0;
		this.Offercounter = 0;
		this.maxCount = Integer.parseInt(maxCount);
		this.OfferDelete = new ArrayList<String>();
		this.UCDelete = new ArrayList<String>();
		this.CreditLimitUCDelete = new ArrayList<String>();
		this.UTDelete = new ArrayList<String>();
		this.DADelete = new ArrayList<String>();
		this.tempOfferDelete = new HashSet<String>();
		String ConfigPath = WorkingFilePath  + "/config/";
		String InputPath = WorkingFilePath  + "/Input/";
		LoadSubscriberMapping lsm = new LoadSubscriberMapping("Zain_Bahrain_SDP.json",WorkingFilePath, ConfigPath, InputPath);
		lsm.intializeCommonConfigMapping();
	}
	
	public void execute()
	{
		//for SUBSCRIBER_00001.DAT 
		AppendDedicatedAccountHeader("DedicatedAccount");
		AppendSubuscriberHeaderTail("SubscriberDAT");
		
		//for Offer.csv
		this.counter = 0;
		CreateOfferDelete("Offer.csv");
		//System.out.println("Size of OfferDelete after Offer: " + OfferDelete.size());
		CreateOfferDelete("SubscriberOffer.csv");
		//System.out.println("Size of OfferDelete after Offer, SubscriberOffer: " + OfferDelete.size());
		CreateOfferDelete("ProviderOffer.csv");
		OfferDelete.addAll(tempOfferDelete);
		//System.out.println("Size of OfferDelete after Offer, SubscriberOffer and ProviderOffer:" + OfferDelete.size());
		AppendHeaderOfferDelete();
		
		//Create UCDelete
		CreateUCDelete("UsageCounter.csv");		
		
		//Create CreditLimit UC delete
		CreateCreditUCDelete("UsageCounter.csv");	
		//Create UTDelete
		CreateUTDelete("UsageThreshold.csv");
		
		//Create DADelete();
		this.counter = 0;
		CreateDADelete("DedicatedAccount");
		AppendHeaderDADelete();
		
		this.counter = 0;
		AppendHeaderServiceClass("ServiceClass");
		
		//populate the SC
		
	}
	
	private void CreateDADelete(String fileName) {
		// TODO Auto-generated method stub
		/*File dir = new File(".");
		FileFilter fileFilter = new WildcardFileFilter("sample*.java");
		File[] files = dir.listFiles(fileFilter);
		for (int i = 0; i < files.length; i++) {
		   System.out.println(files[i]);
		}*/
		
		String FileName = this.OutputFilePath + "/Sorted/" + fileName;
		File File = new File(FileName);
		
		List<String> lines;
		try 
		{
			lines = FileUtils.readLines(File).stream().collect(Collectors.toList());
			for(String str: lines)
			{
				//DA_INSTALL,10000157,1,1009,1800,,,0,,,,
				DADelete.add("DA_DELETE," + str.split(",")[1] + "," + str.split(",")[2] + "," + str.split(",")[3]);
			}
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

	private void CreateUCDelete(String fileName) {
		// TODO Auto-generated method stub
		String FileName = this.OutputFilePath + "/Sorted/" + fileName;
		File File = new File(FileName);
		
		List<String> lines;
		long Transaction_ID = 1000000000;
		try 
		{
			lines = FileUtils.readLines(File).stream().collect(Collectors.toList());
			for(String str: lines)
			{
				//36902033,10182,20734,0,-4160,0,0,NULL,0
				if(!OfferExclude.contains(str.split(",")[1]))
				{
					UCDelete.add(Transaction_ID +"," + str.split(",")[0] + "," + str.split(",")[1]);
					Transaction_ID++;
				}
			}
		}
		catch (Exception e) {
			// TODO: handle exceptione.printStackTrace();
			e.printStackTrace();
		}
		
		//create file
		try 
		{
			File outputFile = new File(this.OutputFilePath + "/DeleteDat/" +  "UC_Delete.csv");
			FileUtils.writeLines(outputFile, UCDelete);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		//create file
		/*try (BufferedWriter bw = new BufferedWriter(new FileWriter(this.OutputFilePath + "/Sorted/" +  "UC_Delete.csv"))){
			for(String str : UCDelete)
			{
				bw.append(str );
				bw.append(System.lineSeparator());
			}			
			
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}*/
		
	}

	private void CreateCreditUCDelete(String fileName) {
		// TODO Auto-generated method stub
		
		GetpreSnapshot();
		
		List<String> lines;
		long Transaction_ID = 100000000;
		
		String FileName_Phase2 = this.OutputFilePath + "/Sorted/Credit_Limit_UC.csv";
		File File_phase2 = new File(FileName_Phase2);
		
		List<String> lines_phase2;
		try 
		{
			lines_phase2 = FileUtils.readLines(File_phase2).stream().collect(Collectors.toList());
			for(String str: lines_phase2)
			{
				//100000001,66305554,1000001,14123
				if(!OfferExclude.contains(str.split(",")[1]))
				{
					if(UCSnapShotBalance.containsKey(str.split(",")[1]))
					{
						HashMap<String, BigDecimal> temp = new HashMap<String, BigDecimal>();
						temp.putAll(UCSnapShotBalance.get(str.split(",")[1]));
						if(temp.containsKey(str.split(",")[2]))
						{
							NumberFormat formatter = new DecimalFormat("#0");
							String Balance =  formatter.format(Double.parseDouble(LoadSubscriberMapping.CommonConfigMapping.get("Monetory_Factor")) * 
									Double.parseDouble(String.valueOf(temp.get(str.split(",")[2]))));
							CreditLimitUCDelete.add(Transaction_ID +"," + str.split(",")[1] + "," + str.split(",")[2] + "," + Balance);
						}
						else
						{
							CreditLimitUCDelete.add(Transaction_ID +"," + str.split(",")[1] + "," + str.split(",")[2] + ",0" );
							
						}
						Transaction_ID++;
					}
					else
					{
						CreditLimitUCDelete.add(Transaction_ID +"," + str.split(",")[1] + "," + str.split(",")[2] + ",0");
						Transaction_ID++;
					}
				}
			}
		}
		catch (Exception e) {
			// TODO: handle exceptione.printStackTrace();
			e.printStackTrace();
		}
		
		//create file
		try 
		{
			File outputFile = new File(this.OutputFilePath + "/DeleteDat/" +  "Credit_Limit_UC_Delete.csv");
			FileUtils.writeLines(outputFile, CreditLimitUCDelete);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		//create file
		/*try (BufferedWriter bw = new BufferedWriter(new FileWriter(this.OutputFilePath + "/Sorted/" +  "UC_Delete.csv"))){
			for(String str : UCDelete)
			{
				bw.append(str );
				bw.append(System.lineSeparator());
			}			
			
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}*/
		
	}

	private void GetpreSnapshot() {
		// TODO Auto-generated method stub
	   File dir = new File(this.SnapShotPrePath);
	   FileFilter fileFilter = new WildcardFileFilter("*.v4.csv");
	   File[] files = dir.listFiles(fileFilter);
	   for (int i = 0; i < files.length; i++) {
		   //System.out.println(files[i]);
		   
		   if(files[i].getName().endsWith("DUMP_usage_counter.v4.csv"))
		   {
			   try (BufferedReader br = new BufferedReader(new FileReader(files[i].toString()));){
			 	   String line;
				    while ((line = br.readLine()) != null) {
				    	String datas[] = line.split(",",-1);
				    	
				    	if(UCSnapShotBalance.containsKey(datas[0]))
						{
							HashMap<String, BigDecimal> temp = new HashMap<String, BigDecimal>();
							temp.putAll(UCSnapShotBalance.get(datas[0]));
							temp.put(datas[1],new BigDecimal(datas[3]));
							UCSnapShotBalance.put(datas[0], temp);
						}
						else
						{
							HashMap<String, BigDecimal> temp = new HashMap<String, BigDecimal>();
							temp.put(datas[1],new BigDecimal(datas[3]));
							UCSnapShotBalance.put(datas[0], temp);
						}
				    }	
				    br.close();			  
				}
				catch (Exception e) {
					// TODO: handle exception
					e.printStackTrace();
				} 
		   }			   
	   }	
    }
	
	public void CalculateSnapShortReport(String path, String outputFileName, String FileName)
    {
	   
	   
   }

	
	private void CreateUTDelete(String fileName) {
	// TODO Auto-generated method stub
		String FileName = this.OutputFilePath + "/Sorted/" + fileName;
		File File = new File(FileName);
		
		List<String> lines;
		try 
		{
			lines = FileUtils.readLines(File).stream().collect(Collectors.toList());
			long Transaction_ID = 1000000000;
			for(String str: lines)
			{
				//36902033,10182,20734,0,-4160,0,0,NULL,0
				if(!OfferExclude.contains(str.split(",")[1]))
				{
					UTDelete.add(Transaction_ID + "," + str.split(",")[0] + "," + str.split(",")[1]);
					Transaction_ID ++;
				}
			}
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		try 
		{
			File outputFile = new File(this.OutputFilePath + "/DeleteDat/" +  "UT_Delete.csv");
			FileUtils.writeLines(outputFile, UTDelete);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	
	private void CreateOfferDelete(String fileName) {
		// TODO Auto-generated method stub
		String FileName = this.OutputFilePath + "/Sorted/" + fileName;
		File File = new File(FileName);
		Set<String> temp = new HashSet<String>();
		List<String> lines;
		try 
		{
			lines = FileUtils.readLines(File).stream().collect(Collectors.toList());
			//System.out.println(fileName + " : "  + lines.size());
			for(String str: lines)
			{
				//36902033,10182,20734,0,-4160,0,0,NULL,0
				temp.add("OFFER_DELETE," + str.split(",")[0] + "," + str.split(",")[1]);
			}
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		tempOfferDelete.addAll(temp);
	}

	public void AppendHeaderDADelete()
	{
		String OutputPath = this.OutputFilePath + "/DeleteDat/";
		String header = "";
		LocalDate today = LocalDate.now();
		//YYYYMMDD
		String formattedDate =  today.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")).replace("-", "");
		
		try 
		{
			int totalInputSize = DADelete.size();
			for(int i = 0; i < DADelete.size() ; i = i + this.maxCount)
			{
				String baseFileName = "";
				int startIndex = i;
				int endIndex = 0;
				
				if(this.counter==0)
				{
					baseFileName = "SUBSCRPT_DATA_" + formattedDate+ "_" + String.valueOf(40000 + this.counter) + ".DAT";
					if(DADelete.size() > this.maxCount)
					{
						endIndex = this.maxCount;
						//LoadSubscriberMapping.CommonConfigMapping.get("BatchVersion") +
						header =  "53," + 
							String.valueOf(endIndex - startIndex) + ",0,1";
					}
					else
					{
						endIndex = DADelete.size();
						//LoadSubscriberMapping.CommonConfigMapping.get("BatchVersion") +
						//System.out.println(LoadSubscriberMapping.CommonConfigMapping.get("BatchVersion"));
						header =  LoadSubscriberMapping.CommonConfigMapping.get("BatchVersion") + "," + 
							String.valueOf(DADelete.size() - startIndex) + ",0,1";
					}
				}
				else
				{
					baseFileName = "SUBSCRPT_DATA_" + formattedDate+ "_" + String.valueOf(40000 + this.counter) + ".DAT";
					if(totalInputSize > this.maxCount)
					{
						startIndex = startIndex + 1;
						endIndex = startIndex + this.maxCount;
					}
					else
					{
						startIndex = startIndex;
						endIndex = DADelete.size();
					}
					//LoadSubscriberMapping.CommonConfigMapping.get("BatchVersion")
					header =  LoadSubscriberMapping.CommonConfigMapping.get("BatchVersion") + "," + 
							String.valueOf(endIndex - startIndex) + ",0,1";
				}				
				
				//System.out.println(startIndex + "----" + endIndex);
				
				List<String> Batchlines = new ArrayList<String>(DADelete.subList(startIndex, endIndex));
				Batchlines.add(0, header);
				
				String newFilePath = OutputPath + baseFileName;
				File outputFile = new File(newFilePath);
				FileUtils.writeLines(outputFile, Batchlines);
				this.counter++;
				totalInputSize = totalInputSize - this.maxCount;
			}
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void AppendHeaderOfferDelete()
	{
		String OutputPath = this.OutputFilePath + "/DeleteDat/";
		String header = "";
			
		List<String> lines;
		LocalDate today = LocalDate.now();
		//YYYYMMDD
		String formattedDate =  today.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")).replace("-", "");
		
		try 
		{
			int totalInputSize = OfferDelete.size();
			for(int i = 0; i < OfferDelete.size() ; i = i + this.maxCount)
			{
				String baseFileName = "";
				int startIndex = i;
				int endIndex = 0;
				if(this.Offercounter==0)
				{
					baseFileName = "SUBSCRPT_DATA_" + formattedDate+ "_" + String.valueOf(10000 + this.Offercounter) + ".DAT";
					if(OfferDelete.size() > this.maxCount)
					{
						endIndex = this.maxCount;
						//LoadSubscriberMapping.CommonConfigMapping.get("BatchVersion") +
						header =  "53," + 
							String.valueOf(endIndex - startIndex) + ",,";
					}
					else
					{
						endIndex = OfferDelete.size();
						//LoadSubscriberMapping.CommonConfigMapping.get("BatchVersion") +
						//System.out.println(LoadSubscriberMapping.CommonConfigMapping.get("BatchVersion"));
						header =  LoadSubscriberMapping.CommonConfigMapping.get("BatchVersion") + "," + 
							String.valueOf(OfferDelete.size() - startIndex) + ",,";
					}
				}
				else
				{
					baseFileName = "SUBSCRPT_DATA_" + formattedDate+ "_" + String.valueOf(10000 + this.Offercounter) + ".DAT";
					if(totalInputSize > this.maxCount)
					{
						startIndex = startIndex;
						endIndex = startIndex + this.maxCount;
					}
					else
					{
						startIndex = startIndex;
						endIndex = OfferDelete.size();
					}
					//LoadSubscriberMapping.CommonConfigMapping.get("BatchVersion")
					header =  LoadSubscriberMapping.CommonConfigMapping.get("BatchVersion") + "," + 
							String.valueOf(endIndex - startIndex) + ",,";
				}				
				
				//System.out.println(startIndex + "----" + endIndex);
				
				List<String> Batchlines = new ArrayList<String>(OfferDelete.subList(startIndex, endIndex));
				Batchlines.add(0, header);
				
				String newFilePath = OutputPath + baseFileName;
				File outputFile = new File(newFilePath);
				FileUtils.writeLines(outputFile, Batchlines);
				this.Offercounter++;
				totalInputSize = totalInputSize - this.maxCount;
			}
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
		
	public void AppendSubuscriberHeaderTail(String fileName)
	{
		String FileName = this.OutputFilePath + fileName;
		String OutputPath = this.OutputFilePath + "/Sorted/";
		String header = "0,127.0.0.1,127.0.0.2";
		String tail = "100";
		File File = new File(FileName);
		List<String> lines;
		
		try 
		{
			String baseFileName = "SUBSCRIBER_00001.DAT";
			String newFilePath = OutputPath + baseFileName;
			File outputFile = new File(newFilePath);
			
			lines = FileUtils.readLines(File).stream().collect(Collectors.toList());
			lines.add(0, header);
			lines.add(lines.size(), tail + "," + lines.size());
			FileUtils.writeLines(outputFile, lines);
		}
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	public void AppendDedicatedAccountHeader(String fileName)
	{
		String FileName = this.OutputFilePath + fileName;
		String OutputPath = this.OutputFilePath + "/Sorted/";
		String header = "";
		File File = new File(FileName);
		
		List<String> lines;
		LocalDate today = LocalDate.now();
		//YYYYMMDD
		String formattedDate =  today.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")).replace("-", "");
		
		try 
		{
			lines = FileUtils.readLines(File).stream().collect(Collectors.toList());
			int totalInputSize = lines.size();
			for(int i = 0; i < lines.size() ; i = i + this.maxCount)
			{
				String baseFileName = "";
				int startIndex = i;
				int endIndex = 0;
				
				if(this.counter==0)
				{
					baseFileName = "SUBSCRPT_DATA_" + formattedDate+ "_" + String.valueOf(20000 + this.counter) + ".DAT";
					if(lines.size() > this.maxCount)
					{
						endIndex = this.maxCount;
						//LoadSubscriberMapping.CommonConfigMapping.get("BatchVersion") +
						header =  "53," + 
							String.valueOf(endIndex - startIndex) + "," + "0,1";
					}
					else
					{
						endIndex = lines.size();
						//LoadSubscriberMapping.CommonConfigMapping.get("BatchVersion") +
						//System.out.println(LoadSubscriberMapping.CommonConfigMapping.get("BatchVersion"));
						header =  LoadSubscriberMapping.CommonConfigMapping.get("BatchVersion") + "," + 
							String.valueOf(lines.size() - startIndex) + "," + "0,1";
					}
				}
				else
				{
					baseFileName = "SUBSCRPT_DATA_" + formattedDate+ "_" + String.valueOf(20000 + this.counter) + ".DAT";
					if(totalInputSize > this.maxCount)
					{
						startIndex = startIndex;
						endIndex = startIndex + this.maxCount;
					}
					else
					{
						startIndex = startIndex;
						endIndex = lines.size();
					}
					//LoadSubscriberMapping.CommonConfigMapping.get("BatchVersion")
					header =  LoadSubscriberMapping.CommonConfigMapping.get("BatchVersion") + "," + 
							String.valueOf(endIndex - startIndex) + "," + "0,1";
				}				
				
				//System.out.println(startIndex + "----" + endIndex);
				
				List<String> Batchlines = new ArrayList<String>(lines.subList(startIndex, endIndex));
				Batchlines.add(0, header);
				
				String newFilePath = OutputPath + baseFileName;
				File outputFile = new File(newFilePath);
				FileUtils.writeLines(outputFile, Batchlines);
				this.counter++;
				totalInputSize = totalInputSize - this.maxCount;
			}
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void AppendHeaderServiceClass(String fileName)
	{
		String FileName = this.OutputFilePath + fileName;
		String OutputPath = this.OutputFilePath + "/Sorted/";
		String header = "";
		File File = new File(FileName);
		
		List<String> lines;
		LocalDate today = LocalDate.now();
		//YYYYMMDD
		String formattedDate =  today.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")).replace("-", "");
		
		try 
		{
			lines = FileUtils.readLines(File).stream().collect(Collectors.toList());
			int totalInputSize = lines.size();
			for(int i = 0; i < lines.size() ; i = i + this.maxCount)
			{
				String baseFileName = "";
				int startIndex = i;
				int endIndex = 0;
				
				if(this.counter==0)
				{
					baseFileName = "SUBSCRPT_DATA_" + formattedDate+ "_" + String.valueOf(30000 + this.counter) + ".DAT";
					if(lines.size() > this.maxCount)
					{
						endIndex = this.maxCount;
						//LoadSubscriberMapping.CommonConfigMapping.get("BatchVersion") +
						header =  "53," + 
							String.valueOf(endIndex - startIndex) + ",,";
					}
					else
					{
						endIndex = lines.size();
						//LoadSubscriberMapping.CommonConfigMapping.get("BatchVersion") +
						//System.out.println(LoadSubscriberMapping.CommonConfigMapping.get("BatchVersion"));
						header =  LoadSubscriberMapping.CommonConfigMapping.get("BatchVersion") + "," + 
							String.valueOf(lines.size() - startIndex) + ",,";
					}
				}
				else
				{
					baseFileName = "SUBSCRPT_DATA_" + formattedDate+ "_" + String.valueOf(30000 + this.counter) + ".DAT";
					if(totalInputSize > this.maxCount)
					{
						startIndex = startIndex;
						endIndex = startIndex + this.maxCount;
					}
					else
					{
						startIndex = startIndex;
						endIndex = lines.size();
					}
					//LoadSubscriberMapping.CommonConfigMapping.get("BatchVersion")
					header =  LoadSubscriberMapping.CommonConfigMapping.get("BatchVersion") + "," + 
							String.valueOf(endIndex - startIndex) + ",,";
				}				
				
				//System.out.println(startIndex + "----" + endIndex);
				
				List<String> Batchlines = new ArrayList<String>(lines.subList(startIndex, endIndex));
				Batchlines.add(0, header);
				
				String newFilePath = OutputPath + baseFileName;
				File outputFile = new File(newFilePath);
				FileUtils.writeLines(outputFile, Batchlines);
				this.counter++;
				totalInputSize = totalInputSize - this.maxCount;
			}
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		String dataFolderPath = "C:\\Ericsson\\MyWorkingProject\\Charging_System\\dm_cs_2020_zain_bahrain\\dev\\src\\";
		String OutputPath = "C:\\Ericsson\\MyWorkingProject\\Charging_System\\dm_cs_2020_zain_bahrain\\dev\\src\\output\\Sorted\\";
		AddHeader da = new AddHeader(dataFolderPath, "1000000");
		da.execute();
	}
	
}
