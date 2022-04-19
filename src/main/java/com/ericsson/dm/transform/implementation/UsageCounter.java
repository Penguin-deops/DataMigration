package com.ericsson.dm.transform.implementation;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import com.ericsson.dm.Utils.CommonUtilities;
import com.ericsson.dm.inititialization.LoadSubscriberMapping;
import com.ericsson.dm.transformation.JsonInputFields;
import com.ericsson.dm.transformation.JsonInputFields.ADDON_OFFER;
import com.ericsson.dm.transformation.JsonInputFields.MAIN_OFFER;

public class UsageCounter{
	Set<String> rejectAndLog;
	Set<String> onlyLog;
	JsonInputFields inputObj;
	CommonFunctions commonfunction;	
	Map<String, String> ProductIDLookUpMap;
	Set<String> CompletedMSISDN;
	boolean isProductPrivate;
	public UsageCounter()
	{
		
	}
	
	public UsageCounter(Set<String> rejectAndLog, Set<String> onlyLog,JsonInputFields inputObj, Map<String, String> ProductIDLookUpMap,boolean isProductPrivate) {
		this.rejectAndLog=rejectAndLog;
		this.onlyLog=onlyLog;
		this.inputObj = inputObj;
		this.commonfunction = new CommonFunctions();
		this.ProductIDLookUpMap = ProductIDLookUpMap;
		this.isProductPrivate = isProductPrivate;
	}
	public UsageCounter(Set<String> rejectAndLog, Set<String> onlyLog,Set<String> CompletedMSISDN, Map<String, String> ProductIDLookUpMap) {
		this.rejectAndLog=rejectAndLog;
		this.onlyLog=onlyLog;
		this.CompletedMSISDN = CompletedMSISDN;
		this.commonfunction = new CommonFunctions();
		this.ProductIDLookUpMap = ProductIDLookUpMap;
	}
	
	public List<String> execute() {
		// TODO Auto-generated method stub
		List<String> UCList = new ArrayList<>();
		UCList.addAll(GenerateFromTimeUC());
		UCList.addAll(GenerateUCFromAddonFile());
		
		return UCList;
	}
	
	private Collection<? extends String> GenerateUCFromAddonFile() {
		// TODO Auto-generated method stub
		List<String> UCValue = new ArrayList<String>();
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date dateobj = new Date();
		
		int productID = Integer.parseInt(LoadSubscriberMapping.CommonConfigMapping.get("P_Counter"));
		TreeSet<String> sortedAddonKey = new TreeSet<String>(inputObj.ADDON_OFFER.keySet());
		
		Map<String, TreeSet<String>> MinOfferStartDate = new HashMap<String, TreeSet<String>>();
		Map<String, TreeSet<String>> MaxOfferExpiryDate = new HashMap<String, TreeSet<String>>();
		Map<String, Long> OfferStartDateMap = new HashMap<String, Long>();
		
		boolean mainOffer = false;
		for(String entryKey  : sortedAddonKey)
		{
			ADDON_OFFER mo = inputObj.new ADDON_OFFER();
			mo = (ADDON_OFFER)inputObj.ADDON_OFFER.get(entryKey);
			String OfferID = entryKey.split(";")[0];
		
			if(OfferStartDateMap.containsKey(OfferID + ";" + mo.Offer_Sequence))
			{
				OfferStartDateMap.put(OfferID + ";" + mo.Offer_Sequence, CommonUtilities.convertDateToEpochSeconds(mo.Start_Date_Time));
			}
			else
			{
				OfferStartDateMap.put(OfferID + ";" + mo.Offer_Sequence, CommonUtilities.convertDateToEpochSeconds(mo.Start_Date_Time));
			}
			
			if(MinOfferStartDate.containsKey(inputObj.MSISDN))
			{
				TreeSet<String> startEpoch = new TreeSet<String>();
				startEpoch.addAll(MinOfferStartDate.get(inputObj.MSISDN));
				startEpoch.add(mo.Start_Date_Time);
				MinOfferStartDate.put( inputObj.MSISDN, startEpoch);
			}
			else
			{
				TreeSet<String> startEpoch = new TreeSet<String>();
				startEpoch.add(mo.Start_Date_Time);
				MinOfferStartDate.put( inputObj.MSISDN, startEpoch);
			}
			
			if(MaxOfferExpiryDate.containsKey(inputObj.MSISDN))
			{
				TreeSet<String> expiryEpoch = new TreeSet<String>();
				expiryEpoch.addAll(MaxOfferExpiryDate.get(inputObj.MSISDN));
				expiryEpoch.add(mo.End_Date_Time);
				MaxOfferExpiryDate.put( inputObj.MSISDN, expiryEpoch); 
			}
			else
			{
				TreeSet<String> expiryEpoch = new TreeSet<String>();
				expiryEpoch.add(mo.End_Date_Time);
				MaxOfferExpiryDate.put( inputObj.MSISDN, expiryEpoch);
			}
		}
		
		//now create a list sorted based on start date.
		TreeSet<Long> sortedStartDate = new TreeSet<Long>(OfferStartDateMap.values());
		List<String> finalSortedAddonKey = new ArrayList<String>();
		for(Long offerEpoch: sortedStartDate)
		{
			for(Entry<String, Long> entryKey : OfferStartDateMap.entrySet())
			{
				if(entryKey.getValue().equals(offerEpoch))
					finalSortedAddonKey.add(entryKey.getKey());
			}
		}
		
		for(String entryKey  : finalSortedAddonKey)
		{
			ADDON_OFFER mo = inputObj.new ADDON_OFFER();
			mo = (ADDON_OFFER)inputObj.ADDON_OFFER.get(entryKey);
			String Offer_ID = entryKey.split(";")[0];
			String Offer_Sequence = entryKey.split(";")[1];
			//String MSISDN = AddOnOffer.split(",")[0];
			String End_Date_Time = mo.End_Date_Time;
			String Start_Date_Time = mo.Start_Date_Time;
			
			if(LoadSubscriberMapping.multiPurposeOfferMapping.containsKey(Offer_ID))
			{
				Map<String, String> hMapData = Arrays.stream( LoadSubscriberMapping.multiPurposeOfferMapping.get(Offer_ID).split(",") )
				        .map(s -> s.split("="))
				        .collect(Collectors.toMap(s -> s[0].trim(), s -> s[1].trim()));
				
				long ExpiryCheck = CommonUtilities.convertDateToEpoch(CommonUtilities.getEndOfDay(mo.End_Date_Time));
				long curentDate = CommonUtilities.convertDateToEpoch(CommonUtilities.getEndOfDay(df.format(dateobj))) ;
				if(ExpiryCheck < curentDate)
				{
					//onlyLog.add("INC11:MSISDN=" + inputObj.MSISDN + ":OFFER_ID=" + mo.Offer_ID + ":UC_Value=" + mo.Usage_Value +":UC_Threshold=" + mo.Usage_Threshold + ":EXPIRY_DATE=" + mo.End_Date_Time + ":DESCRIPTION=OFFER EXPIRED:ACTION=DISCARD & LOG" );
					continue;
				}
				
				if(mo.Usage_Value.equals("0"))
				{
					//onlyLog.add("INC11:MSISDN=" + inputObj.MSISDN + ":OFFER_ID=" + mo.Offer_ID + ":UC_Value=" + mo.Usage_Value +":UC_Threshold=" + mo.Usage_Threshold +  ":EXPIRY_DATE=" + mo.End_Date_Time + ":DESCRIPTION=UC is zero:ACTION=DISCARD & LOG" );
					continue;
				}
				
				if((ProductIDLookUpMap.containsKey(inputObj.MSISDN + ";" +Offer_Sequence +";"+ Offer_ID)))
					productID = Integer.parseInt(ProductIDLookUpMap.get(inputObj.MSISDN + ";" +Offer_Sequence +";"+ Offer_ID));
				
				if(hMapData.get("Multi_Benefit").equals("Y"))
				{
					if(hMapData.get("Multi_Purchase_Offer").equals("Y"))
					{
						if(hMapData.get("Main_Offer_PP").equals("Y"))
						{
							if(hMapData.get("Implicit_Offer_Enable_UC").equals("Y"))
							{
								UCValue.add(PopulateUsageCounter(Offer_ID, inputObj.MSISDN, mo.Usage_Value,String.valueOf(productID)));
							}
							if(hMapData.get("Main_Offer_Enable_UC").equals("Y"))
							{
								UCValue.add(PopulateUsageCounter(hMapData.get("Main_offer_id"), inputObj.MSISDN, mo.Usage_Value,String.valueOf(productID)));
							}
						}
						else if(hMapData.get("Main_Offer_PP").equals("N"))
						{
							if(hMapData.get("Implicit_Offer_Enable_UC").equals("Y"))
							{
								UCValue.add(PopulateUsageCounter(Offer_ID, inputObj.MSISDN, mo.Usage_Value,String.valueOf(productID)));
							}
							if(hMapData.get("Main_Offer_Enable_UC").equals("Y"))
							{
								UCValue.add(PopulateUsageCounter(hMapData.get("Main_offer_id"), inputObj.MSISDN, mo.Usage_Value,String.valueOf(productID)));
							}
						}
					}
					else if(hMapData.get("Multi_Purchase_Offer").equals("N"))
					{
						if(hMapData.get("Main_Offer_PP").equals("Y"))
						{
							//no such case
						}
						else if(hMapData.get("Main_Offer_PP").equals("N"))
						{
							if(hMapData.get("Implicit_Offer_Enable_UC").equals("Y"))
							{
								UCValue.add(PopulateUsageCounter(Offer_ID, inputObj.MSISDN, mo.Usage_Value,String.valueOf("0")));
							}
							
							if(hMapData.get("Main_Offer_Enable_UC").equals("Y"))
							{
								UCValue.add(PopulateUsageCounter(hMapData.get("Main_offer_id"), inputObj.MSISDN, mo.Usage_Value,String.valueOf("0")));
							}
							
						}
					}
				}
				else if(hMapData.get("Multi_Benefit").equals("N"))
				{
					if(hMapData.get("Multi_Purchase_Offer").equals("Y"))
					{
						if(hMapData.get("Main_Offer_PP").equals("Y"))
						{
							if(hMapData.get("Implicit_Offer_Enable_UC").equals("Y"))
							{
								UCValue.add(PopulateUsageCounter(Offer_ID, inputObj.MSISDN, mo.Usage_Value,String.valueOf(productID)));
							}
							
							if(hMapData.get("Main_Offer_Enable_UC").equals("Y"))
							{
								UCValue.add(PopulateUsageCounter(hMapData.get("Main_offer_id"), inputObj.MSISDN, mo.Usage_Value,String.valueOf(productID)));
							}
							
						}
						else if(hMapData.get("Main_Offer_PP").equals("N"))
						{
							if(hMapData.get("Implicit_Offer_Enable_UC").equals("Y"))
							{
								UCValue.add(PopulateUsageCounter(Offer_ID, inputObj.MSISDN, mo.Usage_Value,String.valueOf(productID)));
							}
							
							if(hMapData.get("Main_Offer_Enable_UC").equals("Y"))
							{
								UCValue.add(PopulateUsageCounter(hMapData.get("Main_offer_id"), inputObj.MSISDN, mo.Usage_Value,String.valueOf(productID)));
							}
							
						}
					}
					else if(hMapData.get("Multi_Purchase_Offer").equals("N"))
					{
						if(hMapData.get("Main_Offer_PP").equals("Y"))
						{
							//No need to implement
						}
						else if(hMapData.get("Main_Offer_PP").equals("N"))
						{
							if(hMapData.get("Implicit_Offer_Enable_UC").equals("Y"))
							{
								UCValue.add(PopulateUsageCounter(Offer_ID, inputObj.MSISDN, mo.Usage_Value,String.valueOf("0")));
							}
							
							if(hMapData.get("Main_Offer_Enable_UC").equals("Y"))
							{
								UCValue.add(PopulateUsageCounter(hMapData.get("Main_offer_id"), inputObj.MSISDN, mo.Usage_Value,String.valueOf("0")));
							}							
						}
					}
				}
			}
		}
		return UCValue;
	}

	private Collection<? extends String> GenerateFromTimeUC() {
		// TODO Auto-generated method stub
		List<String> UCValue = new ArrayList<String>();
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date dateobj = new Date();
		
		for(Map.Entry<String,Object> entry : inputObj.MAIN_OFFER.entrySet())
		{
			MAIN_OFFER mo = inputObj.new MAIN_OFFER();
			mo = (MAIN_OFFER)entry.getValue();
			
			long ExpiryCheck = CommonUtilities.convertDateToEpoch(mo.End_Date_Time);
			long curentDate = CommonUtilities.convertDateToEpoch(df.format(dateobj)); 
			
			if(ExpiryCheck!=0 && ExpiryCheck < curentDate)
			{
				//onlyLog.add("INC11:MSISDN=" + inputObj.MSISDN + ":OFFER_ID=" + mo.Offer_ID + ":UC_Value=" + mo.Usage_Value +":UC_Threshold=" + mo.Usage_Threshold + ":EXPIRY_DATE=" + mo.End_Date_Time + ":DESCRIPTION=OFFER EXPIRED:ACTION=DISCARD & LOG" );
				continue;
			}
			
			if(mo.Offer_ID.length() > 0 && LoadSubscriberMapping.implicitOfferMapping.contains(Integer.valueOf(mo.Offer_ID)) 
					&& !mo.Usage_Value.isEmpty() && Integer.valueOf(mo.Usage_Value) > 0)
			{
				UCValue.add(PopulateUsageCounter(mo.Offer_ID, inputObj.MSISDN, mo.Usage_Value,
						"0"));
			}
			//else
				//onlyLog.add("INC12:MSISDN=" + inputObj.MSISDN + ":OFFER_ID=" + mo.Offer_ID + ":UC_Value=" + mo.Usage_Value +":UC_Threshold=" + mo.Usage_Threshold + ":DESCRIPTION=Offer OUT OF SCOPE:ACTION=DISCARD & LOG" );
		}
		return UCValue;
	}
	
	public String PopulateUsageCounter(String UC_ID, String Msisdn, String Balance, String productID)
	{
		StringBuilder sb = new StringBuilder();
		
		sb.append(Msisdn).append(",");
		sb.append(UC_ID).append(",");
		sb.append(Msisdn).append(",");
		sb.append(Balance).append(",");
		sb.append(productID).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_NULL")).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_NULL"));
		
		return sb.toString();
	}
	
	
	/*****************************phase2*****************************/
	
	
	public Collection<? extends String> executePhase2() {
		// TODO Auto-generated method stub
		List<String> UCList = new ArrayList<>();
		UCList.addAll(GenerateFromSCFile());
		return UCList;
	}
	
	private Collection<? extends String> GenerateFromSCFile() {
		// TODO Auto-generated method stub
		List<String> UCList = new ArrayList<String>();

		DecimalFormat df = new DecimalFormat("####");
		for(Entry<String, String> etr : LoadSubscriberMapping.mainCreditLimitInput.entrySet())
		{
			if(LoadSubscriberMapping.CompleteListToMigrate.contains(etr.getKey()))
			{
				String MSISDN = etr.getValue().split(",",-1)[0];
				String UC_Value = etr.getValue().split(",",-1)[9];
				if(UC_Value.length() > 0 && !UC_Value.equals("0"))
				{
					String UC_ID = LoadSubscriberMapping.CommonConfigMapping.get("UC_ID");
					BigDecimal sourceUCValue = new BigDecimal(UC_Value);
					BigDecimal UC_FACTOR = new BigDecimal(LoadSubscriberMapping.CommonConfigMapping.get("UC_FACTOR"));
					String Usage_Value = String.valueOf(df.format(sourceUCValue.multiply(UC_FACTOR)));
					UCList.add(PopulateUsageCounter(UC_ID, MSISDN, Usage_Value,"0"));
				}
			}
		}
		return UCList;
	}

	
	//&&&&&&&&&&&&&&&&&&&&&&&&Added code to Phase1...For extra records&&&&&&&&&&&&&&&&&
	
	public List<String> executeAdditionalRecord() {
		// TODO Auto-generated method stub
		List<String> UCList = new ArrayList<>();
		UCList.addAll(GenerateExtraUCFromAddonFile());
		return UCList;
	}
	
	private Collection<? extends String> GenerateExtraUCFromAddonFile() {
		// TODO Auto-generated method stub
		List<String> UCValue = new ArrayList<String>();
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date dateobj = new Date();
		for(Map.Entry<String, Set<String>> entry : LoadSubscriberMapping.Addon_OfferExtraInput.entrySet())
		{
			boolean mainOffer = false;
			if(!LoadSubscriberMapping.CompleteListToMigrate.contains(entry.getKey()))
			{
				for(String str: entry.getValue())
				{
					String OfferID = str.split(",",-1)[1];
					String UsageCounter = str.split(",",-1)[4];
					String UsageThreshold = str.split(",",-1)[5];
					rejectAndLog.add("INC01:MSISDN=" + entry.getKey() + ":OFFER_UV_UT=" +OfferID+"-" + UsageCounter +"-" + UsageThreshold + ":DESCRIPTION=MSISDN NOT PRESENT IN CS:ACTION=DISCARD & LOG");
				}				
			}
			else
			{
				if(!CompletedMSISDN.contains(entry.getKey()))
				{
					Map<String,String> AddonInput = new HashMap<String,String>();
					
					for(String str: entry.getValue())
					{
						String OfferID = str.split(",",-1)[1];
						String SequenceID = str.split(",",-1)[6];
						AddonInput.put(OfferID + ";" + SequenceID, str);
					}
					
					Map<String, TreeSet<String>> MinOfferStartDate = new HashMap<String, TreeSet<String>>();
					Map<String, TreeSet<String>> MaxOfferExpiryDate = new HashMap<String, TreeSet<String>>();
					Map<String, Long> OfferStartDateMap = new HashMap<String, Long>();
					TreeSet<String> sortedAddonKey = new TreeSet<String>(AddonInput.keySet());
					
					for(String entryKey  : sortedAddonKey)
					{
						String OfferID = entryKey.split(";")[0];
						String Offer_Sequence = entryKey.split(";")[1];
						String MSISDN = AddonInput.get(entryKey).split(",")[0];
						String End_Date_Time = AddonInput.get(entryKey).split(",")[3];
						String Start_Date_Time = AddonInput.get(entryKey).split(",")[2];
						
						
						if(OfferStartDateMap.containsKey(OfferID + ";" + Offer_Sequence))
						{
							OfferStartDateMap.put(OfferID + ";" + Offer_Sequence, CommonUtilities.convertDateToEpochSeconds(Start_Date_Time));
						}
						else
						{
							OfferStartDateMap.put(OfferID + ";" + Offer_Sequence, CommonUtilities.convertDateToEpochSeconds(Start_Date_Time));
						}
						
						if(MinOfferStartDate.containsKey(MSISDN))
						{
							TreeSet<String> startEpoch = new TreeSet<String>();
							startEpoch.addAll(MinOfferStartDate.get(MSISDN));
							startEpoch.add(Start_Date_Time);
							MinOfferStartDate.put( MSISDN, startEpoch);
						}
						else
						{
							TreeSet<String> startEpoch = new TreeSet<String>();
							startEpoch.add(Start_Date_Time);
							MinOfferStartDate.put( MSISDN, startEpoch);
						}
						
						if(MaxOfferExpiryDate.containsKey(MSISDN))
						{
							TreeSet<String> expiryEpoch = new TreeSet<String>();
							expiryEpoch.addAll(MaxOfferExpiryDate.get(MSISDN));
							expiryEpoch.add(End_Date_Time);
							MaxOfferExpiryDate.put( MSISDN, expiryEpoch); 
						}
						else
						{
							TreeSet<String> expiryEpoch = new TreeSet<String>();
							expiryEpoch.add(End_Date_Time);
							MaxOfferExpiryDate.put( MSISDN, expiryEpoch);
						}
					}
					
					//now create a list sorted based on start date.
					TreeSet<Long> sortedStartDate = new TreeSet<Long>(OfferStartDateMap.values());
					List<String> finalSortedAddonKey = new ArrayList<String>();
					for(Long offerEpoch: sortedStartDate)
					{
						for(Entry<String, Long> entryKey : OfferStartDateMap.entrySet())
						{
							if(entryKey.getValue().equals(offerEpoch))
								finalSortedAddonKey.add(entryKey.getKey());
						}
					}
					
					int productID = Integer.parseInt(LoadSubscriberMapping.CommonConfigMapping.get("P_Counter"));
					//for(Map.Entry<String,Object> entry : inputObj.ADDON_OFFER.entrySet())
					for(String entryKey  : finalSortedAddonKey)
					{
						String AddOnOffer = AddonInput.get(entryKey);
						String Offer_ID = AddOnOffer.split(",")[1];
						
											
						String MSISDN = AddOnOffer.split(",")[0];
						String End_Date_Time = AddOnOffer.split(",")[3];
						String Start_Date_Time = AddOnOffer.split(",")[2];
						String Usage_Value = AddOnOffer.split(",")[4];
						String Usage_Threshold = AddOnOffer.split(",")[5];
						String Offer_Sequence = AddOnOffer.split(",")[6];
						
						//if(MSISDN.equals("66320333"))
						//	System.out.println("Vipin");
						if(LoadSubscriberMapping.multiPurposeOfferMapping.containsKey(Offer_ID))
						{
							//List<String> mpOffer = Arrays.asList(LoadSubscriberMapping.multiPurposeOfferMapping.get(OfferID).split(","));
							Map<String, String> hMapData = Arrays.stream( LoadSubscriberMapping.multiPurposeOfferMapping.get(Offer_ID).split(",") )
							        .map(s -> s.split("="))
							        .collect(Collectors.toMap(s -> s[0].trim(), s -> s[1].trim()));
							
							long ExpiryCheck = CommonUtilities.convertDateToEpoch(CommonUtilities.getEndOfDay(End_Date_Time));
							long curentDate = CommonUtilities.convertDateToEpoch(CommonUtilities.getEndOfDay(df.format(dateobj))) ;
							if(ExpiryCheck < curentDate)
							{
								//onlyLog.add("INC11:MSISDN=" + MSISDN + ":OFFER_ID=" + Offer_ID + ":UC_Value=" + Usage_Value +":UC_Threshold=" + Usage_Threshold + ":EXPIRY_DATE=" + End_Date_Time + ":DESCRIPTION=OFFER EXPIRED:ACTION=DISCARD & LOG" );
								continue;
							}
							
							if(Usage_Value.equals("0"))
							{
								//onlyLog.add("INC11:MSISDN=" + MSISDN + ":OFFER_ID=" + Offer_ID + ":UC_Value=" + Usage_Value +":UC_Threshold=" + Usage_Threshold +  ":EXPIRY_DATE=" + End_Date_Time + ":DESCRIPTION=UC is zero:ACTION=DISCARD & LOG" );
								continue;
							}
							
							if((ProductIDLookUpMap.containsKey(MSISDN + ";" +Offer_Sequence +";"+ Offer_ID)))
								productID = Integer.parseInt(ProductIDLookUpMap.get(MSISDN + ";" +Offer_Sequence +";"+ Offer_ID));
							
							if(hMapData.get("Multi_Benefit").equals("Y"))
							{
								if(hMapData.get("Multi_Purchase_Offer").equals("Y"))
								{
									if(hMapData.get("Main_Offer_PP").equals("Y"))
									{
										if(hMapData.get("Implicit_Offer_Enable_UC").equals("Y"))
										{
											UCValue.add(PopulateUsageCounter(Offer_ID, MSISDN, Usage_Value,String.valueOf(productID)));
										}
										
										if(hMapData.get("Main_Offer_Enable_UC").equals("Y"))
										{
											UCValue.add(PopulateUsageCounter(hMapData.get("Main_offer_id"), MSISDN, Usage_Value,String.valueOf(productID)));
										}
									}
									else if(hMapData.get("Main_Offer_PP").equals("N"))
									{
										if(hMapData.get("Implicit_Offer_Enable_UC").equals("Y"))
										{
											UCValue.add(PopulateUsageCounter(Offer_ID, MSISDN, Usage_Value,String.valueOf(productID)));
										}
										
										if(hMapData.get("Main_Offer_Enable_UC").equals("Y"))
										{
											UCValue.add(PopulateUsageCounter(hMapData.get("Main_offer_id"), MSISDN, Usage_Value,String.valueOf(productID)));
										}
									}
								}
								else if(hMapData.get("Multi_Purchase_Offer").equals("N"))
								{
									if(hMapData.get("Main_Offer_PP").equals("Y"))
									{
										//no such case
									}
									else if(hMapData.get("Main_Offer_PP").equals("N"))
									{
										if(hMapData.get("Implicit_Offer_Enable_UC").equals("Y"))
										{
											UCValue.add(PopulateUsageCounter(Offer_ID, MSISDN, Usage_Value,String.valueOf("0")));
										}
										
										if(hMapData.get("Main_Offer_Enable_UC").equals("Y"))
										{
											UCValue.add(PopulateUsageCounter(hMapData.get("Main_offer_id"), MSISDN, Usage_Value,String.valueOf("0")));
										}
										
									}
								}
							}
							else if(hMapData.get("Multi_Benefit").equals("N"))
							{
								if(hMapData.get("Multi_Purchase_Offer").equals("Y"))
								{
									if(hMapData.get("Main_Offer_PP").equals("Y"))
									{
										if(hMapData.get("Implicit_Offer_Enable_UC").equals("Y"))
										{
											UCValue.add(PopulateUsageCounter(Offer_ID, MSISDN, Usage_Value,String.valueOf(productID)));
										}
										
										if(hMapData.get("Main_Offer_Enable_UC").equals("Y"))
										{
											UCValue.add(PopulateUsageCounter(hMapData.get("Main_offer_id"), MSISDN, Usage_Value,String.valueOf(productID)));
										}
										
									}
									else if(hMapData.get("Main_Offer_PP").equals("N"))
									{
										if(hMapData.get("Implicit_Offer_Enable_UC").equals("Y"))
										{
											UCValue.add(PopulateUsageCounter(Offer_ID, MSISDN, Usage_Value,String.valueOf(productID)));
										}
										if(hMapData.get("Main_Offer_Enable_UC").equals("Y"))
										{
											UCValue.add(PopulateUsageCounter(hMapData.get("Main_offer_id"), MSISDN, Usage_Value,String.valueOf("0")));
										}
									}
								}
								else if(hMapData.get("Multi_Purchase_Offer").equals("N"))
								{
									if(hMapData.get("Main_Offer_PP").equals("Y"))
									{
										//No need to implement
									}
									else if(hMapData.get("Main_Offer_PP").equals("N"))
									{
										if(hMapData.get("Implicit_Offer_Enable_UC").equals("Y"))
										{
											UCValue.add(PopulateUsageCounter(Offer_ID, MSISDN, Usage_Value,String.valueOf("0")));
										}
										if(hMapData.get("Main_Offer_Enable_UC").equals("Y"))
										{
											UCValue.add(PopulateUsageCounter(hMapData.get("Main_offer_id"), MSISDN, Usage_Value,String.valueOf("0")));
										}
									}
								}
							}
						}
					}
				}				
			}				
		}
		
		return UCValue;
	}
	
}
