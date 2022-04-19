package com.ericsson.dm.transform.implementation;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
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

public class Offer {
	
	Set<String> rejectAndLog;
	Set<String> onlyLog;
	CommonFunctions commonfunction;
	JsonInputFields inputObj;
	Map<String, String> ProductIDLookUpMap;
	Set<String> CompletedMSISDN;
	Set<String> EnablerOfferSet;
	boolean isProductPrivate;
	public Offer()
	{
		
	}
	 
	public Offer(Set<String> rejectAndLog, Set<String> onlyLog,Set<String> CompletedMSISDN, Map<String, String> ProductIDLookUpMap) {
		// TODO Auto-generated constructor stub
		this.rejectAndLog = rejectAndLog;
		this.onlyLog = onlyLog;
		this.CompletedMSISDN = CompletedMSISDN;
		commonfunction = new CommonFunctions();		
		this.ProductIDLookUpMap = ProductIDLookUpMap;
		
	}
	
	public Offer(Set<String> rejectAndLog, Set<String> onlyLog,JsonInputFields inputObj, Map<String, String> ProductIDLookUpMap, boolean isProductPrivate) {
		// TODO Auto-generated constructor stub
		this.rejectAndLog = rejectAndLog;
		this.onlyLog = onlyLog;
		this.inputObj = inputObj;
		commonfunction = new CommonFunctions();		
		this.ProductIDLookUpMap = ProductIDLookUpMap;
		this.isProductPrivate = isProductPrivate;
	}
	
	public List<String> execute() {
		// TODO Auto-generated method stub
		List<String> OfferValue = new ArrayList<String>();
		
		//populate enabler offer
		getEnablerOfferList();
		OfferValue.addAll(GenerateFromTimeOffer());
		OfferValue.addAll(GenerateMultiEnablerFromAddonFile());
		OfferValue.addAll(GenerateMainOfferFromAddonFile());
		return OfferValue;
	}

	private void getEnablerOfferList() {
		// TODO Auto-generated method stub
		this.EnablerOfferSet = new TreeSet<String>();
		for(Entry<String,String> etr : LoadSubscriberMapping.multiPurposeOfferMapping.entrySet())
		{
			Map<String, String> hMapData = Arrays.stream( LoadSubscriberMapping.multiPurposeOfferMapping.get(etr.getKey()).split(",") )
			        .map(s -> s.split("="))
			        .collect(Collectors.toMap(s -> s[0].trim(), s -> s[1].trim()));
			if(hMapData.get("Enabler").equals("Y") )
			{
				EnablerOfferSet.add(etr.getKey());				
			}				
		}
	}

	private Collection<? extends String> GenerateMainOfferFromAddonFile() {
		// TODO Auto-generated method stub
		List<String> OfferValue = new ArrayList<String>();
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date dateobj = new Date();
		
		int productID = Integer.parseInt(LoadSubscriberMapping.CommonConfigMapping.get("P_Counter"));
		TreeSet<String> sortedAddonKey = new TreeSet<String>(inputObj.ADDON_OFFER.keySet());
		
		Map<String, TreeSet<String>> MinOfferStartDate = new HashMap<String, TreeSet<String>>();
		Map<String, TreeSet<String>> MaxOfferExpiryDate = new HashMap<String, TreeSet<String>>();
		Map<String, Long> OfferStartDateMap = new HashMap<String, Long>();
		
		Set<String> createSingleMainOffer = new HashSet<String>();
		for(String entryKey  : sortedAddonKey)
		{
			ADDON_OFFER mo = inputObj.new ADDON_OFFER();
			mo = (ADDON_OFFER)inputObj.ADDON_OFFER.get(entryKey);
			
			String OfferID = entryKey.split(";")[0];
			if(!EnablerOfferSet.contains(OfferID))
			{
		
				if(OfferStartDateMap.containsKey(OfferID + ";" + mo.Offer_Sequence))
				{
					OfferStartDateMap.put(OfferID + ";" + mo.Offer_Sequence, CommonUtilities.convertDateToEpochSeconds(mo.Start_Date_Time));
				}
				else
				{
					OfferStartDateMap.put(OfferID + ";" + mo.Offer_Sequence, CommonUtilities.convertDateToEpochSeconds(mo.Start_Date_Time));
				}
				
				if(MinOfferStartDate.containsKey(OfferID))
				{
					TreeSet<String> startEpoch = new TreeSet<String>();
					startEpoch.addAll(MinOfferStartDate.get(OfferID));
					startEpoch.add(mo.Start_Date_Time);
					MinOfferStartDate.put( OfferID, startEpoch);
				}
				else
				{
					TreeSet<String> startEpoch = new TreeSet<String>();
					startEpoch.add(mo.Start_Date_Time);
					MinOfferStartDate.put( OfferID, startEpoch);
				}
				
				if(MaxOfferExpiryDate.containsKey(OfferID))
				{
					TreeSet<String> expiryEpoch = new TreeSet<String>();
					expiryEpoch.addAll(MaxOfferExpiryDate.get(OfferID));
					expiryEpoch.add(mo.End_Date_Time);
					MaxOfferExpiryDate.put( OfferID, expiryEpoch); 
				}
				else
				{
					TreeSet<String> expiryEpoch = new TreeSet<String>();
					expiryEpoch.add(mo.End_Date_Time);
					MaxOfferExpiryDate.put( OfferID, expiryEpoch);
				}
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
			
			if(LoadSubscriberMapping.multiPurposeOfferMapping.containsKey(Offer_ID) && !EnablerOfferSet.contains(Offer_ID))
			{
				Map<String, String> hMapData = Arrays.stream( LoadSubscriberMapping.multiPurposeOfferMapping.get(Offer_ID).split(",") )
				        .map(s -> s.split("="))
				        .collect(Collectors.toMap(s -> s[0].trim(), s -> s[1].trim()));
				
				long ExpiryCheck = CommonUtilities.convertDateToEpoch(CommonUtilities.getEndOfDay(mo.End_Date_Time));
				long curentDate = CommonUtilities.convertDateToEpoch(CommonUtilities.getEndOfDay(df.format(dateobj))) ;
				if(ExpiryCheck < curentDate)
				{
					onlyLog.add("INC11:MSISDN=" + inputObj.MSISDN + ":OFFER_ID=" + mo.Offer_ID + ":UC_Value=" + mo.Usage_Value +":UC_Threshold=" + mo.Usage_Threshold + ":EXPIRY_DATE=" + mo.End_Date_Time + ":DESCRIPTION=OFFER EXPIRED:ACTION=DISCARD & LOG" );
					continue;
				}
				
				//getting min start date and max expiry date
				String mainOfferStartDate = MinOfferStartDate.get(Offer_ID).first();
				String mainOfferExpiryDate = MaxOfferExpiryDate.get(Offer_ID).last();
				
				if(hMapData.get("Multi_Benefit").equals("Y"))
				{
					if(hMapData.get("Multi_Purchase_Offer").equals("Y"))
					{
						if(hMapData.get("Main_Offer_PP").equals("Y"))
						{
							if(!createSingleMainOffer.contains(hMapData.get("Main_offer_id")))
							{
								productID++;
								ProductIDLookUpMap.put(inputObj.MSISDN +";"+ Offer_Sequence +";"+ Offer_ID, String.valueOf(productID));
								OfferValue.add(PopulateOffer(inputObj.MSISDN,hMapData.get("Main_offer_id"), "TIMER", mainOfferStartDate, mainOfferExpiryDate,
									String.valueOf(productID), LoadSubscriberMapping.CommonConfigMapping.get("Default_0")));
								createSingleMainOffer.add(hMapData.get("Main_offer_id"));
							}
							productID++;
							ProductIDLookUpMap.put(inputObj.MSISDN +";"+ Offer_Sequence +";"+ Offer_ID, String.valueOf(productID));
							OfferValue.add(PopulateOffer(inputObj.MSISDN,Offer_ID, "TIMER", Start_Date_Time, End_Date_Time,
								String.valueOf(productID), LoadSubscriberMapping.CommonConfigMapping.get("Default_0")));
						}
						else if(hMapData.get("Main_Offer_PP").equals("N"))
						{
							if(!createSingleMainOffer.contains(hMapData.get("Main_offer_id")))
							{
								OfferValue.add(PopulateOffer(inputObj.MSISDN,hMapData.get("Main_offer_id"), "TIMER", mainOfferStartDate, mainOfferExpiryDate,
									LoadSubscriberMapping.CommonConfigMapping.get("Default_0"), LoadSubscriberMapping.CommonConfigMapping.get("Default_0")));
								createSingleMainOffer.add(hMapData.get("Main_offer_id"));
							}
							productID++;
							ProductIDLookUpMap.put(inputObj.MSISDN +";"+ Offer_Sequence +";"+ Offer_ID, String.valueOf(productID));
							OfferValue.add(PopulateOffer(inputObj.MSISDN,Offer_ID, "TIMER", Start_Date_Time, End_Date_Time,
								String.valueOf(productID), LoadSubscriberMapping.CommonConfigMapping.get("Default_0")));
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
							if(!createSingleMainOffer.contains(hMapData.get("Main_offer_id")))
							{
								OfferValue.add(PopulateOffer(inputObj.MSISDN,hMapData.get("Main_offer_id"), "TIMER", mainOfferStartDate, mainOfferExpiryDate,
									LoadSubscriberMapping.CommonConfigMapping.get("Default_0"), LoadSubscriberMapping.CommonConfigMapping.get("Default_0")));
								
								createSingleMainOffer.add(hMapData.get("Main_offer_id"));
							}
							OfferValue.add(PopulateOffer(inputObj.MSISDN,Offer_ID, "TIMER", Start_Date_Time, End_Date_Time,
									LoadSubscriberMapping.CommonConfigMapping.get("Default_0"), LoadSubscriberMapping.CommonConfigMapping.get("Default_0")));
						}
					}
				}
				else if(hMapData.get("Multi_Benefit").equals("N"))
				{
					if(hMapData.get("Multi_Purchase_Offer").equals("Y"))
					{
						if(hMapData.get("Main_Offer_PP").equals("Y"))
						{
							productID++;
							ProductIDLookUpMap.put(inputObj.MSISDN +";"+ Offer_Sequence +";"+ Offer_ID, String.valueOf(productID));
							OfferValue.add(PopulateOffer(inputObj.MSISDN,Offer_ID, "TIMER", Start_Date_Time, End_Date_Time,
								String.valueOf(productID), LoadSubscriberMapping.CommonConfigMapping.get("Default_0")));
						}
						else if(hMapData.get("Main_Offer_PP").equals("N"))
						{
							productID++;
							ProductIDLookUpMap.put(inputObj.MSISDN +";"+ Offer_Sequence +";"+ Offer_ID, String.valueOf(productID));
							OfferValue.add(PopulateOffer(inputObj.MSISDN,Offer_ID, "TIMER", Start_Date_Time, End_Date_Time,
								String.valueOf(productID), LoadSubscriberMapping.CommonConfigMapping.get("Default_0")));
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
							OfferValue.add(PopulateOffer(inputObj.MSISDN,Offer_ID, "TIMER", Start_Date_Time, End_Date_Time,
									LoadSubscriberMapping.CommonConfigMapping.get("Default_0"), LoadSubscriberMapping.CommonConfigMapping.get("Default_0")));
						
						}
					}
				}
			}
		}
		return OfferValue;
	}

	private Collection<? extends String> GenerateMultiEnablerFromAddonFile() {
		// TODO Auto-generated method stub
		List<String> OfferValue = new ArrayList<String>();
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date dateobj = new Date();
		
		TreeSet<String> sortedAddonKey = new TreeSet<String>(inputObj.ADDON_OFFER.keySet());
		int productID = Integer.parseInt(LoadSubscriberMapping.CommonConfigMapping.get("P_Counter"));
		Map<String, TreeSet<String>> OfferStartDate = new HashMap<String, TreeSet<String>>();
		Map<String, TreeSet<String>> OfferExpiryDate = new HashMap<String, TreeSet<String>>();
		for(String entryKey  : sortedAddonKey)
		{
			ADDON_OFFER mo = inputObj.new ADDON_OFFER();
			mo = (ADDON_OFFER)inputObj.ADDON_OFFER.get(entryKey);
			String Offer_ID = entryKey.split(";")[0];
			String Offer_Sequence = entryKey.split(";")[1];
			//String MSISDN = AddOnOffer.split(",")[0];
			String End_Date_Time = mo.End_Date_Time;
			String Start_Date_Time = mo.Start_Date_Time;
			String Usage_Value = mo.Usage_Value;
			String Usage_Threshold = mo.Usage_Threshold;
			
			if(LoadSubscriberMapping.multiPurposeOfferMapping.containsKey(Offer_ID))
			{
				Map<String, String> hMapData = Arrays.stream( LoadSubscriberMapping.multiPurposeOfferMapping.get(Offer_ID).split(",") )
				        .map(s -> s.split("="))
				        .collect(Collectors.toMap(s -> s[0].trim(), s -> s[1].trim()));
				if(hMapData.get("Enabler").equals("Y") )
				{
					OfferValue.add(PopulateEnablerOffer(inputObj.MSISDN,Offer_ID, "TIMER", Start_Date_Time, End_Date_Time,
						"0", LoadSubscriberMapping.CommonConfigMapping.get("Default_0")));
				
				}
			}
		}
		return OfferValue;
	}
	
	private Collection<? extends String> GenerateFromTimeOffer() {
		// TODO Auto-generated method stub
		List<String> OfferValue = new ArrayList<String>();
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
				onlyLog.add("INC11:MSISDN=" + inputObj.MSISDN + ":OFFER_ID=" + mo.Offer_ID + ":UC_Value=" + mo.Usage_Value +":UC_Threshold=" + mo.Usage_Threshold +  ":EXPIRY_DATE=" + mo.End_Date_Time + ":DESCRIPTION=OFFER EXPIRED:ACTION=DISCARD & LOG" );
				continue;
			}
			if(LoadSubscriberMapping.ParentChildRejectionMSISDN.containsKey(inputObj.MSISDN) && LoadSubscriberMapping.sharedInputRelationMSISDN.contains(inputObj.MSISDN) && LoadSubscriberMapping.ParentChildRejectionMSISDN.get(inputObj.MSISDN) != null)
			{
				//onlyLog.add("INC12:MSISDN=" + inputObj.MSISDN + ":OFFER_ID=" + LoadSubscriberMapping.ParentChildRejectionMSISDN.get(MSISDN) + ":UC_Value=0" +":UC_Threshold=0" + ":DESCRIPTION=Offer OUT OF SCOPE:ACTION=DISCARD & LOG" );
				onlyLog.add("INC12:MSISDN=" + inputObj.MSISDN + ":OFFER_ID=" + mo.Offer_ID + ":UC_Value=" + mo.Usage_Value +":UC_Threshold=" + mo.Usage_Threshold + ":DESCRIPTION=Offer OUT OF SCOPE:ACTION=DISCARD & LOG" );
			}
			else
			{
				if(mo.Offer_ID.length() > 0 && LoadSubscriberMapping.timerOfferMapping.contains(Integer.valueOf(mo.Offer_ID)))
				{
					OfferValue.add(PopulateOffer(mo.Offer_ID, "TIMER", mo.Start_Date_Time, mo.End_Date_Time,
							LoadSubscriberMapping.CommonConfigMapping.get("Default_0"),
							LoadSubscriberMapping.CommonConfigMapping.get("Default_0")));
				}
				else
				{
					onlyLog.add("INC12:MSISDN=" + inputObj.MSISDN + ":OFFER_ID=" + mo.Offer_ID + ":UC_Value=" + mo.Usage_Value +":UC_Threshold=" + mo.Usage_Threshold + ":DESCRIPTION=Offer OUT OF SCOPE:ACTION=DISCARD & LOG" );
				}
			}
		}
		return OfferValue;
	}

	
	/////////////////////////For master Code(100*)//////////////
		
	public List<String> execute(String Master) {
		// TODO Auto-generated method stub
		
		List<String> OfferValue = new ArrayList<String>();
		OfferValue.addAll(GenerateFromTimeOffer("ForMaster"));
		return OfferValue;
	}
	
	private Collection<? extends String> GenerateFromTimeOffer(String Master) {
		// TODO Auto-generated method stub
		List<String> OfferValue = new ArrayList<String>();
		
		//Calculate the minimum start date from available startdate
		HashMap<Long, String> MinimumStartDateMap = new HashMap<Long, String>();
		for(Map.Entry<String,Object> entry : inputObj.MAIN_OFFER.entrySet())
		{
			MAIN_OFFER mo = inputObj.new MAIN_OFFER();
			mo = (MAIN_OFFER)entry.getValue();
			if(!mo.Start_Date_Time.isEmpty())
			{
				MinimumStartDateMap.put(CommonUtilities.convertDateToEpochSeconds(mo.Start_Date_Time), mo.Start_Date_Time);
			}
			
			if(mo.Offer_ID.length() > 0 && LoadSubscriberMapping.timerOfferMapping.contains(Integer.valueOf(mo.Offer_ID)))
			{
				OfferValue.add(PopulateOffer(mo.Offer_ID, "TIMER", mo.Start_Date_Time, mo.End_Date_Time,
						LoadSubscriberMapping.CommonConfigMapping.get("Default_0"),
						LoadSubscriberMapping.CommonConfigMapping.get("Default_0")));
			}
		}
		Long miimumDateInSec = Collections.min(MinimumStartDateMap.keySet());
		String MinimumStartDate = MinimumStartDateMap.get(miimumDateInSec);
		
		//Create two hardcoded offer as well 5001 and 7201
		
		OfferValue.add(PopulateOffer("5001", "TIMER", MinimumStartDate, "",
				LoadSubscriberMapping.CommonConfigMapping.get("Default_0"),
				LoadSubscriberMapping.CommonConfigMapping.get("Default_0")));
		
		OfferValue.add(PopulateOffer("7201", "TIMER", MinimumStartDate, "",
				LoadSubscriberMapping.CommonConfigMapping.get("Default_0"),
				LoadSubscriberMapping.CommonConfigMapping.get("Default_0")));
		
		return OfferValue;
	}

	private String PopulateOffer(String Offer_ID,String Offer_Type, String Start_Date,String Expiry_Date, 
			String Product_Private, String flag) {
		//StringBuffer sb = new StringBuffer();
		String Offer_Startdate = "";
		String Offer_StartSec= "";
		String Offer_Expirydate= "";
		String Offer_ExpirySec= "";
	
		if(Offer_Type.toUpperCase().equals("TIMER"))
		{
			if(!Start_Date.isEmpty() )
			{
				Offer_Startdate = CommonUtilities.convertDateToTimerOfferDate(Start_Date)[0].toString();
				Offer_StartSec = CommonUtilities.convertDateToTimerOfferDate(Start_Date)[1].toString();								
			}
			else
			{
				Offer_Startdate = LoadSubscriberMapping.CommonConfigMapping.get("Default_0").toString();
				Offer_StartSec = LoadSubscriberMapping.CommonConfigMapping.get("Default_0").toString();
			}
			if(!Expiry_Date.isEmpty())
			{
				Offer_Expirydate = CommonUtilities.convertDateToTimerOfferDate(Expiry_Date)[0].toString(); 
				Offer_ExpirySec = CommonUtilities.convertDateToTimerOfferDate(Expiry_Date)[1].toString(); 
			}
			else
			{
				Offer_Expirydate = LoadSubscriberMapping.CommonConfigMapping.get("Default_0").toString();
				Offer_ExpirySec = LoadSubscriberMapping.CommonConfigMapping.get("Default_0").toString();
			}
		}
		else
		{
			if(!Start_Date.isEmpty())
			{
				Offer_Startdate = String.valueOf(CommonUtilities.convertDateToEpoch(Start_Date));
				Offer_StartSec = LoadSubscriberMapping.CommonConfigMapping.get("Default_0").toString();								
			}
			else
			{
				Offer_Startdate = LoadSubscriberMapping.CommonConfigMapping.get("Default_0").toString();
				Offer_StartSec = LoadSubscriberMapping.CommonConfigMapping.get("Default_0").toString();
			}
			if(!Expiry_Date.isEmpty())
			{
				Offer_Expirydate = String.valueOf(CommonUtilities.convertDateToEpoch(Expiry_Date)); 
				Offer_ExpirySec = LoadSubscriberMapping.CommonConfigMapping.get("Default_0").toString(); 
			}
			else
			{
				Offer_Expirydate = LoadSubscriberMapping.CommonConfigMapping.get("Default_0").toString();
				Offer_ExpirySec = LoadSubscriberMapping.CommonConfigMapping.get("Default_0").toString();
			}
		}
		
		
		StringBuffer sb = new StringBuffer();
		sb.append(inputObj.MSISDN).append(",");
		sb.append(Offer_ID).append(",");
		sb.append(Offer_Startdate).append(",");
		sb.append(Offer_Expirydate).append(",");
		sb.append(Offer_StartSec).append(",");
		sb.append(Offer_ExpirySec).append(",");
		sb.append(flag).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_NULL")).append(",");
		sb.append(Product_Private);
		
		// TODO Auto-generated method stub
		return sb.toString();
	}

	private String PopulateOffer(String MSISDN,String Offer_ID,String Offer_Type, String Start_Date,String Expiry_Date, 
			String Product_Private, String flag) {
		//StringBuffer sb = new StringBuffer();
		String Offer_Startdate = "";
		String Offer_StartSec= "";
		String Offer_Expirydate= "";
		String Offer_ExpirySec= "";
	
		if(Offer_Type.toUpperCase().equals("TIMER"))
		{
			if(!Start_Date.isEmpty() )
			{
				Offer_Startdate = CommonUtilities.convertDateToTimerOfferDate(Start_Date)[0].toString();
				Offer_StartSec = CommonUtilities.convertDateToTimerOfferDate(Start_Date)[1].toString();								
			}
			else
			{
				Offer_Startdate = LoadSubscriberMapping.CommonConfigMapping.get("Default_0").toString();
				Offer_StartSec = LoadSubscriberMapping.CommonConfigMapping.get("Default_0").toString();
			}
			if(!Expiry_Date.isEmpty())
			{
				Offer_Expirydate = CommonUtilities.convertDateToTimerOfferDate(Expiry_Date)[0].toString(); 
				Offer_ExpirySec = CommonUtilities.convertDateToTimerOfferDate(Expiry_Date)[1].toString(); 
			}
			else
			{
				Offer_Expirydate = LoadSubscriberMapping.CommonConfigMapping.get("Default_NULL").toString();
				Offer_ExpirySec = LoadSubscriberMapping.CommonConfigMapping.get("Default_NULL").toString();
			}
		}
		else
		{
			if(!Start_Date.isEmpty())
			{
				Offer_Startdate = String.valueOf(CommonUtilities.convertDateToEpoch(Start_Date));
				Offer_StartSec = LoadSubscriberMapping.CommonConfigMapping.get("Default_0").toString();								
			}
			else
			{
				Offer_Startdate = LoadSubscriberMapping.CommonConfigMapping.get("Default_0").toString();
				Offer_StartSec = LoadSubscriberMapping.CommonConfigMapping.get("Default_0").toString();
			}
			if(!Expiry_Date.isEmpty())
			{
				Offer_Expirydate = String.valueOf(CommonUtilities.convertDateToEpoch(Expiry_Date)); 
				Offer_ExpirySec = LoadSubscriberMapping.CommonConfigMapping.get("Default_0").toString(); 
			}
			else
			{
				Offer_Expirydate = LoadSubscriberMapping.CommonConfigMapping.get("Default_0").toString();
				Offer_ExpirySec = LoadSubscriberMapping.CommonConfigMapping.get("Default_0").toString();
			}
		}
		
		
		StringBuffer sb = new StringBuffer();
		sb.append(MSISDN).append(",");
		sb.append(Offer_ID).append(",");
		sb.append(Offer_Startdate).append(",");
		sb.append(Offer_Expirydate).append(",");
		sb.append(Offer_StartSec).append(",");
		sb.append(Offer_ExpirySec).append(",");
		sb.append(flag).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_NULL")).append(",");
		sb.append(Product_Private);
		
		// TODO Auto-generated method stub
		return sb.toString();
	}
	
	private String PopulateEnablerOffer(String MSISDN,String Offer_ID,String Offer_Type, String Start_Date,String Expiry_Date, 
			String Product_Private, String flag) {
		//StringBuffer sb = new StringBuffer();
		String Offer_Startdate = "";
		String Offer_StartSec= "";
		String Offer_Expirydate= "";
		String Offer_ExpirySec= "";
	
		if(Offer_Type.toUpperCase().equals("TIMER"))
		{
			if(!Start_Date.isEmpty() )
			{
				Offer_Startdate = CommonUtilities.convertDateToTimerOfferDate(Start_Date)[0].toString();
				Offer_StartSec = CommonUtilities.convertDateToTimerOfferDate(Start_Date)[1].toString();								
			}
			else
			{
				Offer_Startdate = LoadSubscriberMapping.CommonConfigMapping.get("Default_0").toString();
				Offer_StartSec = LoadSubscriberMapping.CommonConfigMapping.get("Default_0").toString();
			}
			if(!Expiry_Date.isEmpty())
			{
				Offer_Expirydate = CommonUtilities.convertDateToTimerOfferDate(Expiry_Date)[0].toString(); 
				Offer_ExpirySec = CommonUtilities.convertDateToTimerOfferDate(Expiry_Date)[1].toString(); 
			}
			else
			{
				Offer_Expirydate = LoadSubscriberMapping.CommonConfigMapping.get("Default_NULL").toString();
				Offer_ExpirySec = LoadSubscriberMapping.CommonConfigMapping.get("Default_NULL").toString();
			}
		}
		else
		{
			if(!Start_Date.isEmpty())
			{
				Offer_Startdate = String.valueOf(CommonUtilities.convertDateToEpoch(Start_Date));
				Offer_StartSec = LoadSubscriberMapping.CommonConfigMapping.get("Default_0").toString();								
			}
			else
			{
				Offer_Startdate = LoadSubscriberMapping.CommonConfigMapping.get("Default_0").toString();
				Offer_StartSec = LoadSubscriberMapping.CommonConfigMapping.get("Default_0").toString();
			}
			if(!Expiry_Date.isEmpty())
			{
				Offer_Expirydate = String.valueOf(CommonUtilities.convertDateToEpoch(Expiry_Date)); 
				Offer_ExpirySec = LoadSubscriberMapping.CommonConfigMapping.get("Default_0").toString(); 
			}
			else
			{
				Offer_Expirydate = LoadSubscriberMapping.CommonConfigMapping.get("Default_0").toString();
				Offer_ExpirySec = LoadSubscriberMapping.CommonConfigMapping.get("Default_0").toString();
			}
		}
		
		
		StringBuffer sb = new StringBuffer();
		sb.append(MSISDN).append(",");
		sb.append(Offer_ID).append(",");
		sb.append(Offer_Startdate).append(",");
		sb.append("0").append(",");
		sb.append(Offer_StartSec).append(",");
		sb.append("0").append(",");
		sb.append(flag).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_NULL")).append(",");
		sb.append(Product_Private);
		
		// TODO Auto-generated method stub
		return sb.toString();
	}

	/*****************************phase2*****************************/
	
	public Collection<? extends String> executePhase2() {
		// TODO Auto-generated method stub
		List<String> OfferList = new ArrayList<>();
		//Commented this code on Oct1,2020, as it is not needed.
		//OfferList.addAll(GenerateFromSCFile());
		return OfferList;
	}

	private Collection<? extends String> GenerateFromSCFile() {
		// TODO Auto-generated method stub
		List<String> OfferList = new ArrayList<String>();
		int j = 0;
		for(Entry<String, String> etr : LoadSubscriberMapping.mainCreditLimitInput.entrySet())
		{
			if(LoadSubscriberMapping.CompleteListToMigrate.contains(etr.getKey()))
			{
				String MSISDN = etr.getValue().split(",",-1)[0];
				int SC = Integer.parseInt(etr.getValue().split(",",-1)[1]);
				//If service_class=3300/4200 create default offer 5001
				//if(SC == 3300 || SC == 4200)
				if(SC == 3300)
				{	
					String startDate = LoadSubscriberMapping.CommonConfigMapping.get("Default_Offer_Date").substring(0,4) + "-" + 
							LoadSubscriberMapping.CommonConfigMapping.get("Default_Offer_Date").substring(4,6) + "-" +
							LoadSubscriberMapping.CommonConfigMapping.get("Default_Offer_Date").substring(6,8) + " " +
							LoadSubscriberMapping.CommonConfigMapping.get("Default_Offer_Date").split(" ")[1];
					
					OfferList.add(PopulateOffer(MSISDN, "5001", "TIMER", startDate, "", LoadSubscriberMapping.CommonConfigMapping.get("Default_0"),
							LoadSubscriberMapping.CommonConfigMapping.get("Default_0")));
				}
			}
		}
		return OfferList;
	}

	//&&&&&&&&&&&&&&&&&&&&&&&&Added code to Phase1...For extra records&&&&&&&&&&&&&&&&&
	
	public List<String> executeAdditionalRecord() {
		// TODO Auto-generated method stub
		List<String> OfferValue = new ArrayList<String>();
		getEnablerOfferList();
		OfferValue.addAll(GenerateExtraEnablerFromAddonFile());
		OfferValue.addAll(GenerateExtraFromAddonFile());
		return OfferValue;
	}
	
	private Collection<? extends String> GenerateExtraFromAddonFile() {
		// TODO Auto-generated method stub
		List<String> OfferValue = new ArrayList<String>();
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date dateobj = new Date();
		
		for(Map.Entry<String, Set<String>> entry : LoadSubscriberMapping.Addon_OfferExtraInput.entrySet())
		{
			Set<String> createSingleMainOffer = new HashSet<String>();
			
			if(!LoadSubscriberMapping.CompleteListToMigrate.contains(entry.getKey()))
			{
				for(String str: entry.getValue())
				{
					String OfferID = str.split(",",-1)[1];
					String UsageCounter = str.split(",",-1)[4];
					String UsageThreshold = str.split(",",-1)[5];
					rejectAndLog.add("INC01:MSISDN=" + entry.getKey() + ":OFFER_UV_UT=" +OfferID+"-" + UsageCounter +"-" + UsageThreshold + ":DESCRIPTION=MSISDN NOT PRESENT IN CS:ACTION=DISCARD & LOG");
					continue;
				}				
			}
			else
			{
				
				if(!CompletedMSISDN.contains(entry.getKey()))
				{
					if(LoadSubscriberMapping.AlreadyMigratedMsisdn.contains(entry.getKey()))
					{
						for(String str: entry.getValue())
						{
							String OfferID = str.split(",",-1)[1];
							String UsageCounter = str.split(",",-1)[4];
							String UsageThreshold = str.split(",",-1)[5];
							rejectAndLog.add("INC01:MSISDN=" + entry.getKey() + ":OFFER_UV_UT=" +OfferID+"-" + UsageCounter +"-" + UsageThreshold + ":DESCRIPTION=MSISDN ALREADY MIGRATED:ACTION=DISCARD & LOG");
						}					
					}
					else 
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
							
							if(MinOfferStartDate.containsKey(OfferID))
							{
								TreeSet<String> startEpoch = new TreeSet<String>();
								startEpoch.addAll(MinOfferStartDate.get(OfferID));
								startEpoch.add(Start_Date_Time);
								MinOfferStartDate.put( OfferID, startEpoch);
							}
							else
							{
								TreeSet<String> startEpoch = new TreeSet<String>();
								startEpoch.add(Start_Date_Time);
								MinOfferStartDate.put( OfferID, startEpoch);
							}
							
							if(MaxOfferExpiryDate.containsKey(OfferID))
							{
								TreeSet<String> expiryEpoch = new TreeSet<String>();
								expiryEpoch.addAll(MaxOfferExpiryDate.get(OfferID));
								expiryEpoch.add(End_Date_Time);
								MaxOfferExpiryDate.put( OfferID, expiryEpoch); 
							}
							else
							{
								TreeSet<String> expiryEpoch = new TreeSet<String>();
								expiryEpoch.add(End_Date_Time);
								MaxOfferExpiryDate.put( OfferID, expiryEpoch);
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
							
							if(LoadSubscriberMapping.multiPurposeOfferMapping.containsKey(Offer_ID) && !EnablerOfferSet.contains(Offer_ID))
							{
								//List<String> mpOffer = Arrays.asList(LoadSubscriberMapping.multiPurposeOfferMapping.get(OfferID).split(","));
								Map<String, String> hMapData = Arrays.stream( LoadSubscriberMapping.multiPurposeOfferMapping.get(Offer_ID).split(",") )
								        .map(s -> s.split("=")).collect(Collectors.toMap(s -> s[0].trim(), s -> s[1].trim()));
								
								long ExpiryCheck = CommonUtilities.convertDateToEpoch(CommonUtilities.getEndOfDay(End_Date_Time));
								long curentDate = CommonUtilities.convertDateToEpoch(CommonUtilities.getEndOfDay(df.format(dateobj))) ;
								if(ExpiryCheck < curentDate)
								{
									onlyLog.add("INC11:MSISDN=" + MSISDN + ":OFFER_ID=" + Offer_ID + ":UC_Value=" + Usage_Value +":UC_Threshold=" + Usage_Threshold + ":EXPIRY_DATE=" + End_Date_Time + ":DESCRIPTION=OFFER EXPIRED:ACTION=DISCARD & LOG" );
									continue;
								}
								
								//populate max expiry and min start date for he main offer
								String mainOfferStartDate = MinOfferStartDate.get(Offer_ID).first();
								String mainOfferExpiryDate = MaxOfferExpiryDate.get(Offer_ID).last();
								
								if(hMapData.get("Multi_Benefit").equals("Y"))
								{
									if(hMapData.get("Multi_Purchase_Offer").equals("Y"))
									{
										if(hMapData.get("Main_Offer_PP").equals("Y"))
										{
											if(!createSingleMainOffer.contains(hMapData.get("Main_offer_id")))
											{
												productID++;
												ProductIDLookUpMap.put(MSISDN +";"+ Offer_Sequence +";"+ Offer_ID, String.valueOf(productID));
												OfferValue.add(PopulateOffer(MSISDN,hMapData.get("Main_offer_id"), "TIMER", mainOfferStartDate, mainOfferExpiryDate,
													String.valueOf(productID), LoadSubscriberMapping.CommonConfigMapping.get("Default_0")));
												createSingleMainOffer.add(hMapData.get("Main_offer_id"));
											}
											productID++;
											ProductIDLookUpMap.put(MSISDN +";"+ Offer_Sequence +";"+ Offer_ID, String.valueOf(productID));
											OfferValue.add(PopulateOffer(MSISDN,Offer_ID, "TIMER", Start_Date_Time, End_Date_Time,
												String.valueOf(productID), LoadSubscriberMapping.CommonConfigMapping.get("Default_0")));
										}
										else if(hMapData.get("Main_Offer_PP").equals("N"))
										{
											if(!createSingleMainOffer.contains(hMapData.get("Main_offer_id")))
											{
												OfferValue.add(PopulateOffer(MSISDN,hMapData.get("Main_offer_id"), "TIMER", mainOfferStartDate, mainOfferExpiryDate,
													LoadSubscriberMapping.CommonConfigMapping.get("Default_0"), LoadSubscriberMapping.CommonConfigMapping.get("Default_0")));
												
												createSingleMainOffer.add(hMapData.get("Main_offer_id"));
											}
											productID++;
											ProductIDLookUpMap.put(MSISDN +";"+ Offer_Sequence +";"+ Offer_ID, String.valueOf(productID));
											OfferValue.add(PopulateOffer(MSISDN,Offer_ID, "TIMER", Start_Date_Time, End_Date_Time,
												String.valueOf(productID), LoadSubscriberMapping.CommonConfigMapping.get("Default_0")));
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
											if(!createSingleMainOffer.contains(hMapData.get("Main_offer_id")))
											{
												OfferValue.add(PopulateOffer(MSISDN,hMapData.get("Main_offer_id"), "TIMER", mainOfferStartDate, mainOfferExpiryDate,
													LoadSubscriberMapping.CommonConfigMapping.get("Default_0"), LoadSubscriberMapping.CommonConfigMapping.get("Default_0")));
												
												createSingleMainOffer.add(hMapData.get("Main_offer_id"));
											}
											OfferValue.add(PopulateOffer(MSISDN,Offer_ID, "TIMER", Start_Date_Time, End_Date_Time,
													LoadSubscriberMapping.CommonConfigMapping.get("Default_0"), LoadSubscriberMapping.CommonConfigMapping.get("Default_0")));
										}
									}
								}
								else if(hMapData.get("Multi_Benefit").equals("N"))
								{
									if(hMapData.get("Multi_Purchase_Offer").equals("Y"))
									{
										if(hMapData.get("Main_Offer_PP").equals("Y"))
										{
											productID++;
											ProductIDLookUpMap.put(MSISDN +";"+ Offer_Sequence +";"+ Offer_ID, String.valueOf(productID));
											OfferValue.add(PopulateOffer(MSISDN,Offer_ID, "TIMER", Start_Date_Time, End_Date_Time,
												String.valueOf(productID), LoadSubscriberMapping.CommonConfigMapping.get("Default_0")));
										}
										else if(hMapData.get("Main_Offer_PP").equals("N"))
										{
											productID++;
											ProductIDLookUpMap.put(MSISDN +";"+ Offer_Sequence +";"+ Offer_ID, String.valueOf(productID));
											OfferValue.add(PopulateOffer(MSISDN,Offer_ID, "TIMER", Start_Date_Time, End_Date_Time,
												String.valueOf(productID), LoadSubscriberMapping.CommonConfigMapping.get("Default_0")));
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
											OfferValue.add(PopulateOffer(MSISDN,Offer_ID, "TIMER", Start_Date_Time, End_Date_Time,
													LoadSubscriberMapping.CommonConfigMapping.get("Default_0"), LoadSubscriberMapping.CommonConfigMapping.get("Default_0")));
										
										}
									}
								}	
							}
						}
					}
					
				}
			}
		}
		return OfferValue;
	}

	private Collection<? extends String> GenerateExtraEnablerFromAddonFile() {
		// TODO Auto-generated method stub
		List<String> OfferValue = new ArrayList<String>();
		for(Map.Entry<String, Set<String>> entry : LoadSubscriberMapping.Addon_OfferExtraInput.entrySet())
		{
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
					if(LoadSubscriberMapping.AlreadyMigratedMsisdn.contains(entry.getKey()))
					{
						for(String str: entry.getValue())
						{
							String OfferID = str.split(",",-1)[1];
							String UsageCounter = str.split(",",-1)[4];
							String UsageThreshold = str.split(",",-1)[5];
							rejectAndLog.add("INC01:MSISDN=" + entry.getKey() + ":OFFER_UV_UT=" +OfferID+"-" + UsageCounter +"-" + UsageThreshold + ":DESCRIPTION=MSISDN ALREADY MIGRATED:ACTION=DISCARD & LOG");
						}					
					}
					else 
					{
						Map<String,String> AddonInput = new HashMap<String,String>();
						for(String str: entry.getValue())
						{
							String OfferID = str.split(",",-1)[1];
							String Sequence = str.split(",",-1)[6];
							AddonInput.put(OfferID + ";" + Sequence, str);
						}
						
						TreeSet<String> sortedAddonKey = new TreeSet<String>(AddonInput.keySet());
						
						//for(Map.Entry<String,Object> entry : inputObj.ADDON_OFFER.entrySet())
						for(String entryKey  : sortedAddonKey)
						{
							String AddOnOffer = AddonInput.get(entryKey);
							String Offer_ID = entryKey.split(";")[0];
							String Offer_Sequence = entryKey.split(";")[1];
												
							String MSISDN = AddOnOffer.split(",")[0];
							String End_Date_Time = AddOnOffer.split(",")[3];
							String Start_Date_Time = AddOnOffer.split(",")[2];
							String Usage_Value = AddOnOffer.split(",")[4];
							String Usage_Threshold = AddOnOffer.split(",")[5];
							if(LoadSubscriberMapping.multiPurposeOfferMapping.containsKey(Offer_ID))
							{
								//List<String> mpOffer = Arrays.asList(LoadSubscriberMapping.multiPurposeOfferMapping.get(OfferID).split(","));
								Map<String, String> hMapData = Arrays.stream( LoadSubscriberMapping.multiPurposeOfferMapping.get(Offer_ID).split(",") )
								        .map(s -> s.split("="))
								        .collect(Collectors.toMap(s -> s[0].trim(), s -> s[1].trim()));
								if(hMapData.get("Enabler").equals("Y") )
								{
									OfferValue.add(PopulateEnablerOffer(MSISDN,Offer_ID, "TIMER", Start_Date_Time, End_Date_Time,
										"0", LoadSubscriberMapping.CommonConfigMapping.get("Default_0")));
								
								}
							}
						}
					}
				}
			}
		}
		return OfferValue;
	}

}
