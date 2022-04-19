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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.ericsson.dm.Utils.CommonUtilities;
import com.ericsson.dm.inititialization.LoadSubscriberMapping;
import com.ericsson.dm.transformation.JsonInputFields;
import com.ericsson.dm.transformation.JsonInputFields.ADDON_OFFER;
import com.ericsson.dm.transformation.JsonInputFields.CREDIT_LIMIT;
import com.ericsson.dm.transformation.JsonInputFields.IMPLICIT_SHARED_OFFER;
import com.ericsson.dm.transformation.JsonInputFields.MAIN_OFFER;


public class UsageThreshold {
	Set<String> rejectAndLog;
	Set<String> onlyLog;
	List<String> DuplicateUTLog;
	JsonInputFields inputObj;
	CommonFunctions commonfunction;	
	Set<String> CompletedMSISDN;
	boolean isProductPrivate;
	//Map<String, String> ProductIDLookUpMap;
	HashMap<String, Set<String>> AvoidDuplicateUT;
	public UsageThreshold( Set<String> rejectAndLog, Set<String> onlyLog, Set<String> CompletedMSISDN, Map<String, String> ProductIDLookUpMap, List<String> DuplicateUTLog) {
		this.rejectAndLog=rejectAndLog;
		this.onlyLog=onlyLog;
		this.DuplicateUTLog = DuplicateUTLog;
		this.AvoidDuplicateUT = new HashMap<String, Set<String>>();
		this.CompletedMSISDN = CompletedMSISDN;
		this.commonfunction = new CommonFunctions();
		
		//this.ProductIDLookUpMap = ProductIDLookUpMap;
		// TODO Auto-generated constructor stub
	}
	
	public UsageThreshold( Set<String> rejectAndLog, Set<String> onlyLog,JsonInputFields inputObj, Map<String, String> ProductIDLookUpMap,boolean isProductPrivate,List<String> DuplicateUTLog) {
		this.rejectAndLog=rejectAndLog;
		this.onlyLog=onlyLog;
		this.inputObj = inputObj;
		this.DuplicateUTLog = DuplicateUTLog;
		this.AvoidDuplicateUT = new HashMap<String, Set<String>>();
		this.commonfunction = new CommonFunctions();
		this.isProductPrivate = isProductPrivate;
		//this.ProductIDLookUpMap = ProductIDLookUpMap;
		// TODO Auto-generated constructor stub
	}
	
	public List<String> execute() {
		// TODO Auto-generated method stub
		List<String> UTList = new ArrayList<>();
		this.AvoidDuplicateUT.clear();
		UTList.addAll(GenerateFromTimeUsageThreshold());
		UTList.addAll(GenerateFromImplicitOffer());
		UTList.addAll(GenerateMainUTFromAddonFile());
		UTList.addAll(GenerateSpecialUTFromAddonFile());
		return UTList;
	}
	
	private Collection<? extends String> GenerateSpecialUTFromAddonFile() {
		// TODO Auto-generated method stub
		List<String> UTValue = new ArrayList<String>();
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date dateobj = new Date();
		
		TreeSet<String> sortedAddonKey = new TreeSet<String>(inputObj.ADDON_OFFER.keySet());
		for(String entryKey  : sortedAddonKey)
		{
			ADDON_OFFER mo = inputObj.new ADDON_OFFER();
			mo = (ADDON_OFFER)inputObj.ADDON_OFFER.get(entryKey);
			String OfferID = entryKey.split(";")[0];
			int Quantity = Integer.valueOf(mo.Quality);
			long ExpiryCheck = CommonUtilities.convertDateToEpoch(CommonUtilities.getEndOfDay(mo.End_Date_Time));
			long curentDate = CommonUtilities.convertDateToEpoch(CommonUtilities.getEndOfDay(df.format(dateobj))) ;
			if(ExpiryCheck < curentDate)
			{
				//onlyLog.add("INC11:MSISDN=" + inputObj.MSISDN + ":OFFER_ID=" + mo.Offer_ID + ":UC_Value=" + mo.Usage_Value +":UC_Threshold=" + mo.Usage_Threshold + ":EXPIRY_DATE=" + mo.End_Date_Time + ":DESCRIPTION=OFFER EXPIRED:ACTION=DISCARD & LOG" );
				continue;
			}
			else
			{
				if(LoadSubscriberMapping.SpecialUTMapping.containsKey(OfferID) && (Quantity) > 0)
				{
					String specialUT = LoadSubscriberMapping.SpecialUTMapping.get(OfferID);
					String UTID = specialUT.split(",")[0].split("=")[1];
					//Quantity = Quantity + 1;
					String UT_Value = String.valueOf(Long.parseLong(specialUT.split(",")[1].split("_")[1]) * (Quantity));
					UTValue.add(PopulateUsageThreshold(UTID, inputObj.MSISDN, UT_Value));
					break;
				}
			}
		}	
		
		return UTValue;
	}

	private Collection<? extends String> GenerateMainUTFromAddonFile() {
		// TODO Auto-generated method stub
		List<String> UTValue = new ArrayList<String>();
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
			
			HashSet<String> DuplicateUTpresent = new HashSet<String>();
			DuplicateUTpresent.addAll(AvoidDuplicateUT.get(inputObj.MSISDN));
			if(!DuplicateUTpresent.contains(Offer_ID))
			{
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
					
					//if((ProductIDLookUpMap.containsKey(inputObj.MSISDN + ";" +Offer_Sequence +";"+ Offer_ID)))
					//	productID = Integer.parseInt(ProductIDLookUpMap.get(inputObj.MSISDN + ";" +Offer_Sequence +";"+ Offer_ID));
					
					if(hMapData.get("Multi_Benefit").equals("Y"))
					{
						if(hMapData.get("Multi_Purchase_Offer").equals("Y"))
						{
							if(hMapData.get("Main_Offer_PP").equals("Y"))
							{
								if(hMapData.get("Implicit_Offer_Enable_UT").equals("Y"))
								{
									UTValue.add(PopulateUsageThreshold(Offer_ID, inputObj.MSISDN, mo.Usage_Threshold));
								}
								if(hMapData.get("Main_Offer_Enable_UT").equals("Y"))
								{
									UTValue.add(PopulateUsageThreshold(Offer_ID, inputObj.MSISDN, mo.Usage_Threshold));
								}
								
							}
							else if(hMapData.get("Main_Offer_PP").equals("N"))
							{
								if(hMapData.get("Implicit_Offer_Enable_UT").equals("Y"))
								{
									UTValue.add(PopulateUsageThreshold(Offer_ID, inputObj.MSISDN, mo.Usage_Threshold));
								}
								if(hMapData.get("Main_Offer_Enable_UT").equals("Y"))
								{
									UTValue.add(PopulateUsageThreshold(Offer_ID, inputObj.MSISDN, mo.Usage_Threshold));
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
								if(hMapData.get("Implicit_Offer_Enable_UT").equals("Y"))
								{
									UTValue.add(PopulateUsageThreshold(Offer_ID, inputObj.MSISDN, mo.Usage_Threshold));
								}
								if(hMapData.get("Main_Offer_Enable_UT").equals("Y"))
								{
									UTValue.add(PopulateUsageThreshold(hMapData.get("Main_offer_id"), inputObj.MSISDN, mo.Usage_Threshold));
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
								if(hMapData.get("Implicit_Offer_Enable_UT").equals("Y"))
								{
									UTValue.add(PopulateUsageThreshold(Offer_ID, inputObj.MSISDN, mo.Usage_Threshold));
								}
								if(hMapData.get("Main_Offer_Enable_UT").equals("Y"))
								{
									UTValue.add(PopulateUsageThreshold(hMapData.get("Main_offer_id"), inputObj.MSISDN, mo.Usage_Threshold));
								}
							}
							else if(hMapData.get("Main_Offer_PP").equals("N"))
							{
								if(hMapData.get("Implicit_Offer_Enable_UT").equals("Y"))
								{
									UTValue.add(PopulateUsageThreshold(Offer_ID, inputObj.MSISDN, mo.Usage_Threshold));
								}
								if(hMapData.get("Main_Offer_Enable_UT").equals("Y"))
								{
									UTValue.add(PopulateUsageThreshold(Offer_ID, inputObj.MSISDN, mo.Usage_Threshold));
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
								if(hMapData.get("Implicit_Offer_Enable_UT").equals("Y"))
								{
									UTValue.add(PopulateUsageThreshold(Offer_ID, inputObj.MSISDN, mo.Usage_Threshold));
								}
								if(hMapData.get("Main_Offer_Enable_UT").equals("Y"))
								{
									UTValue.add(PopulateUsageThreshold(Offer_ID, inputObj.MSISDN, mo.Usage_Threshold));
								}
							}
						}
					}
				}
			}
			else
			{
				DuplicateUTLog.add("INC55:MSISDN=" + inputObj.MSISDN  + ":OFFER_UV_UT=" + mo.Offer_ID+"-" + mo.Usage_Value +"-" + mo.Usage_Value + ":DESCRIPTION=Duplicate UsageThreshold is rejected=DISCARD & LOG");
			}
		}
		return UTValue;
	}

	private Collection<? extends String> GenerateFromImplicitOffer() {
		// TODO Auto-generated method stub
		List<String> UTList = new ArrayList<>();
		for(Map.Entry<String,Object> entry : inputObj.IMPLICIT_SHARED_OFFER.entrySet())
		{
			IMPLICIT_SHARED_OFFER mo = inputObj.new IMPLICIT_SHARED_OFFER();
			mo = (IMPLICIT_SHARED_OFFER)entry.getValue();
			
			if(mo.Shared_Offer_ID.length() > 0 && LoadSubscriberMapping.sharedOfferMapping.containsKey((mo.Shared_Offer_ID)) 
					&& !mo.Usage_Threshold.isEmpty() && Integer.valueOf(mo.Usage_Threshold) > 0)
			{
				String UTFlag = LoadSubscriberMapping.sharedOfferMapping.get(mo.Shared_Offer_ID).split(",")[1].split("=")[1];
				if(UTFlag.equals("Y"))
					UTList.add(PopulateUsageThreshold(mo.Shared_Offer_ID, inputObj.MSISDN, mo.Usage_Threshold));
				else
					onlyLog.add("INC12:MSISDN=" + inputObj.MSISDN + ":OFFER_ID=" + mo.Shared_Offer_ID +  ":UC_Value=" + mo.Usage_Value +":UC_Threshold=" + mo.Usage_Threshold + ":DESCRIPTION=Offer OUT OF SCOPE:ACTION=DISCARD & LOG" );
			}
		}
		
		return UTList;
	}

	private Collection<? extends String> GenerateFromTimeUsageThreshold() {
		// TODO Auto-generated method stub
		List<String> UTList = new ArrayList<>();
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date dateobj = new Date();
		for(Map.Entry<String,Object> entry : inputObj.MAIN_OFFER.entrySet())
		{
			MAIN_OFFER mo = inputObj.new MAIN_OFFER();
			mo = (MAIN_OFFER)entry.getValue();
			
			long ExpiryCheck = CommonUtilities.convertDateToEpoch(CommonUtilities.getEndOfDay(mo.End_Date_Time));
			long curentDate = CommonUtilities.convertDateToEpoch(CommonUtilities.getEndOfDay(df.format(dateobj))); 
			
			if(ExpiryCheck!=0 && ExpiryCheck < curentDate)
			{
				onlyLog.add("INC11:MSISDN=" + inputObj.MSISDN + ":OFFER_ID=" + mo.Offer_ID + ":EXPIRY_DATE=" + mo.End_Date_Time + ":DESCRIPTION=OFFER EXPIRED:ACTION=DISCARD & LOG" );
				continue;
			}
			
			if(mo.Offer_ID.length() > 0 && LoadSubscriberMapping.implicitOfferMapping.contains(Integer.valueOf(mo.Offer_ID)) 
					&& !mo.Usage_Value.isEmpty() && Integer.valueOf(mo.Usage_Threshold) > 0)
			{
				UTList.add(PopulateUsageThreshold(mo.Offer_ID, inputObj.MSISDN, mo.Usage_Threshold));
			}
			//else
				 //":UC_Value=" + mo.Usage_Value +":UC_Threshold=" + mo.Usage_Threshold +
				//onlyLog.add("INC12:MSISDN=" + inputObj.MSISDN + ":OFFER_ID=" + mo.Offer_ID +  ":UC_Value=" + mo.Usage_Value +":UC_Threshold=" + mo.Usage_Threshold + ":DESCRIPTION=Offer OUT OF SCOPE:ACTION=DISCARD & LOG" );
		}	
		
		return UTList;
	}

	private String PopulateUsageThreshold(String offer_ID, String mSISDN, String usage_Threshold) {
		// TODO Auto-generated method stub
		StringBuilder sb = new StringBuilder();
		sb.append(mSISDN).append(",");
		sb.append(offer_ID).append(",");
		sb.append(mSISDN).append(",");
		sb.append(usage_Threshold);
		
		//make an entry to check duplicate entry for the UT and acoid it.
		if(AvoidDuplicateUT.containsKey(mSISDN))
		{
			HashSet<String> temp = new HashSet<String>();
			temp.addAll(AvoidDuplicateUT.get(mSISDN));
			temp.add(offer_ID);
			AvoidDuplicateUT.put(mSISDN, temp);
		}
		else
		{
			HashSet<String> temp = new HashSet<String>();
			temp.add(offer_ID);
			AvoidDuplicateUT.put(mSISDN, temp);
		}
		return sb.toString();
	}
	
	//&&&&&&&&&&&&&&&&&&&&&&&&Added code to Phase2...&&&&&&&&&&&&&&&&&
	
	public Collection<? extends String> executePhase2() {
		// TODO Auto-generated method stub
		List<String> UTValue = new ArrayList<String>();		
		UTValue.addAll(GenerateFromCreditLimit());		
		return UTValue;
	}

	private Collection<? extends String> GenerateFromCreditLimit() {
		// TODO Auto-generated method stub
		DecimalFormat df = new DecimalFormat("########");
		List<String> UTList = new ArrayList<String>();
		
		for(Entry<String, String> etr : LoadSubscriberMapping.mainCreditLimitInput.entrySet())
		{
			//if(etr.getKey().equals("36039731"))
			//	System.out.println("VipinSIngh");
			if(LoadSubscriberMapping.CompleteListToMigrate.contains(etr.getKey()))
			{

				Map<String, String> creditLimit = new HashMap<String, String>();
				String MSISDN = etr.getValue().split(",",-1)[0];
				
				creditLimit.put("MSISDN", MSISDN);
			
				creditLimit.put("REFERENCE_CREDIT_LIMIT", etr.getValue().split(",",-1)[2]);
				creditLimit.put("SUBSCRIBER_BALANCE", etr.getValue().split(",",-1)[3]);
				creditLimit.put("AVAILABLE_CREDIT_LIMIT", etr.getValue().split(",",-1)[4]);
				creditLimit.put("MINPAY_FACTOR", etr.getValue().split(",",-1)[5]);
				creditLimit.put("Notification_70", etr.getValue().split(",",-1)[6]);
				creditLimit.put("Notification_80", etr.getValue().split(",",-1)[7]);
				creditLimit.put("Notification_90", etr.getValue().split(",",-1)[8]);
				
				if(creditLimit.size() !=0)
				{		
					if(LoadSubscriberMapping.AlreadyMigratedMsisdn.contains(MSISDN))
					{
						rejectAndLog.add("INC02:MSISDN=" + etr.getValue().split(",",-1)[0] + ":REFERENCE_CREDIT_LIMIT=" + etr.getValue().split(",",-1)[2] 
								+ ":SUBSCRIBER_BALANCE=" + etr.getValue().split(",",-1)[3] + ":AVAILABLE_CREDIT_LIMIT=" + etr.getValue().split(",",-1)[4]  
								+ ":Notification_70=" + etr.getValue().split(",",-1)[6] + ":Notification_80=" + etr.getValue().split(",",-1)[7] 
								+ ":Notification_90=" + etr.getValue().split(",",-1)[8] + ":UsageCounter=" + etr.getValue().split(",",-1)[9] + 
								":ServiceClass=" + etr.getValue().split(",",-1)[1] + ":DESCRIPTION=MSISDN ALREADY MIGRATED:ACTION=DISCARD & LOG");
									
					}
					else
					{
					  boolean bREFERENCE_CREDIT_LIMIT = true; 
					  boolean bAVAILABLE_CREDIT_LIMIT= true;
					  boolean bSUBSCRIBER_BALANCE = true; 
					  boolean bMINPAY_FACTOR = true; 
					  boolean bNOTIFICATION_70 = true; 
					  boolean bNOTIFICATION_80 = true; 
					  boolean bNOTIFICATION_90 = true;
						 
						
						if(creditLimit.containsKey("REFERENCE_CREDIT_LIMIT") && creditLimit.get("REFERENCE_CREDIT_LIMIT").isEmpty())
						{	
							bREFERENCE_CREDIT_LIMIT = false;
							onlyLog.add("INC31:MSISDN=" + MSISDN + ":REFERENCE_CREDIT_LIMIT=" + creditLimit.get("REFERENCE_CREDIT_LIMIT") + ":DESCRIPTION=REFERENCE_CREDIT_LIMIT is zero or empty=DISCARD & LOG" );
						}
						
						if(creditLimit.containsKey("AVAILABLE_CREDIT_LIMIT") && creditLimit.get("AVAILABLE_CREDIT_LIMIT").isEmpty())
						{
							bAVAILABLE_CREDIT_LIMIT = false;
							onlyLog.add("INC31:MSISDN=" + MSISDN + ":AVAILABLE_CREDIT_LIMIT=" + creditLimit.get("AVAILABLE_CREDIT_LIMIT") + ":DESCRIPTION=AVAILABLE_CREDIT_LIMIT is zero or empty=DISCARD & LOG" );
						}
						
						if(creditLimit.containsKey("SUBSCRIBER_BALANCE") && creditLimit.get("SUBSCRIBER_BALANCE").isEmpty())
						{
							bSUBSCRIBER_BALANCE = false;
							onlyLog.add("INC31:MSISDN=" + MSISDN + ":SUBSCRIBER_BALANCE=" + creditLimit.get("SUBSCRIBER_BALANCE") + ":DESCRIPTION=SUBSCRIBER_BALANCE is zero or empty=DISCARD & LOG" );
						}
						
						if(creditLimit.containsKey("MINPAY_FACTOR") && creditLimit.get("MINPAY_FACTOR").isEmpty())
						{
							bMINPAY_FACTOR = false;
							onlyLog.add("INC31:MSISDN=" + MSISDN + ":MINPAY_FACTOR=" + creditLimit.get("MINPAY_FACTOR") + ":DESCRIPTION=MINPAY_FACTOR is zero or empty=DISCARD & LOG" );
						}
						
						if(creditLimit.containsKey("Notification_70") && creditLimit.get("Notification_70").isEmpty())
						{
							bNOTIFICATION_70 = false;
							onlyLog.add("INC31:MSISDN=" + MSISDN + ":NOTIFICATION_70=" + creditLimit.get("Notification_70") + ":DESCRIPTION=NOTIFICATION_70 is zero or empty=DISCARD & LOG" );
						}
						
					    if(creditLimit.containsKey("Notification_80") && creditLimit.get("Notification_80").isEmpty())
						{
					    	bNOTIFICATION_80 = false;
							onlyLog.add("INC31:MSISDN=" + MSISDN + ":NOTIFICATION_80=" + creditLimit.get("Notification_80") + ":DESCRIPTION=NOTIFICATION_80 is zero or empty=DISCARD & LOG" );
						}
					    
						if(creditLimit.containsKey("Notification_90") && creditLimit.get("Notification_90").isEmpty())
						{
							bNOTIFICATION_90 = false; 
							onlyLog.add("INC31:MSISDN=" + MSISDN + ":NOTIFICATION_90=" + creditLimit.get("Notification_90") + ":DESCRIPTION=NOTIFICATION_90 is zero or empty=DISCARD & LOG" );
						}
						
						//if(creditLimit.containsKey("REFERENCE_CREDIT_LIMIT") && creditLimit.get("REFERENCE_CREDIT_LIMIT").equals("99999999") && creditLimit.containsKey("AVAILABLE_CREDIT_LIMIT"))
						
						if(bAVAILABLE_CREDIT_LIMIT && bREFERENCE_CREDIT_LIMIT && creditLimit.get("REFERENCE_CREDIT_LIMIT").equals("99999999"))
						{
							//If UT_01 =99999999 then only populate UT_01 and UT03 no other columns need to be populated
							BigDecimal UCFactor = new BigDecimal(LoadSubscriberMapping.CommonConfigMapping.get("UC_FACTOR"));
							BigDecimal REFERENCE_CREDIT_LIMIT = new BigDecimal(creditLimit.get("REFERENCE_CREDIT_LIMIT"));
							BigDecimal AVAILABLE_CREDIT_LIMIT = new BigDecimal(creditLimit.get("AVAILABLE_CREDIT_LIMIT"));
							if(REFERENCE_CREDIT_LIMIT.compareTo(BigDecimal.ZERO) > 0)
								UTList.add(PopulateUsageThreshold(LoadSubscriberMapping.utMapping.get("REFERENCE_CREDIT_LIMIT").split("=")[1], MSISDN, String.valueOf(df.format(REFERENCE_CREDIT_LIMIT.multiply(UCFactor)))));
							else
								onlyLog.add("INC31:MSISDN=" + MSISDN + ":REFERENCE_CREDIT_LIMIT=" + creditLimit.get("REFERENCE_CREDIT_LIMIT") + ":DESCRIPTION=REFERENCE_CREDIT_LIMIT is zero or empty=DISCARD & LOG" );
							
							if(AVAILABLE_CREDIT_LIMIT.compareTo(BigDecimal.ZERO) > 0)
								UTList.add(PopulateUsageThreshold(LoadSubscriberMapping.utMapping.get("AVAILABLE_CREDIT_LIMIT").split("=")[1], MSISDN, String.valueOf(df.format(AVAILABLE_CREDIT_LIMIT.multiply(UCFactor)))));
							else
								onlyLog.add("INC31:MSISDN=" + MSISDN + ":REFERENCE_CREDIT_LIMIT=" + creditLimit.get("AVAILABLE_CREDIT_LIMIT") + ":DESCRIPTION=AVAILABLE_CREDIT_LIMIT is zero or empty=DISCARD & LOG" );
						}
						else if(bAVAILABLE_CREDIT_LIMIT && bREFERENCE_CREDIT_LIMIT && bSUBSCRIBER_BALANCE)
						{
							
							BigDecimal REFERENCE_CREDIT_LIMIT = new BigDecimal(creditLimit.get("REFERENCE_CREDIT_LIMIT"));
							BigDecimal AVAILABLE_CREDIT_LIMIT = new BigDecimal(creditLimit.get("AVAILABLE_CREDIT_LIMIT"));
							BigDecimal SUBSCRIBER_BALANCE = new BigDecimal(creditLimit.get("SUBSCRIBER_BALANCE"));
							if(!REFERENCE_CREDIT_LIMIT.subtract(AVAILABLE_CREDIT_LIMIT).equals(SUBSCRIBER_BALANCE) )
							{
								onlyLog.add("INC4000:MSISDN=" + MSISDN + ":REFERENCE_CREDIT_LIMIT=" + REFERENCE_CREDIT_LIMIT + ":AVAILABLE_CREDIT_LIMIT=" + AVAILABLE_CREDIT_LIMIT +":SUBSCRIBER_BALANCE=" + SUBSCRIBER_BALANCE + ":DESCRIPTION=Mismatch in Reference, Available and Subuscriber Credit limit=DISCARD & LOG" );
								//continue;
							}			
							//else 
							{
								//Else Values for UT01 to UT04 should always be present.If not log the record.				
								if(bMINPAY_FACTOR && !creditLimit.get("MINPAY_FACTOR").isEmpty())
								{
									BigDecimal MINPAY_FACTOR = new BigDecimal(creditLimit.get("MINPAY_FACTOR"));
									BigDecimal MinPayFactor = new BigDecimal(LoadSubscriberMapping.CommonConfigMapping.get("MIN_FACTOR"));
									if(MINPAY_FACTOR.compareTo(BigDecimal.ZERO) > 0)
										UTList.add(PopulateUsageThreshold(LoadSubscriberMapping.utMapping.get("MINPAY_FACTOR").split("=")[1], MSISDN, String.valueOf(df.format(MINPAY_FACTOR.multiply(MinPayFactor)))));
									else
										onlyLog.add("INC31:MSISDN=" + MSISDN + ":MINPAY_FACTOR=" + creditLimit.get("MINPAY_FACTOR") + ":DESCRIPTION=MINPAY_FACTOR is zero or empty=DISCARD & LOG" );
								}
								if(bREFERENCE_CREDIT_LIMIT)
								{
									BigDecimal UCFactor = new BigDecimal(LoadSubscriberMapping.CommonConfigMapping.get("UC_FACTOR"));
									if(REFERENCE_CREDIT_LIMIT.compareTo(BigDecimal.ZERO) > 0)
										UTList.add(PopulateUsageThreshold(LoadSubscriberMapping.utMapping.get("REFERENCE_CREDIT_LIMIT").split("=")[1], MSISDN, String.valueOf(df.format(REFERENCE_CREDIT_LIMIT.multiply(UCFactor)))));
									else
										onlyLog.add("INC31:MSISDN=" + MSISDN + ":REFERENCE_CREDIT_LIMIT=" + creditLimit.get("REFERENCE_CREDIT_LIMIT") + ":DESCRIPTION=REFERENCE_CREDIT_LIMIT is zero or empty=DISCARD & LOG" );
								}
								if(bAVAILABLE_CREDIT_LIMIT)
								{
									//if()
									BigDecimal UCFactor = new BigDecimal(LoadSubscriberMapping.CommonConfigMapping.get("UC_FACTOR"));
									if(AVAILABLE_CREDIT_LIMIT.compareTo(BigDecimal.ZERO) < 0.000 || String.valueOf(AVAILABLE_CREDIT_LIMIT).equals("0.000"))
									{
										//UTList.add(PopulateUsageThreshold(LoadSubscriberMapping.utMapping.get("SUBSCRIBER_BALANCE").split("=")[1], MSISDN, String.valueOf("0")));
										onlyLog.add("INC32:MSISDN=" + MSISDN + ":AVAILABLE_CREDIT_LIMIT=" + AVAILABLE_CREDIT_LIMIT + ":DESCRIPTION=SUBSCRIBER_BALANCE is negative=DISCARD & LOG" );
									}
									else
									{
										UTList.add(PopulateUsageThreshold(LoadSubscriberMapping.utMapping.get("AVAILABLE_CREDIT_LIMIT").split("=")[1], MSISDN, String.valueOf(df.format(AVAILABLE_CREDIT_LIMIT.multiply(UCFactor)))));
									}
								}
								if(bSUBSCRIBER_BALANCE)
								{
									BigDecimal UCFactor = new BigDecimal(LoadSubscriberMapping.CommonConfigMapping.get("UC_FACTOR"));
									if(SUBSCRIBER_BALANCE.compareTo(BigDecimal.ZERO) < 0.000 || String.valueOf(SUBSCRIBER_BALANCE).equals("0.000"))
									{
										//UTList.add(PopulateUsageThreshold(LoadSubscriberMapping.utMapping.get("SUBSCRIBER_BALANCE").split("=")[1], MSISDN, String.valueOf("0")));
										onlyLog.add("INC32:MSISDN=" + MSISDN + ":SUBSCRIBER_BALANCE=" + SUBSCRIBER_BALANCE + ":DESCRIPTION=SUBSCRIBER_BALANCE is negative=DISCARD & LOG" );
									}
									else
									{
										UTList.add(PopulateUsageThreshold(LoadSubscriberMapping.utMapping.get("SUBSCRIBER_BALANCE").split("=")[1], MSISDN, String.valueOf(df.format(SUBSCRIBER_BALANCE.multiply(UCFactor)))));
									}
								}			
							}		
						}
								
						//any one of the three columns UT05 to UT07 should be populated with value. Else log the record
				
						if(bNOTIFICATION_70 && !String.valueOf(creditLimit.get("Notification_70")).equals("0.000"))
						{
							BigDecimal UCFactor = new BigDecimal(LoadSubscriberMapping.CommonConfigMapping.get("UC_FACTOR"));
							BigDecimal NOTIFICATION_70 = new BigDecimal(creditLimit.get("Notification_70"));
							UTList.add(PopulateUsageThreshold(LoadSubscriberMapping.utMapping.get("NOTIFICATION_70").split("=")[1], MSISDN, String.valueOf(df.format(NOTIFICATION_70.multiply(UCFactor)))));
						}
						if(bNOTIFICATION_80 && !String.valueOf(creditLimit.get("Notification_80")).equals("0.000"))
						{
							BigDecimal NOTIFICATION_80 = new BigDecimal(creditLimit.get("Notification_80"));
							BigDecimal UCFactor = new BigDecimal(LoadSubscriberMapping.CommonConfigMapping.get("UC_FACTOR"));
							UTList.add(PopulateUsageThreshold(LoadSubscriberMapping.utMapping.get("NOTIFICATION_80").split("=")[1], MSISDN, String.valueOf(df.format(NOTIFICATION_80.multiply(UCFactor)))));
						}
						if(bNOTIFICATION_90 && !String.valueOf(creditLimit.get("Notification_90")).equals("0.000"))
						{
							BigDecimal NOTIFICATION_90 = new BigDecimal(creditLimit.get("Notification_90"));
							BigDecimal UCFactor = new BigDecimal(LoadSubscriberMapping.CommonConfigMapping.get("UC_FACTOR"));
							UTList.add(PopulateUsageThreshold(LoadSubscriberMapping.utMapping.get("NOTIFICATION_90").split("=")[1], MSISDN, String.valueOf(df.format(NOTIFICATION_90.multiply(UCFactor)))));
						}
					}
				}				
			}
			else
			{
				rejectAndLog.add("INC02:MSISDN=" + etr.getValue().split(",",-1)[0] + ":REFERENCE_CREDIT_LIMIT=" + etr.getValue().split(",",-1)[2] 
					+ ":SUBSCRIBER_BALANCE=" + etr.getValue().split(",",-1)[3] + ":AVAILABLE_CREDIT_LIMIT=" + etr.getValue().split(",",-1)[4]  
					+ ":Notification_70=" + etr.getValue().split(",",-1)[6] + ":Notification_80=" + etr.getValue().split(",",-1)[7] 
					+ ":Notification_90=" + etr.getValue().split(",",-1)[8] + ":UsageCounter=" + etr.getValue().split(",",-1)[9] + 
					":ServiceClass=" + etr.getValue().split(",",-1)[1] + ":DESCRIPTION=MSISDN NOT PRESENT IN CS:ACTION=DISCARD & LOG");
				
				//Now check if MSISDN is present in main offer, if so just log it
				if(LoadSubscriberMapping.mainOfferInputRelation.containsKey(etr.getValue().split(",",-1)[0]))
				{
					Set<String> MainOffer = new HashSet<String>();
					MainOffer.addAll(LoadSubscriberMapping.mainOfferInputRelation.get(etr.getValue().split(",",-1)[0]));
					for (String str : MainOffer)
					{
						String OfferID = str.split(",",-1)[1];
						String UsageCounter = str.split(",",-1)[4];
						String UsageThreshold = str.split(",",-1)[5];
						rejectAndLog.add("INC01:MSISDN=" + etr.getValue().split(",",-1)[0] + ":OFFER_UV_UT=" +OfferID+"-" + UsageCounter +"-" + UsageThreshold + ":DESCRIPTION=MSISDN NOT PRESENT IN CS:ACTION=DISCARD & LOG");
					}
				}
				/*if(LoadSubscriberMapping.Addon_OfferExtraInput.containsKey(etr.getValue().split(",",-1)[0]))
				{
					Set<String> MainOffer = new HashSet<String>();
					MainOffer.addAll(LoadSubscriberMapping.Addon_OfferExtraInput.get(etr.getValue().split(",",-1)[0]));
					for (String str : MainOffer)
					{
						String OfferID = str.split(",",-1)[1];
						String UsageCounter = str.split(",",-1)[4];
						String UsageThreshold = str.split(",",-1)[5];
						rejectAndLog.add("INC01:MSISDN=" + etr.getValue().split(",",-1)[0] + ":OFFER_UV_UT=" +OfferID+"-" + UsageCounter +"-" + UsageThreshold + ":DESCRIPTION=MSISDN NOT PRESENT IN CS:ACTION=DISCARD & LOG");
					}
				}*/
				
			}
		}	
		return UTList;
	}
	
	//&&&&&&&&&&&&&&&&&&&&&&&&Added code to Phase1...For extra records&&&&&&&&&&&&&&&&&
	
	public List<String> executeAdditionalRecord() {
		// TODO Auto-generated method stub
		
		List<String> UTList = new ArrayList<>();
		UTList.addAll(GenerateExtraImplicitOfferFile());
		UTList.addAll(GenerateExtraMainUTFromAddonFile());
		UTList.addAll(GenerateExtraSpecialUTFromAddonFile());
		return UTList;
	}
	
	private Collection<? extends String> GenerateExtraSpecialUTFromAddonFile() {
		// TODO Auto-generated method stub
		List<String> UTValue = new ArrayList<String>();
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date dateobj = new Date();
		for(Map.Entry<String, Set<String>> entry : LoadSubscriberMapping.Addon_OfferExtraInput.entrySet())
		{
			//if(entry.getKey().equals("36478999"))
			//	System.out.println("Vipin SIngh ");
			if(!LoadSubscriberMapping.CompleteListToMigrate.contains(entry.getKey()))
			{
								
			}
			else
			{
				if(LoadSubscriberMapping.AlreadyMigratedMsisdn.contains(entry.getKey()))
				{
											
				}
				else if(!CompletedMSISDN.contains(entry.getKey()))
				{
					Map<String,String> AddonInput = new HashMap<String,String>();
					
					for(String str: entry.getValue())
					{
						String OfferID = str.split(",",-1)[1];
						String SequenceID = str.split(",",-1)[6];
						AddonInput.put(OfferID + ";" + SequenceID, str);
					}
					TreeSet<String> sortedAddonKey = new TreeSet<String>(AddonInput.keySet());
					
					for(String entryKey  : sortedAddonKey)
					{
						String OfferID = entryKey.split(";")[0];
						String MSISDN = AddonInput.get(entryKey).split(",")[0];
						String End_Date_Time = AddonInput.get(entryKey).split(",")[3];
						long ExpiryCheck = CommonUtilities.convertDateToEpoch(CommonUtilities.getEndOfDay(End_Date_Time));
						long curentDate = CommonUtilities.convertDateToEpoch(CommonUtilities.getEndOfDay(df.format(dateobj))) ;
						if(ExpiryCheck < curentDate)
						{
							
							continue;
						}
						else
						{
							int Quantity = Integer.parseInt(AddonInput.get(entryKey).split(",")[7]);
							
							if(LoadSubscriberMapping.SpecialUTMapping.containsKey(OfferID) && (Quantity) > 0)
							{
								String specialUT = LoadSubscriberMapping.SpecialUTMapping.get(OfferID);
								String UTID = specialUT.split(",")[0].split("=")[1];
								String UT_Value = String.valueOf(Long.parseLong(specialUT.split(",")[1].split("_")[1]) * (Quantity));
								UTValue.add(PopulateUsageThreshold(UTID, MSISDN, UT_Value));
								break;
							}
						}
					}
				}				
			}			
		}
		
		return UTValue;
	}
	
	private Collection<? extends String> GenerateExtraMainUTFromAddonFile() {
		// TODO Auto-generated method stub
		List<String> UTValue = new ArrayList<String>();
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
							continue;
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
							
							HashSet<String> DuplicateUTpresent = new HashSet<String>();
							if(AvoidDuplicateUT.size() > 0 &&  AvoidDuplicateUT.containsKey(MSISDN))
								DuplicateUTpresent.addAll(AvoidDuplicateUT.get(MSISDN));
							if(!DuplicateUTpresent.contains(Offer_ID))
							{
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
									
									
									if(hMapData.get("Multi_Benefit").equals("Y"))
									{
										if(hMapData.get("Multi_Purchase_Offer").equals("Y"))
										{
											if(hMapData.get("Main_Offer_PP").equals("Y"))
											{
												if(hMapData.get("Implicit_Offer_Enable_UT").equals("Y"))
												{
													UTValue.add(PopulateUsageThreshold(Offer_ID, MSISDN, Usage_Threshold));
												}
												if(hMapData.get("Main_Offer_Enable_UT").equals("Y"))
												{
													UTValue.add(PopulateUsageThreshold(hMapData.get("Main_offer_id"), MSISDN, Usage_Threshold));
												}									
											}
											else if(hMapData.get("Main_Offer_PP").equals("N"))
											{
												if(hMapData.get("Implicit_Offer_Enable_UT").equals("Y"))
												{
													UTValue.add(PopulateUsageThreshold(Offer_ID, MSISDN, Usage_Threshold));
												}
												if(hMapData.get("Main_Offer_Enable_UT").equals("Y"))
												{
													UTValue.add(PopulateUsageThreshold(hMapData.get("Main_offer_id"), MSISDN, Usage_Threshold));
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
												if(hMapData.get("Implicit_Offer_Enable_UT").equals("Y"))
												{
													UTValue.add(PopulateUsageThreshold(Offer_ID, MSISDN, Usage_Threshold));
												}
												if(hMapData.get("Main_Offer_Enable_UT").equals("Y"))
												{
													UTValue.add(PopulateUsageThreshold(hMapData.get("Main_offer_id"), MSISDN, Usage_Threshold));
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
												if(hMapData.get("Implicit_Offer_Enable_UT").equals("Y"))
												{
													UTValue.add(PopulateUsageThreshold(Offer_ID, MSISDN, Usage_Threshold));
												}
												if(hMapData.get("Main_Offer_Enable_UT").equals("Y"))
												{
													UTValue.add(PopulateUsageThreshold(hMapData.get("Main_offer_id"), MSISDN, Usage_Threshold));
												}
											}
											else if(hMapData.get("Main_Offer_PP").equals("N"))
											{
												if(hMapData.get("Implicit_Offer_Enable_UT").equals("Y"))
												{
													UTValue.add(PopulateUsageThreshold(Offer_ID, MSISDN, Usage_Threshold));
												}
												if(hMapData.get("Main_Offer_Enable_UT").equals("Y"))
												{
													UTValue.add(PopulateUsageThreshold(hMapData.get("Main_offer_id"), MSISDN, Usage_Threshold));
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
												if(hMapData.get("Implicit_Offer_Enable_UT").equals("Y"))
												{
													UTValue.add(PopulateUsageThreshold(Offer_ID, MSISDN, Usage_Threshold));
												}
												if(hMapData.get("Main_Offer_Enable_UT").equals("Y"))
												{
													UTValue.add(PopulateUsageThreshold(Offer_ID, MSISDN, Usage_Threshold));
												}
											}
										}
									}
								}
							}
							else
							{
								DuplicateUTLog.add("INC55:MSISDN=" + MSISDN  + ":OFFER_UV_UT=" + Offer_ID+"-" + Usage_Value +"-" + Usage_Value + ":DESCRIPTION=Duplicate UsageThreshold is rejected=DISCARD & LOG");
							}
						}
					}
				}
			}
		}
		return UTValue;
	}
	
	private Collection<? extends String> GenerateExtraImplicitOfferFile() {
		// TODO Auto-generated method stub
		List<String> UTList = new ArrayList<>();
		
		for(Map.Entry<String, Set<String>> entry : LoadSubscriberMapping.Implicit_Shared_OfferExtraInput.entrySet())
		{
			if(!LoadSubscriberMapping.CompleteListToMigrate.contains(entry.getKey()))
			{
				for(String str: entry.getValue())
				{
					//System.out.println(str);					
				}				
			}
			else
			{
				if(LoadSubscriberMapping.AlreadyMigratedMsisdn.contains(entry.getKey()))
				{
										
				}
				else if(!CompletedMSISDN.contains(entry.getKey()))
				{
					Set<String> InputSet = new HashSet<String>(entry.getValue());
					for(String str : InputSet)
					{
						String MSISDN = str.split(",",-1)[0];
						String Shared_Offer_ID = str.split(",",-1)[1];
						String Usage_Threshold = str.split(",",-1)[5];
						String Usage_Value = str.split(",",-1)[4];
						String Start_Date = str.split(",",-1)[2];;
						String End_Date =str.split(",",-1)[3];
						
						if(Shared_Offer_ID.length() > 0 && LoadSubscriberMapping.sharedOfferMapping.containsKey((Shared_Offer_ID)) 
								&& ! Usage_Threshold.isEmpty() && Integer.valueOf(Usage_Threshold) > 0)
						{
							String UTFlag = LoadSubscriberMapping.sharedOfferMapping.get(Shared_Offer_ID).split(",")[1].split("=")[1];
							if(UTFlag.equals("Y"))
								UTList.add(PopulateUsageThreshold(Shared_Offer_ID, MSISDN, Usage_Threshold));
							else
								onlyLog.add("INC12:MSISDN=" + MSISDN + ":OFFER_ID=" + Shared_Offer_ID +  ":UC_Value=" + Usage_Value +":UC_Threshold=" + Usage_Threshold + ":DESCRIPTION=Offer OUT OF SCOPE:ACTION=DISCARD & LOG" );
						}
					}
				}
			}
		}		
		return UTList;
	}
}
