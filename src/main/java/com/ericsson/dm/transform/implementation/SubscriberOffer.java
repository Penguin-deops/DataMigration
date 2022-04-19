package com.ericsson.dm.transform.implementation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ericsson.dm.Utils.CommonUtilities;
import com.ericsson.dm.inititialization.LoadSubscriberMapping;
import com.ericsson.dm.transformation.JsonInputFields;
import com.ericsson.dm.transformation.JsonInputFields.IMPLICIT_SHARED_OFFER;
import com.ericsson.dm.transformation.JsonInputFields.SHARED_OFFER_RELATION;

public class SubscriberOffer {
	Set<String> rejectAndLog;
	Set<String> onlyLog;
	JsonInputFields inputObj;
	CommonFunctions commonfunction;
	Set<String> CompletedMSISDN;
	
	public SubscriberOffer( Set<String> rejectAndLog, Set<String> onlyLog,Set<String> CompletedMSISDN, Map<String, String> ProductIDLookUpMap) {
		this.rejectAndLog=rejectAndLog;
		this.onlyLog=onlyLog;
		this.CompletedMSISDN = CompletedMSISDN;
		this.commonfunction = new CommonFunctions();
		// TODO Auto-generated constructor stub
	}
	
	public SubscriberOffer( Set<String> rejectAndLog, Set<String> onlyLog,JsonInputFields inputObj, Map<String, String> ProductIDLookUpMap) {
		this.rejectAndLog=rejectAndLog;
		this.onlyLog=onlyLog;
		this.inputObj = inputObj;
		this.commonfunction = new CommonFunctions();
		// TODO Auto-generated constructor stub
	}
	
	public List<String> execute() {
		// TODO Auto-generated method stub
		List<String> SOList = new ArrayList<>();
		SOList.addAll(GenerateFromImplicitOffer());
		return SOList;
			
	}
	
	private Collection<? extends String> GenerateFromImplicitOffer() {
		// TODO Auto-generated method stub
		Set<String> SOList = new HashSet<>();
		for(Map.Entry<String,Object> entry : inputObj.IMPLICIT_SHARED_OFFER.entrySet())
		{
			IMPLICIT_SHARED_OFFER mo = inputObj.new IMPLICIT_SHARED_OFFER();
			mo = (IMPLICIT_SHARED_OFFER)entry.getValue();
			
			if(mo.Shared_Offer_ID.length() > 0 && LoadSubscriberMapping.sharedOfferMapping.containsKey((mo.Shared_Offer_ID)) 
					&& !mo.Usage_Threshold.isEmpty() && Integer.valueOf(mo.Usage_Threshold) > 0)
			{
				String SOFlag = LoadSubscriberMapping.sharedOfferMapping.get(mo.Shared_Offer_ID).split(",")[0].split("=")[1];
				if(SOFlag.equals("Y"))
				{
					//check corresponding providerID from SharedProviderOffer_mapping
					if(LoadSubscriberMapping.sharedProviderOfferMapping.containsKey(mo.Shared_Offer_ID))
					{
						String ProviderID = LoadSubscriberMapping.sharedProviderOfferMapping.get(mo.Shared_Offer_ID);
						if(LoadSubscriberMapping.sharedInputRelation.containsKey(inputObj.MSISDN))
						{
							Set<String> relations = new HashSet<String>(LoadSubscriberMapping.sharedInputRelation.get(inputObj.MSISDN));
							for(String s : relations) {
								List<String> parentOfferList = Arrays.asList(s.split(","));
								if(parentOfferList.contains(ProviderID))
								{
									//Create Shared_Offer_Relation.csv //parentOfferList.get(0)
								    if(LoadSubscriberMapping.CompleteListToMigrate.contains(parentOfferList.get(0)))
								    {
								    	if(LoadSubscriberMapping.ParentChildRejectionMSISDN.containsKey(parentOfferList.get(0)) && LoadSubscriberMapping.sharedInputRelationMSISDN.contains(parentOfferList.get(0)) && LoadSubscriberMapping.ParentChildRejectionMSISDN.get(parentOfferList.get(0)) != null)
										{
								    		onlyLog.add("INC12:MSISDN=" + parentOfferList.get(0) + ":OFFER_ID=" + ProviderID + ":UC_Value=0" +":UC_Threshold=0" + ":DESCRIPTION=Offer OUT OF SCOPE:ACTION=DISCARD & LOG" );
										}
								    	else
								    	{
									    	SOList.add(PopulateSharedOffer(ProviderID,parentOfferList.get(0) , mo.Start_Date , mo.End_Date));
										}
								    	//Create IMPLICIT_SHARED_OFFER.csv 
								    	if(LoadSubscriberMapping.ParentChildRejectionMSISDN.containsKey(inputObj.MSISDN) && LoadSubscriberMapping.sharedInputRelationMSISDN.contains(inputObj.MSISDN) && LoadSubscriberMapping.ParentChildRejectionMSISDN.get(inputObj.MSISDN) != null)
										{
								    		onlyLog.add("INC12:MSISDN=" + inputObj.MSISDN + ":OFFER_ID=" + mo.Shared_Offer_ID + ":UC_Value=0" +":UC_Threshold=0" + ":DESCRIPTION=Offer OUT OF SCOPE:ACTION=DISCARD & LOG" );
										}
								    	else
								    	{									    	
											SOList.add(PopulateSharedOffer(mo.Shared_Offer_ID, inputObj.MSISDN, mo.Start_Date , mo.End_Date));
										}
										
								    }
								    else if(inputObj.MSISDN.startsWith("100"))
								    {
								    	if(LoadSubscriberMapping.ParentChildRejectionMSISDN.containsKey(inputObj.MSISDN) && LoadSubscriberMapping.sharedInputRelationMSISDN.contains(inputObj.MSISDN) && LoadSubscriberMapping.ParentChildRejectionMSISDN.get(inputObj.MSISDN) != null)
										{
								    		onlyLog.add("INC12:MSISDN=" + inputObj.MSISDN + ":OFFER_ID=" + mo.Shared_Offer_ID + ":UC_Value=0" +":UC_Threshold=0" + ":DESCRIPTION=Offer OUT OF SCOPE:ACTION=DISCARD & LOG" );
										}
								    	else
								    	{									    	
											SOList.add(PopulateSharedOffer(mo.Shared_Offer_ID, inputObj.MSISDN, mo.Start_Date , mo.End_Date));
										}
								    }
								    //else
								    	//onlyLog.add("INC22:MSISDN=" + inputObj.MSISDN + ":OFFER_ID=" + mo.Shared_Offer_ID +  ":UC_Value=" + mo.Usage_Value +":UC_Threshold=" + mo.Usage_Threshold + ":DESCRIPTION=SHARED PARENT OFFER WITHOUT CHILD OFFER:ACTION=DISCARD & LOG" );
								}
							}
						}
					}					
				}
				else
					//onlyLog.add("INC12:MSISDN=" + inputObj.MSISDN + ":OFFER_ID=" + ":UC_Value=" + mo.Usage_Value +":UC_Threshold=" + mo.Usage_Threshold + mo.Offer_ID + ":DESCRIPTION=Offer OUT OF SCOPE:ACTION=DISCARD & LOG" );
					onlyLog.add("INC12:MSISDN=" + inputObj.MSISDN + ":OFFER_ID=" + mo.Shared_Offer_ID +  ":UC_Value=" + mo.Usage_Value +":UC_Threshold=" + mo.Usage_Threshold + ":DESCRIPTION=Offer OUT OF SCOPE:ACTION=DISCARD & LOG" );
			}
		}	
		return SOList;
	}

	private String PopulateSharedOffer(String shared_Offer_ID, String mSISDN, String start_date, String expiry_date) {
		// TODO Auto-generated method stub
		StringBuilder sb = new StringBuilder();
	
		String Offer_Startdate = "";
		String Offer_StartSec = "";
		String Offer_Expirydate = "";
		String Offer_ExpirySec = "";
		
		if(!start_date.isEmpty() )
		{
			Offer_Startdate = CommonUtilities.convertDateToTimerOfferDate(start_date + " 00:00:00")[0].toString();
			Offer_StartSec = CommonUtilities.convertDateToTimerOfferDate(start_date + " 00:00:00")[1].toString();
		}
		else
		{
			Offer_Startdate = LoadSubscriberMapping.CommonConfigMapping.get("Default_0").toString();
			Offer_StartSec = LoadSubscriberMapping.CommonConfigMapping.get("Default_0").toString();
		}
		
		if(!expiry_date.isEmpty() )
		{
			Offer_Expirydate = CommonUtilities.convertDateToTimerOfferDate(expiry_date + " 00:00:00")[0].toString(); 
			Offer_ExpirySec = CommonUtilities.convertDateToTimerOfferDate(expiry_date + " 00:00:00")[1].toString();
		}
		else
		{	Offer_Expirydate = LoadSubscriberMapping.CommonConfigMapping.get("Default_0").toString();
			Offer_ExpirySec = LoadSubscriberMapping.CommonConfigMapping.get("Default_0").toString();
		}
		
		sb.append(mSISDN).append(",");
		sb.append(shared_Offer_ID).append(",");
		sb.append(Offer_Startdate).append(",");
		sb.append(Offer_Expirydate).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_NULL")).append(",");
		sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_0")).append(",");
		sb.append(Offer_StartSec).append(",");
		sb.append(Offer_ExpirySec);
		
		return sb.toString();
	}	

	//&&&&&&&&&&&&&&&&&&&&&&&&Added code to Phase1...For extra records&&&&&&&&&&&&&&&&&
	
	public List<String> executeAdditionalRecord() {
		// TODO Auto-generated method stub
		List<String> SOList = new ArrayList<>();
		SOList.addAll(GenerateFromImplicitOfferAdditionalRecord());
		return SOList;
			
	}
	
	private Collection<? extends String> GenerateFromImplicitOfferAdditionalRecord() {
		// TODO Auto-generated method stub
		Set<String> SOList = new HashSet<>();
		
		for(Map.Entry<String, Set<String>> entry : LoadSubscriberMapping.Implicit_Shared_OfferExtraInput.entrySet())
		{
			//if(entry.getKey().equals("10006603"))
			//	System.out.println("Vipin");
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
								&& !Usage_Threshold.isEmpty() && Integer.valueOf(Usage_Threshold) > 0)
						{
							String SOFlag = LoadSubscriberMapping.sharedOfferMapping.get(Shared_Offer_ID).split(",")[0].split("=")[1];
							if(SOFlag.equals("Y"))
							{
								//check corresponding providerID from SharedProviderOffer_mapping
								if(LoadSubscriberMapping.sharedProviderOfferMapping.containsKey(Shared_Offer_ID))
								{
									String ProviderID = LoadSubscriberMapping.sharedProviderOfferMapping.get(Shared_Offer_ID);
									if(LoadSubscriberMapping.sharedInputRelation.containsKey(MSISDN))
									{
										Set<String> relations = new HashSet<String>(LoadSubscriberMapping.sharedInputRelation.get(MSISDN));
										for(String s : relations) {
											List<String> parentOfferList = Arrays.asList(s.split(","));
											if(parentOfferList.contains(ProviderID))
											{
												//Create Shared_Offer_Relation.csv //parentOfferList.get(0)
											    if(LoadSubscriberMapping.CompleteListToMigrate.contains(parentOfferList.get(0)))
											    {
											    	if(LoadSubscriberMapping.ParentChildRejectionMSISDN.containsKey(parentOfferList.get(0)) && LoadSubscriberMapping.sharedInputRelationMSISDN.contains(parentOfferList.get(0)) && LoadSubscriberMapping.ParentChildRejectionMSISDN.get(parentOfferList.get(0)) != null)
													{
											    		onlyLog.add("INC12:MSISDN=" + parentOfferList.get(0) + ":OFFER_ID=" + ProviderID + ":UC_Value=0" +":UC_Threshold=0" + ":DESCRIPTION=Offer OUT OF SCOPE:ACTION=DISCARD & LOG" );
													}
											    	else
											    	{
												    	SOList.add(PopulateSharedOffer(ProviderID,parentOfferList.get(0) , Start_Date , End_Date));
													}
											    	//Create IMPLICIT_SHARED_OFFER.csv 
													if(LoadSubscriberMapping.ParentChildRejectionMSISDN.containsKey(MSISDN) && LoadSubscriberMapping.sharedInputRelationMSISDN.contains(MSISDN) && LoadSubscriberMapping.ParentChildRejectionMSISDN.get(MSISDN) != null)
													{
											    		onlyLog.add("INC12:MSISDN=" + MSISDN + ":OFFER_ID=" + Shared_Offer_ID + ":UC_Value=0" +":UC_Threshold=0" + ":DESCRIPTION=Offer OUT OF SCOPE:ACTION=DISCARD & LOG" );
													}
											    	else
											    	{									    	
														SOList.add(PopulateSharedOffer(Shared_Offer_ID, MSISDN, Start_Date , End_Date));
													}
											    }
											    else
											    {
											    	rejectAndLog.add("INC01:MSISDN=" + parentOfferList.get(0) + ":OFFER_UV_UT=" +ProviderID+"-0-0" + ":DESCRIPTION=MSISDN NOT PRESENT IN CS:ACTION=DISCARD & LOG");
											    }
											}
										}
									}
									else
									{
										SOList.add(PopulateSharedOffer(Shared_Offer_ID, MSISDN, Start_Date , End_Date));
									}
								}					
							}
							else
								//onlyLog.add("INC12:MSISDN=" + inputObj.MSISDN + ":OFFER_ID=" + ":UC_Value=" + mo.Usage_Value +":UC_Threshold=" + mo.Usage_Threshold + mo.Offer_ID + ":DESCRIPTION=Offer OUT OF SCOPE:ACTION=DISCARD & LOG" );
								onlyLog.add("INC12:MSISDN=" + MSISDN + ":OFFER_ID=" + Shared_Offer_ID +  ":UC_Value=" + Usage_Value +":UC_Threshold=" + Usage_Threshold + ":DESCRIPTION=Offer OUT OF SCOPE:ACTION=DISCARD & LOG" );
						}
					}
				}
			}
		}		
		
		return SOList;
	}
}
