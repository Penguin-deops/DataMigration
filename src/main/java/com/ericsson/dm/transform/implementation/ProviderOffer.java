package com.ericsson.dm.transform.implementation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ericsson.dm.inititialization.LoadSubscriberMapping;
import com.ericsson.dm.transformation.JsonInputFields;
import com.ericsson.dm.transformation.JsonInputFields.SHARED_OFFER_RELATION;

public class ProviderOffer {
	Set<String> rejectAndLog;
	Set<String> onlyLog;
	JsonInputFields inputObj;
	CommonFunctions commonfunction;
	Set<String> CompletedMSISDN;
	
	public ProviderOffer( Set<String> rejectAndLog, Set<String> onlyLog,Set<String> CompletedMSISDN, Map<String, String> ProductIDLookUpMap) {
		this.rejectAndLog=rejectAndLog;
		this.onlyLog=onlyLog;
		this.CompletedMSISDN = CompletedMSISDN;
		this.commonfunction = new CommonFunctions();
		// TODO Auto-generated constructor stub
	}
	
	public ProviderOffer( Set<String> rejectAndLog, Set<String> onlyLog,JsonInputFields inputObj, Map<String, String> ProductIDLookUpMap) {
		this.rejectAndLog=rejectAndLog;
		this.onlyLog=onlyLog;
		this.inputObj = inputObj;
		this.commonfunction = new CommonFunctions();
		// TODO Auto-generated constructor stub
	}
	
	public List<String> execute() {
		// TODO Auto-generated method stub
		List<String> POList = new ArrayList<>();
		POList.addAll(GenerateFromProviderOffer());
		//UCList.addAll(GenerateFromAddonUC());
		return POList;

	}	
	
	private Collection<? extends String> GenerateFromProviderOffer() {
		// TODO Auto-generated method stub
		
		List<String> POList = new ArrayList<>();
		for(Map.Entry<String,Object> entry : inputObj.SHARED_OFFER_RELATION.entrySet())
		{
			SHARED_OFFER_RELATION mo = inputObj.new SHARED_OFFER_RELATION();
			mo = (SHARED_OFFER_RELATION)entry.getValue();
			
			if(LoadSubscriberMapping.ParentChildRejectionMSISDN.containsKey(inputObj.MSISDN) && LoadSubscriberMapping.sharedInputRelationMSISDN.contains(inputObj.MSISDN) && LoadSubscriberMapping.ParentChildRejectionMSISDN.get(inputObj.MSISDN) != null)
			{
				onlyLog.add("INC12:MSISDN=" + inputObj.MSISDN + ":OFFER_ID=" + mo.Provider_Offer_ID + ":UC_Value=0" +":UC_Threshold=0" + ":DESCRIPTION=Offer OUT OF SCOPE:ACTION=DISCARD & LOG" );
			}
			else
			{			
				if(mo.Provider_Offer_ID.length() > 0 && LoadSubscriberMapping.providerOfferMapping.containsKey((mo.Provider_Offer_ID)))
				{
					String poFlag = LoadSubscriberMapping.providerOfferMapping.get(mo.Provider_Offer_ID).split("=")[1];
					if(poFlag.equals("Y"))
						POList.add(PopulateProviderOffer(mo.Provider_Offer_ID, inputObj.MSISDN, mo.Provider_Msisdn));
					else
						onlyLog.add("INC12:MSISDN=" + inputObj.MSISDN + ":OFFER_ID=" + mo.Provider_Offer_ID + ":UC_Value=" +":UC_Threshold=" + ":DESCRIPTION=Offer OUT OF SCOPE:ACTION=DISCARD & LOG" );
				}
			}
		}	
		return POList;		
	}

	private String PopulateProviderOffer(String provider_Offer_ID, String mSISDN, String proMSISDN) {
		// TODO Auto-generated method stub
		StringBuilder sb = new StringBuilder();
		
		sb.append(mSISDN).append(",");
		sb.append(provider_Offer_ID).append(",");
		//calculate providerID
		String ProviderVersion = LoadSubscriberMapping.CommonConfigMapping.get("ProviderVersion");
		String lenProMsisdn = String.valueOf(proMSISDN.length());
		String providerID = pad(ProviderVersion + lenProMsisdn + proMSISDN, 32, '0');
		sb.append(providerID);
		
		return sb.toString();
	}
	
	public String pad(String str, int size, char padChar)
	{
		StringBuffer padded = new StringBuffer(str);
		  
		while (padded.length() < size)
		{
			padded.append(padChar);
		}
		
		return padded.toString();
	}
	
	//&&&&&&&&&&&&&&&&&&&&&&&&Added code to Phase1...For extra records&&&&&&&&&&&&&&&&& 
	
	public List<String> executeAdditionalRecord() {
		// TODO Auto-generated method stub
		List<String> POList = new ArrayList<>();
		POList.addAll(GenerateFromProviderOfferAdditionalRecord());
		//UCList.addAll(GenerateFromAddonUC());
		return POList;

	}
	
	private Collection<? extends String> GenerateFromProviderOfferAdditionalRecord() {
		// TODO Auto-generated method stub
		
		List<String> POList = new ArrayList<>();
		
		for(Map.Entry<String, Set<String>> entry : LoadSubscriberMapping.Shared_Offer_RelationExtraInput.entrySet())
		{
			//if(entry.getKey().equals("10012524"))
			//	System.out.println("Vipin Singh");
			
			//if(entry.getKey().equals("33112442"))
			//	System.out.println("singh");
			if(!LoadSubscriberMapping.CompleteListToMigrate.contains(entry.getKey()))
			{
				for(String str: entry.getValue())
				{
					//System.out.println(str);
					String OfferID = str.split(",",-1)[1];
					String UsageCounter = "";
					String UsageThreshold = "";
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
							//System.out.println(str);
							String OfferID = str.split(",",-1)[1];
							String UsageCounter = "";
							String UsageThreshold = "";
							rejectAndLog.add("INC01:MSISDN=" + entry.getKey() + ":OFFER_UV_UT=" +OfferID+"-" + UsageCounter +"-" + UsageThreshold + ":DESCRIPTION=MSISDN ALREADY MIGRATED:ACTION=DISCARD & LOG");
						}						
					}
					else
					{
						Set<String> InputSet = new HashSet<String>(entry.getValue());
						for(String str : InputSet)
						{
							String MSISDN = str.split(",",-1)[0];
							String Provider_Offer_ID = str.split(",",-1)[1];
							String Provider_Msisdn = str.split(",",-1)[2];
							
							if(!LoadSubscriberMapping.CompleteListToMigrate.contains(Provider_Msisdn))
							{
								/*for(String str: entry.getValue())
								{
									//System.out.println(str);
									String OfferID = str.split(",",-1)[1];
									String UsageCounter = "";
									String UsageThreshold = "";
									rejectAndLog.add("INC01:MSISDN=" + entry.getKey() + ":OFFER_UV_UT=" +OfferID+"-" + UsageCounter +"-" + UsageThreshold + ":DESCRIPTION=MSISDN NOT PRESENT IN CS:ACTION=DISCARD & LOG");
								}*/
								onlyLog.add("INC12:MSISDN=" + MSISDN + ":OFFER_ID=" + Provider_Offer_ID + ":UC_Value=" +":UC_Threshold=" + ":DESCRIPTION=Offer OUT OF SCOPE:ACTION=DISCARD & LOG" );
							}
							else
							{						
								if(LoadSubscriberMapping.ParentChildRejectionMSISDN.containsKey(MSISDN) && LoadSubscriberMapping.sharedInputRelationMSISDN.contains(MSISDN) && LoadSubscriberMapping.ParentChildRejectionMSISDN.get(MSISDN) != null)
								{
									onlyLog.add("INC12:MSISDN=" + MSISDN + ":OFFER_ID=" + Provider_Offer_ID + ":UC_Value=0" +":UC_Threshold=0" + ":DESCRIPTION=Offer OUT OF SCOPE:ACTION=DISCARD & LOG" );
								}
								else
								{			
									if(Provider_Offer_ID.length() > 0 && LoadSubscriberMapping.providerOfferMapping.containsKey((Provider_Offer_ID)))
									{
										String poFlag = LoadSubscriberMapping.providerOfferMapping.get(Provider_Offer_ID).split("=")[1];
										if(poFlag.equals("Y"))
											POList.add(PopulateProviderOffer(Provider_Offer_ID, MSISDN, Provider_Msisdn));
										else
											onlyLog.add("INC12:MSISDN=" + MSISDN + ":OFFER_ID=" + Provider_Offer_ID + ":UC_Value=" +":UC_Threshold=" + ":DESCRIPTION=Offer OUT OF SCOPE:ACTION=DISCARD & LOG" );
									}
								}
							}
						}
					}
				}				
			}
		}
		
		return POList;		
	}
	
}
