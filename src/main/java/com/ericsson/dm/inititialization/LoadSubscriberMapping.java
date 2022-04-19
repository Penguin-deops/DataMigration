package com.ericsson.dm.inititialization;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class LoadSubscriberMapping {
	String migToolPath;
	String JsonFileName;
	String configPath;
	String inputPath;
		
	final static Logger LOG = Logger.getLogger(LoadSubscriberMapping.class);
	public static Random rand = new Random();
	
	
	public static final Map<String, String> LanguageMap = new ConcurrentHashMap<>(50, 0.75f, 30);
	
	public static final Map<String, String> CommonConfigMapping = new ConcurrentHashMap<>(100, 0.75f, 30);
	public static final Set<Integer> timerOfferMapping = new HashSet<Integer>();
	public static final Set<Integer> implicitOfferMapping = new HashSet<Integer>();
	public static final Map<String, String> multiPurposeOfferMapping = new HashMap<String, String>();	
	public static final Set<String> multiPurposeOfferID = new HashSet<String>();
	
	public static final Map<String, String> sharedOfferMapping = new HashMap<String, String>();
	public static final Map<String, String> providerOfferMapping = new HashMap<String, String>();
	public static final Map<String, String> parentChildRelationMapping = new HashMap<String, String>();
	public static final Map<String, String> sharedProviderOfferMapping = new HashMap<String, String>();
	public static final Map<String, String> utMapping = new HashMap<String, String>();
	public static final Map<String, String> daMapping = new HashMap<String, String>();
	public static final Map<String, String> pamMapping = new HashMap<String, String>();
	
	public static final Map<String, String> dataOfferMapping = new HashMap<String, String>();
	public static final Map<String, String> SpecialUTMapping = new HashMap<String, String>();
	
	public static final Map<String, String> serviceClassMapping = new HashMap<String, String>();
	public static final Map<String, Set<String>> ParentChildRelation = new HashMap<String, Set<String>>();
	public static final Map<String, String> ParentChildRejectionMSISDN = new HashMap<String, String>();
	
	//load all the input file into memory for phase 1.
	public static final Map<String,Set<String>> Implicit_Shared_OfferExtraInput = new HashMap<String,Set<String>>();
	public static final Map<String,Set<String>> Addon_OfferExtraInput = new HashMap<String,Set<String>>();
	public static final Map<String,Set<String>> Shared_Offer_RelationExtraInput = new HashMap<String,Set<String>>();
	
	//loading all the three input file for phase2
	public static final Set<String> CompleteListToMigrate = new HashSet<String>();
	public static final Set<String> AccountInputMsisdn = new HashSet<String>();
	public static final Set<String> CreditLimitInputMsisdn = new HashSet<String>();
	
	public static final Set<String> AlreadyMigratedMsisdn = new HashSet<String>();
	
	public static final Map<String,String> mainCreditLimitInput = new HashMap<String,String>();
	public static final Map<String, Set<String>> mainOfferInputRelation = new HashMap<String, Set<String>>();
	
	public static final Map<String, Set<String>> implicitSharedOfferInputRelation = new HashMap<String, Set<String>>();
	public static final Map<String, Set<String>> sharedInputRelation = new HashMap<String, Set<String>>();
	public static final Set<String> sharedInputRelationMSISDN = new HashSet<String>();
	
	public static final Map<String, Long> SourceServiceClass = new HashMap<String, Long>();
	
	public static final Map<String, Long> KPImainOfferInput = new HashMap<String, Long>();
	public static final Map<String, Long> KPIimplicitSharedOffer = new HashMap<String, Long>();
	public static final Map<String, Long> KPIsharedOfferInput = new HashMap<String, Long>();
	public static final Map<String, Long> KPIUsageCounterCount = new HashMap<String, Long>();
	public static final Map<String, Long> KPIUsageCounterZeroCount = new HashMap<String, Long>();
	public static final Map<String, Long> KPIUsageThresholdCount = new HashMap<String, Long>();
	public static final Map<String, Long> KPIUsageThresholdZeroCount = new HashMap<String, Long>();
	public static final Map<String, Long> KPIUsageCounterBalance = new HashMap<String, Long>();
	public static final Map<String, Long> KPIUsageThresholdBalance = new HashMap<String, Long>();
	public static final Map<String, Long> KPIUsageCounterInputCount = new HashMap<String, Long>();
	public static final Map<String, Long> KPIUsageThresholdInputCount = new HashMap<String, Long>();
	
	public static final Map<String, Long> KPICreditLimitInputCount = new HashMap<String, Long>();
	public static final Map<String, BigDecimal> KPICreditLimitInputBalance = new HashMap<String, BigDecimal>();
	
	
	public static final Set<String> UsageThresholdMSISDN = new HashSet<String>();
	public static final Set<String> UsageCounterMSISDN = new HashSet<String>();
	
	public static BigDecimal KPIUsageCounterTotalBalance = new BigDecimal("0");
	public static BigDecimal KPIUsageThresholdTotalBalance = new BigDecimal("0");
	
	public static Set<String> MSISDNMismatch = new HashSet<String>();
	
	public static int counterForSC = 0;
	
	public static Map<String, Object> createHashMapFromJsonString(String json) {
		JsonParser parser = new JsonParser();
		JsonObject object = (JsonObject) parser.parse(json);
		Set<Map.Entry<String, JsonElement>> set = object.entrySet();
		Iterator<Map.Entry<String, JsonElement>> iterator = set.iterator();
		HashMap<String, Object> map = new HashMap<String, Object>();

		while (iterator.hasNext()) {

			Map.Entry<String, JsonElement> entry = iterator.next();
			String key = '"' + entry.getKey() + '"';
			JsonElement value = entry.getValue();

			if (null != value) {
				if (!value.isJsonPrimitive()) {
					if (value.isJsonObject()) {
						map.put(key, createHashMapFromJsonString(value.toString()));
					} else if (value.isJsonArray() && value.toString().contains(":")) {

						List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
						JsonArray array = value.getAsJsonArray();
						if (null != array) {
							for (JsonElement element : array) {
								list.add(createHashMapFromJsonString(element.toString()));
							}
							map.put(key, list);
						}
					} else if (value.isJsonArray() && !value.toString().contains(":")) {
						map.put(key, value.getAsJsonArray());
					}
				} else {
					map.put(key, '"' + value.getAsString() + '"');
				}
			}
		}
		return map;
	}
	
	public LoadSubscriberMapping(String JsonFileName,String pathOfMigtool, String configPath, String inputPath) {
		this.migToolPath = pathOfMigtool;
		this.configPath = configPath;
		this.inputPath = inputPath;
	    this.JsonFileName = JsonFileName;
	    
	}
	
	public Map<? extends String, ? extends String> PopulateKeyValueMapping(String value) {
		// TODO Auto-generated method stub
		Map<String,String> OfferMap = new HashMap<String,String>();
		Map<String, Object> firstLevelmap = createHashMapFromJsonString(value);
	  	for (Map.Entry<String,Object> entry : firstLevelmap.entrySet())  
	  	{
	  		String KeyValue = entry.getKey().replaceAll("\"", "");
	  		String ValueValue = entry.getValue().toString().replaceAll("\"", "").replace("{", "").replace("}", "").replace("Value=", "");
	  		OfferMap.put(KeyValue, ValueValue);
	  		//System.out.println("Key = " + KeyValue + ", Value = " + ValueValue);	  		
	  	}
		return OfferMap;
	}
	
	public Map<? extends String, ? extends String> PopulateKeyValueMappingParentChild(String value) {
		// TODO Auto-generated method stub
		Map<String,String> OfferMap = new HashMap<String,String>();
		Map<String, Object> firstLevelmap = createHashMapFromJsonString(value);
	  	for (Map.Entry<String,Object> entry : firstLevelmap.entrySet())  
	  	{
	  		String KeyValue = entry.getKey().replaceAll("\"", "");
	  		String ValueValue = entry.getValue().toString().replaceAll("\"", "").replace("{", "").replace("}", "").replace("Value=", "").replace(",0", "").replace("0,", "") ;
	  		
	  		OfferMap.put(KeyValue, ValueValue);
	  		//System.out.println("Key = " + KeyValue + ", Value = " + ValueValue);	  		
	  	}
	  	
		return OfferMap;
	}
	
	public Map<String,Set<String>> PopulateKeyValueInSetMapping(String value) {
		// TODO Auto-generated method stub
		Map<String,Set<String>> OfferMap = new HashMap<String,Set<String>>();
		
		Object jsonObject;
		try {
			jsonObject = new JSONParser().parse(value);
			JSONObject jo = (JSONObject) jsonObject;
			  
			  for (Object jsonKey : jo.keySet()) 
			  { 
				  String key = (String) jsonKey;
			      String valueObject = String.valueOf(jo.get(key));
			      if(OfferMap.containsKey(key))
				  {
			    	  Set<String> child = new HashSet<String>(OfferMap.get(key));
			    	  child.add(valueObject.split(":")[1].replaceAll("\"", "").replaceAll("\\}", "").replaceAll("\\]", "").replaceAll("\\[", ""));
			    	  OfferMap.put(key,child);					
				  }
			      else
			      {
			    	  Set<String> child = new HashSet<String>();
					  child.add((valueObject.split(":")[1].replaceAll("\"", "").replaceAll("\\}", "").replaceAll("\\]", "").replaceAll("\\[", "")));
					  OfferMap.put(key,child);
				  }			      
			  }
		}  
		catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return OfferMap;
	}

	public Collection<? extends Integer> PopulateRangeMapping(String value) {
		Set<Integer> OfferSet = new HashSet<Integer>();
		Map<String, Object> firstLevelmap = createHashMapFromJsonString(value);
	  	for (Map.Entry<String,Object> entry : firstLevelmap.entrySet())  
	  	{
	  	  //System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
	  	  String[] RangeValue = entry.getKey().replaceAll("\"", "").split(":");
		  /*
		  * BigInteger StartRange = new BigInteger(RangeValue[0]); BigInteger EndRange =
		  * new BigInteger(RangeValue[1]); for(BigInteger i = StartRange;
		  * i.compareTo(EndRange) <=0; i.add(BigInteger.ONE)) { timerOfferSet.add(i); }
		  */
	  	  int StartRange = Integer.parseInt(RangeValue[0]);
	  	  int EndRange = Integer.parseInt(RangeValue[1]);
	  	  for(int i = StartRange; i <= EndRange; i++)
	  	  {
	  		OfferSet.add(i);
	  	  }
	  	}
	  	return OfferSet;
	}
	
	public void intializeCommonConfigMapping() {
		try 
		  { 
			  Object jsonObject = new JSONParser().parse(new FileReader(configPath + JsonFileName)); 
			  JSONObject jo = (JSONObject) jsonObject;
		  
			  for (Object jsonKey : jo.keySet()) 
			  { 
				  String key = (String) jsonKey;
			      //System.out.println(key); 
			      Object value = jo.get(key); 
			      if(key.toUpperCase().equals("VARIABLES")) 
			      {
			    	  CommonConfigMapping.putAll(PopulateKeyValueMapping(value.toString()));
			    	  System.out.println("CommonConfigMap: " + CommonConfigMapping.size());		    	
			      }
			  }
		  }
		  catch (Exception e) 
		  {
			   // TODO Auto-generated catch block 
			 LOG.error("Exception occured ", e); 
		  }
	}

	public void intializeMapping() {
		// TODO Auto-generated method stub
		try 
		  { 
			  Object jsonObject = new JSONParser().parse(new FileReader(configPath + JsonFileName)); 
			  JSONObject jo = (JSONObject) jsonObject;
		  
			  for (Object jsonKey : jo.keySet()) 
			  { 
				  String key = (String) jsonKey;
			      //System.out.println(key); 
			      Object value = jo.get(key); 
			      if(key.toUpperCase().equals("TIMER_OFFER")) 
			      {
			    	  timerOfferMapping.addAll(PopulateRangeMapping(value.toString()));
			    	  System.out.println("timerOfferMapping: " + timerOfferMapping.size());
			      }		      
			      if(key.toUpperCase().equals("IMPLICIT_MAIN_OFFERS")) 
			      {
			    	  implicitOfferMapping.addAll(PopulateRangeMapping(value.toString()));
			    	  System.out.println("implicitOfferMapping: " + implicitOfferMapping.size());
			      }
			      if(key.toUpperCase().equals("MULTI_PURCHASE_OFFER")) 
			      {
			    	  multiPurposeOfferMapping.putAll(PopulateKeyValueMapping(value.toString()));
			    	  System.out.println("multiPurposeOfferMapping: " + multiPurposeOfferMapping.size());
			      }
			      if(key.toUpperCase().equals("VARIABLES")) 
			      {
			    	  CommonConfigMapping.putAll(PopulateKeyValueMapping(value.toString()));
			    	  System.out.println("CommonConfigMap: " + CommonConfigMapping.size());		    	
			      }
			      if(key.toUpperCase().equals("SHARED_OFFER")) 
			      {
			    	  sharedOfferMapping.putAll(PopulateKeyValueMapping(value.toString()));
			    	  System.out.println("sharedOfferMapping: " + sharedOfferMapping.size());		    	
			      }
			      if(key.toUpperCase().equals("PROVIDER_OFFER")) 
			      {
			    	  providerOfferMapping.putAll(PopulateKeyValueMapping(value.toString()));
			    	  System.out.println("providerOfferMapping: " + providerOfferMapping.size());		    	
			      }
			      if(key.toUpperCase().equals("PARENT_CHILD_RELATION")) 
			      {
			    	  parentChildRelationMapping.putAll(PopulateKeyValueMapping(value.toString()));
			    	  System.out.println("parentChildRelationMapping: " + parentChildRelationMapping.size());		    	
			      }
			      if(key.toUpperCase().equals("SHAREDPROVIDEROFFER")) 
			      {
			    	  Map<String, String> tempsharedProviderOfferMapping = new HashMap<String, String>();
			    	  tempsharedProviderOfferMapping.putAll(PopulateKeyValueMapping(value.toString()));
			    	  for(Entry<String, String> s: tempsharedProviderOfferMapping.entrySet())
			    	  {
			    		  if(s.getValue().contains("Provider_Offer_ID="))
			    			  sharedProviderOfferMapping.put(s.getKey(), s.getValue().replace("Provider_Offer_ID=", ""));
			    		  else
			    			  sharedProviderOfferMapping.put(s.getKey(), s.getValue());
			    	  }
			    	  System.out.println("sharedProviderOffer: " + sharedProviderOfferMapping.size());		    	
			      }
			      if(key.toUpperCase().equals("UT")) 
			      {
			    	  utMapping.putAll(PopulateKeyValueMapping(value.toString()));
			    	  System.out.println("utMapping: " + utMapping.size());		    	
			      }
			      if(key.toUpperCase().equals("DA")) 
			      {
			    	  daMapping.putAll(PopulateKeyValueMapping(value.toString()));
			    	  System.out.println("daMapping: " + daMapping.size());		    	
			      }
			      if(key.toUpperCase().equals("DATA_OFFER")) 
			      {
			    	  dataOfferMapping.putAll(PopulateKeyValueMapping(value.toString()));
			    	  System.out.println("dataOfferMapping: " + dataOfferMapping.size());		    	
			      }
			      if(key.toUpperCase().equals("SPECIAL_UT")) 
			      {
			    	  SpecialUTMapping.putAll(PopulateKeyValueMapping(value.toString()));
			    	  System.out.println("dataOfferMapping: " + SpecialUTMapping.size());		    	
			      }
			      if(key.toUpperCase().equals("PAM")) 
			      {
			    	  pamMapping.putAll(PopulateKeyValueMapping(value.toString()));
			    	  System.out.println("pamMapping: " + pamMapping.size());		    	
			      }
			      if(key.toUpperCase().equals("SERVICECLASS")) 
			      {
			    	  serviceClassMapping.putAll(PopulateKeyValueMapping(value.toString()));
			    	  System.out.println("serviceClassMapping: " + serviceClassMapping.size());		    	
			      }
			      if(key.toUpperCase().equals("PARENT_CHILD_RELATION")) 
			      {
			    	  ParentChildRelation.putAll(PopulateKeyValueInSetMapping(value.toString()));
			    	  System.out.println("ParentChildRelation: " + ParentChildRelation.size());		    	
			      }
			  }
			  
			  //Get Offer which is only Multipurchage & multibenefit and MainOffer_PP=Y
			  
			  //load Credit limit only for phase 2
			  List<String> MigrationPhase = Arrays.asList(LoadSubscriberMapping.CommonConfigMapping.get("MIGRATION_PHASE").toString().split(","));
			  if(MigrationPhase.contains("phase2"))
			  {
				loadCreditLimitFile();
				loadAccountFile();				
				
				loadAddonOfferFile(inputPath);
				UniqueMSISDNList();
				System.out.println("Unique MSISDN List: " + CompleteListToMigrate.size());
				//Write source metrics
				
				//Get the list of MSISDN which already migrated.
				MigratedMSISDNList();
				//Calculate source metrics
				loadCreditLimitInputRelation();
				WritePhase2InputMatricsToFile();
			  }
			  
			  //Loading MainOffer.csv
			  loadMainOfferInputRelation();	  
			  System.out.println("Main_offers: " + mainOfferInputRelation.size());
			  //Loading Implicit_Shared_Offer.csv
			  loadImplicitOfferMapping();
			  System.out.println("Implicit_Shared_Offer: " + implicitSharedOfferInputRelation.size());
			  //Loading the Shared_Offer_Relation.csv file into map
			  loadingSharedOfferRelation();
			  System.out.println("Shared_Offer_Relation: " + sharedInputRelation.size());
			  
			  WriteAddonOfferMetrics();
			  //Now write Everything in the File.
			  WriteInputMatricsToFile();
			  
			  //intialize and establish parentchild relations.
			  FindParentChildRelationInSourceDump();
			  
			  
		  }
		  catch (Exception e) 
		  {
			   // TODO Auto-generated catch block 
			  e.printStackTrace();
		  }
	}

	private void MigratedMSISDNList() {
		// TODO Auto-generated method stub
		try {
			  
		  BufferedReader br = new BufferedReader(new FileReader(migToolPath + "/config/OfferDetails/MigratedMSISDN.txt"));
		  String line = "";
		  while ((line = br.readLine()) != null) {
			if(!line.isEmpty())
			{
				AlreadyMigratedMsisdn.add((line));
				CompleteListToMigrate.add(line);
			}
		  }
		}
		catch(Exception ex)
	    {
		   ex.printStackTrace();
	    }
	}

	private void UniqueMSISDNList() {
		// TODO Auto-generated method stub
		
		CompleteListToMigrate.addAll(CreditLimitInputMsisdn.stream() 
                .filter(AccountInputMsisdn::contains) 
                .collect(Collectors 
                             .toList()));
	}

	private void WritePhase2InputMatricsToFile() {
		// TODO Auto-generated method stub
		
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(migToolPath + "/Temp/" +  "Phase2sourceCreditLimitCount.csv"));){
			for(Entry<String,Long> str : KPICreditLimitInputCount.entrySet())
			{
			  bw.append(str.getKey() + ":" + str.getValue());
			  bw.append(System.lineSeparator());
			}
			bw.flush();
			bw.close();			  
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(migToolPath + "/Temp/" +  "Phase2sourceCreditLimitBalance.csv"));){
			for(Entry<String,BigDecimal> str : KPICreditLimitInputBalance.entrySet())
			{
			  bw.append(str.getKey() + ":" + str.getValue());
			  bw.append(System.lineSeparator());
			}
			bw.flush();
			bw.close();			  
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(migToolPath + "/Temp/" +  "Phase2sourceServiceClass.csv"));){
			for(Entry<String,Long> str : SourceServiceClass.entrySet())
			{
			  bw.append(str.getKey() + ":" + str.getValue());
			  bw.append(System.lineSeparator());
			}
			bw.flush();
			bw.close();			  
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

	public static void loadImplicitSharedOfferFile(String inputPath)
	{
		try {
			  
		  BufferedReader br = new BufferedReader(new FileReader(inputPath + "/Implicit_Shared_Offer.csv"));
		  String line = "";
		  br.readLine();
		  while ((line = br.readLine()) != null) {
			String datas[] = line.split(",",-1);
			if(!LoadSubscriberMapping.mainOfferInputRelation.containsKey(datas[0]))
			{
				if(LoadSubscriberMapping.Implicit_Shared_OfferExtraInput.containsKey(datas[0]))
				{
					Set<String> child = new HashSet<String>(LoadSubscriberMapping.Implicit_Shared_OfferExtraInput.get(datas[0]));
					child.add(line);
					LoadSubscriberMapping.Implicit_Shared_OfferExtraInput.put(datas[0],child);					
				}
				else
				{
					Set<String> child = new HashSet<String>();
					child.add(line);
					LoadSubscriberMapping.Implicit_Shared_OfferExtraInput.put(datas[0],child);
				}
			}			
		  }
		 br.close();
	   }
	   catch(Exception ex)
	   {
		   ex.printStackTrace();
	   }
	}
	
	public static void loadAddonOfferFile(String inputPath)
	{
		try {
			  
			  BufferedReader br = new BufferedReader(new FileReader(inputPath + "/Addon_Offer.csv"));
			  String line = "";
			  br.readLine();
			  while ((line = br.readLine()) != null) {
				String datas[] = line.split(",",-1);
				if(LoadSubscriberMapping.Addon_OfferExtraInput.containsKey(datas[0]))
				{
					Set<String> child = new HashSet<String>(LoadSubscriberMapping.Addon_OfferExtraInput.get(datas[0]));
					child.add(line);
					LoadSubscriberMapping.Addon_OfferExtraInput.put(datas[0],child);					
				}
				else
				{
					Set<String> child = new HashSet<String>();
					child.add(line);
					LoadSubscriberMapping.Addon_OfferExtraInput.put(datas[0],child);
				}
			  }
		 br.close();
	   }
	   catch(Exception ex)
	   {
		   ex.printStackTrace();
	   }
	}
	
	public static void loadSharedOfferRelationFile(String inputPath)
	{
		try {			  
			  BufferedReader br = new BufferedReader(new FileReader(inputPath + "/Shared_Offer_Relation.csv"));
			  String line = "";
			  br.readLine();
			  while ((line = br.readLine()) != null) {
				String datas[] = line.split(",",-1);
				if(!LoadSubscriberMapping.mainOfferInputRelation.containsKey(datas[0]))
				{
					if(LoadSubscriberMapping.Shared_Offer_RelationExtraInput.containsKey(datas[0]))
					{
						Set<String> child = new HashSet<String>(LoadSubscriberMapping.Shared_Offer_RelationExtraInput.get(datas[0]));
						child.add(line);
						LoadSubscriberMapping.Shared_Offer_RelationExtraInput.put(datas[0],child);					
					}
					else
					{
						Set<String> child = new HashSet<String>();
						child.add(line);
						LoadSubscriberMapping.Shared_Offer_RelationExtraInput.put(datas[0],child);
					}
				}				
			  }
		    br.close();
	   }
	   catch(Exception ex)
	   {
		   ex.printStackTrace();
	   }
	}
	
	
	private void FindParentChildRelationInSourceDump() {
		// TODO Auto-generated method stub
		//take value from offer file
		Map<String,Set<String>> inputOfferFile = new HashMap<String, Set<String>>();
		try {
		  BufferedReader br = new BufferedReader(new FileReader(inputPath + "/Main_Offer.csv"));
		  String line = "";
		  br.readLine();
		  while ((line = br.readLine()) != null) {
			String mMSISDN = line.split(",",-1)[0];
			
			if(inputOfferFile.containsKey(mMSISDN))
			{
				Set<String> temp = new HashSet<String>();
				temp.addAll(inputOfferFile.get(mMSISDN));
				temp.add(line);
				inputOfferFile.put(mMSISDN, temp);
			}
			else
			{
				Set<String> temp = new HashSet<String>();
				temp.add(line);
				inputOfferFile.put(mMSISDN, temp);
			}			
		  }
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		//populate all theMSISDN of sharedRelations
		for(Entry<String,Set<String>> etr : sharedInputRelation.entrySet())
		{
			sharedInputRelationMSISDN.add(etr.getKey());
			Set<String> temp = new HashSet<String>(etr.getValue());
			for(String str: temp)
			{
				sharedInputRelationMSISDN.add(str.split(",",-1)[0]);
				sharedInputRelationMSISDN.add(str.split(",",-1)[2]);
			}
		}
		
		try {
		  for (Entry<String,Set<String>> inputValue : inputOfferFile.entrySet()) {
			String mMSISDN = inputValue.getKey();
			if(mMSISDN.startsWith("100"))
			{
				//Check in Shared_Offer_Relation.csv
				String MasterOfferID = inputValue.getValue().toString().split(",",-1)[1];
				
				//Get ChildOfferID
				Set<String> ChildOfferID = new HashSet<String>();
				if(ParentChildRelation.containsKey(MasterOfferID))
				{
					ChildOfferID.addAll(Arrays.asList(ParentChildRelation.get(MasterOfferID).toString().replace("[", "").replace("]", "").split(",")));
				}
				
 				if(LoadSubscriberMapping.sharedInputRelation.containsKey(mMSISDN))
				{
					Set<String> childOffers = new HashSet<String>(); 
					childOffers.addAll(LoadSubscriberMapping.sharedInputRelation.get(mMSISDN));
					Map<String, Set<String>> ChildOfferList = new HashMap<String, Set<String>>();
					for(String childOffer: childOffers)
					{
						if(inputOfferFile.containsKey(childOffer.split(",",-1)[0]))
						{
							Set<String> childOfferList = new HashSet<String>(inputOfferFile.get(childOffer.split(",",-1)[0]));
							for(String str : childOfferList)
							{
								if(ChildOfferList.containsKey(str.split(",")[1]))
								{
									Set<String> tempOffer = new HashSet<String>();
									
									tempOffer.addAll(ChildOfferList.get(str.split(",")[1]));
									tempOffer.add(str.split(",")[0]);
									ChildOfferList.put(str.split(",")[1],tempOffer);
								}
								else
								{
									Set<String> tempOffer = new HashSet<String>();
									tempOffer.add(str.split(",")[0]);
									ChildOfferList.put(str.split(",")[1],tempOffer);
								}
							}														
						}
					}
					for(Entry<String,Set<String>> etr : ChildOfferList.entrySet())
					{
						if(!ChildOfferID.contains(etr.getKey()))
						{
							for(String rejMSISDN: etr.getValue())
							{
								if(!etr.getKey().startsWith("30000"))
									ParentChildRejectionMSISDN.put(rejMSISDN,etr.getKey().toString());
							}
							/*for(Entry<String, Set<String>> trst:  ChildOfferList.entrySet())
								ParentChildRejectionMSISDN.addAll(trst.getValue());*/									
							break;
						}
					}					
					
					//childOfferMsisdn.addAll(LoadSubscriberMapping.sharedInputRelation.get(mMSISDN));
					//for(String childMsisdn: )
					//System.out.println("-------------");
					//childOfferMsisdn.forEach(s->System.out.println(s));
				}
			}			
		  }		 
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}

	public static void loadExtraAddonOfferFile(String inputPath)
	{
		try {
			  
		  BufferedReader br = new BufferedReader(new FileReader(inputPath + "/Addon_Offer.csv"));
		  String line = "";
		  br.readLine();
		  while ((line = br.readLine()) != null) {
			String datas[] = line.split(",",-1);
			if(!LoadSubscriberMapping.mainOfferInputRelation.containsKey(datas[0]))
			{
				if(LoadSubscriberMapping.Addon_OfferExtraInput.containsKey(datas[0]))
				{
					Set<String> child = new HashSet<String>(LoadSubscriberMapping.Addon_OfferExtraInput.get(datas[0]));
					child.add(line);
					LoadSubscriberMapping.Addon_OfferExtraInput.put(datas[0],child);					
				}
				else
				{
					Set<String> child = new HashSet<String>();
					child.add(line);
					LoadSubscriberMapping.Addon_OfferExtraInput.put(datas[0],child);
				}
			}				
		  }
		 br.close();
	   }
	   catch(Exception ex)
	   {
		   ex.printStackTrace();
	   }
	}
	
	private void loadAccountFile() {
		// TODO Auto-generated method stub
		try {
			  
			  BufferedReader br = new BufferedReader(new FileReader(inputPath + "/Account.csv"));
			  String line = "";
			  br.readLine();
			  while ((line = br.readLine()) != null) {
				String datas[] = line.split(",",-1);
				AccountInputMsisdn.add(datas[0]);
			  }
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	private void loadCreditLimitFile() {
		// TODO Auto-generated method stub
		try {
			  
			  BufferedReader br = new BufferedReader(new FileReader(inputPath + "/Credit_Limit.csv"));
			  String line = "";
			  br.readLine();
			  while ((line = br.readLine()) != null) {
				String datas[] = line.split(",",-1);
				mainCreditLimitInput.put(datas[0],line);
				CreditLimitInputMsisdn.add(datas[0]);
			  }
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

	private void WriteAddonOfferMetrics() {
		// TODO Auto-generated method stub
		Map<String, Long> AddOnInputOffer = new HashMap<String, Long>();
		try {
		  BufferedReader br = new BufferedReader(new FileReader(inputPath + "/Addon_Offer.csv"));
		  String line = "";
		  br.readLine();
		  while ((line = br.readLine()) != null) {
			String datas[] = line.split(",",-1);
			if(!LoadSubscriberMapping.mainOfferInputRelation.containsKey(datas[0]) && !LoadSubscriberMapping.AccountInputMsisdn.contains(datas[0]))
			{
				//System.out.println(datas[0]);
				String Value = "INC50:MSISDN=" + datas[0] + ":OFFER_UV_UT=" + datas[1] + "-" + datas[4]  + "-" + datas[5] + ":DESCRIPTION=ADDON OFFER MSISDN NOT PRESENT IN ACCOUNT:ACTION=DISCARD & LOG";
				MSISDNMismatch.add(Value);
				//continue;
			}
			
			if(AddOnInputOffer.containsKey(datas[1]))
			{
				Long temp = AddOnInputOffer.get(datas[1]);
				AddOnInputOffer.put(datas[1], temp+1);
				if(LoadSubscriberMapping.KPIUsageCounterInputCount.containsKey(datas[1]))
				{
					Long currentBalance = LoadSubscriberMapping.KPIUsageCounterInputCount.get(datas[1]) + 1;
					LoadSubscriberMapping.KPIUsageCounterInputCount.put(datas[1],currentBalance);					
				}
				else
				{
					LoadSubscriberMapping.KPIUsageCounterInputCount.put(datas[1],1L);
				}
				if(LoadSubscriberMapping.KPIUsageThresholdInputCount.containsKey(datas[1]))
				{
					Long currentBalance = LoadSubscriberMapping.KPIUsageThresholdInputCount.get(datas[1]) + 1;
					LoadSubscriberMapping.KPIUsageThresholdInputCount.put(datas[1],currentBalance);					
				}
				else
				{
					LoadSubscriberMapping.KPIUsageThresholdInputCount.put(datas[1],1L);
				}
				if(!datas[4].isEmpty() && Long.parseLong(datas[4]) > 0 )
				{
					if(LoadSubscriberMapping.KPIUsageCounterBalance.containsKey(datas[1]))
					{
						UsageCounterMSISDN.add(datas[0] + ":" + datas[1]);
						Long count = LoadSubscriberMapping.KPIUsageCounterCount.get(datas[1]);
						LoadSubscriberMapping.KPIUsageCounterCount.put(datas[1],count + 1L);
						
						Long currentBalance = LoadSubscriberMapping.KPIUsageCounterBalance.get(datas[1]) + Long.parseLong(datas[4]);
						LoadSubscriberMapping.KPIUsageCounterBalance.put(datas[1],currentBalance);					
					}
					else
					{
						UsageCounterMSISDN.add(datas[0] + ":" + datas[1]);
						
						LoadSubscriberMapping.KPIUsageCounterCount.put(datas[1], 1L);
						LoadSubscriberMapping.KPIUsageCounterBalance.put(datas[1],Long.parseLong(datas[4]));
					}
					
					LoadSubscriberMapping.KPIUsageCounterTotalBalance = LoadSubscriberMapping.KPIUsageCounterTotalBalance.add(new BigDecimal(datas[4].toString()));
				}
				else
				{
					if(LoadSubscriberMapping.CompleteListToMigrate.contains(datas[0]))
					{
						if(LoadSubscriberMapping.KPIUsageCounterZeroCount.containsKey(datas[1]))
						{
							Long count = LoadSubscriberMapping.KPIUsageCounterZeroCount.get(datas[1]);
							LoadSubscriberMapping.KPIUsageCounterZeroCount.put(datas[1],count + 1L);					
						}
						else
						{
							LoadSubscriberMapping.KPIUsageCounterZeroCount.put(datas[1], 1L);						
						}
					}
				}
				if(!datas[5].isEmpty() && Long.parseLong(datas[5]) > 0)
				{
					if(LoadSubscriberMapping.KPIUsageThresholdBalance.containsKey(datas[1]))
					{
						UsageThresholdMSISDN.add(datas[0] + ":" + datas[1]);
						Long count = LoadSubscriberMapping.KPIUsageThresholdCount.get(datas[1]);
						LoadSubscriberMapping.KPIUsageThresholdCount.put(datas[1],count + 1L);
						
						Long currentBalance = LoadSubscriberMapping.KPIUsageThresholdBalance.get(datas[1]) + Long.parseLong(datas[5]);
						LoadSubscriberMapping.KPIUsageThresholdBalance.put(datas[1],currentBalance);
						
					}
					else
					{
						UsageThresholdMSISDN.add(datas[0] + ":" + datas[1]);
						LoadSubscriberMapping.KPIUsageThresholdCount.put(datas[1], 1L);						
						LoadSubscriberMapping.KPIUsageThresholdBalance.put(datas[1],Long.parseLong(datas[5]));
					}
					LoadSubscriberMapping.KPIUsageThresholdTotalBalance = LoadSubscriberMapping.KPIUsageThresholdTotalBalance.add(new BigDecimal(datas[5].toString()));
				}
				else
				{
					if(LoadSubscriberMapping.KPIUsageThresholdZeroCount.containsKey(datas[1]))
					{
						Long count = LoadSubscriberMapping.KPIUsageThresholdZeroCount.get(datas[1]);
						LoadSubscriberMapping.KPIUsageThresholdZeroCount.put(datas[1],count + 1L);					
					}
					else
					{
						LoadSubscriberMapping.KPIUsageThresholdZeroCount.put(datas[1], 1L);						
					}
				}
			}
			else
			{
				AddOnInputOffer.put(datas[1], 1L);
				if(LoadSubscriberMapping.KPIUsageCounterInputCount.containsKey(datas[1]))
				{
					Long currentBalance = LoadSubscriberMapping.KPIUsageCounterInputCount.get(datas[1]) + 1;
					LoadSubscriberMapping.KPIUsageCounterInputCount.put(datas[1],currentBalance);					
				}
				else
				{
					LoadSubscriberMapping.KPIUsageCounterInputCount.put(datas[1],1L);
				}
				if(LoadSubscriberMapping.KPIUsageThresholdInputCount.containsKey(datas[1]))
				{
					Long currentBalance = LoadSubscriberMapping.KPIUsageThresholdInputCount.get(datas[1]) + 1;
					LoadSubscriberMapping.KPIUsageThresholdInputCount.put(datas[1],currentBalance);					
				}
				else
				{
					LoadSubscriberMapping.KPIUsageThresholdInputCount.put(datas[1],1L);
				}
				if(!datas[4].isEmpty() && Long.parseLong(datas[4]) > 0)
				{
					if(LoadSubscriberMapping.KPIUsageCounterBalance.containsKey(datas[1]))
					{
						UsageCounterMSISDN.add(datas[0] + ":" + datas[1]);
						Long count = LoadSubscriberMapping.KPIUsageCounterCount.get(datas[1]);
						LoadSubscriberMapping.KPIUsageCounterCount.put(datas[1],count + 1L);
						
						Long currentBalance = LoadSubscriberMapping.KPIUsageCounterBalance.get(datas[1]) + Long.parseLong(datas[4]);
						LoadSubscriberMapping.KPIUsageCounterBalance.put(datas[1],currentBalance);					
					}
					else
					{
						UsageCounterMSISDN.add(datas[0] + ":" + datas[1]);
						
						LoadSubscriberMapping.KPIUsageCounterCount.put(datas[1],1L);
						LoadSubscriberMapping.KPIUsageCounterBalance.put(datas[1],Long.parseLong(datas[4]));
					}
					LoadSubscriberMapping.KPIUsageCounterTotalBalance = LoadSubscriberMapping.KPIUsageCounterTotalBalance.add(new BigDecimal(datas[4].toString()));
				}
				else
				{
					if(LoadSubscriberMapping.CompleteListToMigrate.contains(datas[0]))
					{
						if(LoadSubscriberMapping.KPIUsageCounterZeroCount.containsKey(datas[1]))
						{
							Long count = LoadSubscriberMapping.KPIUsageCounterZeroCount.get(datas[1]);
							LoadSubscriberMapping.KPIUsageCounterZeroCount.put(datas[1],count + 1L);					
						}
						else
						{
							LoadSubscriberMapping.KPIUsageCounterZeroCount.put(datas[1], 1L);						
						}
					}
				}
				if(!datas[5].isEmpty() && Long.parseLong(datas[5]) > 0)
				{
					if(LoadSubscriberMapping.KPIUsageThresholdBalance.containsKey(datas[1]))
					{
						UsageThresholdMSISDN.add(datas[0] + ":" + datas[1]);
						Long count = LoadSubscriberMapping.KPIUsageThresholdCount.get(datas[1]);
						LoadSubscriberMapping.KPIUsageThresholdCount.put(datas[1],count + 1L);
						
						Long currentBalance = LoadSubscriberMapping.KPIUsageThresholdBalance.get(datas[1]) + Long.parseLong(datas[5]);
						LoadSubscriberMapping.KPIUsageThresholdBalance.put(datas[1],currentBalance);					
					}
					else
					{
						UsageThresholdMSISDN.add(datas[0] + ":" + datas[1]);
						
						LoadSubscriberMapping.KPIUsageThresholdCount.put(datas[1],1L);
						LoadSubscriberMapping.KPIUsageThresholdBalance.put(datas[1],Long.parseLong(datas[5]));
					}
					LoadSubscriberMapping.KPIUsageThresholdTotalBalance = LoadSubscriberMapping.KPIUsageThresholdTotalBalance.add(new BigDecimal(datas[5].toString()));
				}
				else
				{
					if(LoadSubscriberMapping.KPIUsageThresholdZeroCount.containsKey(datas[1]))
					{
						Long count = LoadSubscriberMapping.KPIUsageThresholdZeroCount.get(datas[1]);
						LoadSubscriberMapping.KPIUsageThresholdZeroCount.put(datas[1],count + 1L);					
					}
					else
					{
						LoadSubscriberMapping.KPIUsageThresholdZeroCount.put(datas[1], 1L);						
					}
				}
			 }
		  }
		}
		catch (Exception e) {
			// TODO: handle exception
			e.getStackTrace();
		}
		
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(migToolPath + "/Temp/" +  "sourceAddonOfferStat.csv"));){
			for(Entry<String,Long> str : AddOnInputOffer.entrySet())
			{
			  bw.append(str.getKey() + ":" + str.getValue());
			  bw.append(System.lineSeparator());
			}
			bw.flush();
			bw.close();			  
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
	}

	private void WriteInputMatricsToFile() {
		// TODO Auto-generated method stub
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(migToolPath + "/Temp/" +  "sourceImplicitOfferStat.csv"));){
			for(Entry<String,Long> str : KPIimplicitSharedOffer.entrySet())
			{
			  bw.append(str.getKey() + ":" + str.getValue());
			  bw.append(System.lineSeparator());
			}
			bw.flush();
			bw.close();			  
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
			 
		try(BufferedWriter bw = new BufferedWriter(new FileWriter(migToolPath + "/Temp/" +  "sourceMainOfferStat.csv")))
		{
			for(Entry<String,Long> str : KPImainOfferInput.entrySet())
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
		
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(migToolPath + "/Temp/" +  "sourceSharedOfferStat.csv"))){
			for(Entry<String,Long> str : KPIsharedOfferInput.entrySet())
			{
			  bw.append(str.getKey() + ":" + str.getValue());
			  bw.append(System.lineSeparator());
			}
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.getStackTrace();
		}	
		
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(migToolPath + "/Temp/" +  "sourceUsageCounterStat.csv"))){
			for(Entry<String,Long> str : KPIUsageCounterCount.entrySet())
			{
			  bw.append(str.getKey() + ":" + str.getValue());
			  bw.append(System.lineSeparator());
			}
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.getStackTrace();
		}
		
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(migToolPath + "/Temp/" +  "sourceUsageThresholdStat.csv"))){
			for(Entry<String,Long> str : KPIUsageThresholdCount.entrySet())
			{
			  bw.append(str.getKey() + ":" + str.getValue());
			  bw.append(System.lineSeparator());
			}
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.getStackTrace();
		}
		
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(migToolPath + "/Temp/" +  "sourceUsageCounterTotalBal.csv"))){
			bw.append(KPIUsageCounterTotalBalance.toString());
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.getStackTrace();
		}
		
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(migToolPath + "/Temp/" +  "sourceUsageThresholdTotalBal.csv"))){
			bw.append(KPIUsageThresholdTotalBalance.toString());
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.getStackTrace();
		}
		
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(migToolPath + "/Temp/" +  "sourceUsageThresholdBalances.csv"))){
			for(Entry<String,Long> str : KPIUsageThresholdBalance.entrySet())
			{
			  bw.append(str.getKey() + ":" + str.getValue());
			  bw.append(System.lineSeparator());
			}
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.getStackTrace();
		}
		
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(migToolPath + "/Temp/" +  "sourceUsageCounterBalances.csv"))){
			for(Entry<String,Long> str : KPIUsageCounterBalance.entrySet())
			{
			  bw.append(str.getKey() + ":" + str.getValue());
			  bw.append(System.lineSeparator());
			}
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.getStackTrace();
		}
		
		//writing all the MSISDN to file
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(migToolPath + "/Temp/" +  "MSISDN_MisMatch.csv"))){
			for(String str : MSISDNMismatch)
			{
			  bw.append(str);
			  bw.append(System.lineSeparator());
			}
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.getStackTrace();
		}
		
		//UsageThresholdMSISDN.add(datas[0]);
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(migToolPath + "/Temp/" +  "UsageThresholdMSISDN.csv"))){
			for(String str : UsageThresholdMSISDN)
			{
			  bw.append(str);
			  bw.append(System.lineSeparator());
			}
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.getStackTrace();
		}
		
		//UsageThresholdMSISDN.add(datas[0]);
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(migToolPath + "/Temp/" +  "UsageCounterMSISDN.csv"))){
			for(String str : UsageCounterMSISDN)
			{
			  bw.append(str);
			  bw.append(System.lineSeparator());
			}
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.getStackTrace();
		}
		
		//UsageThresholdMSISDN.add(datas[0]);
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(migToolPath + "/Temp/" +  "SourceZeroUsageCounterCount.csv"))){
			for(Entry<String,Long> str : KPIUsageCounterZeroCount.entrySet())
			{
			  bw.append(str.getKey() + ":" + str.getValue());
			  bw.append(System.lineSeparator());
			}
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.getStackTrace();
		}
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(migToolPath + "/Temp/" +  "SourceZeroUsageThresholdCount.csv"))){
			for(Entry<String,Long> str : KPIUsageThresholdZeroCount.entrySet())
			{
			  bw.append(str.getKey() + ":" + str.getValue());
			  bw.append(System.lineSeparator());
			}
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.getStackTrace();
		}
		
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(migToolPath + "/Temp/" +  "SourceInputUsageCounterCount.csv"))){
			for(Entry<String,Long> str : KPIUsageCounterInputCount.entrySet())
			{
			  bw.append(str.getKey() + ":" + str.getValue());
			  bw.append(System.lineSeparator());
			}
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.getStackTrace();
		}
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(migToolPath + "/Temp/" +  "SourceInputUsageThresholdCount.csv"))){
			for(Entry<String,Long> str : KPIUsageThresholdInputCount.entrySet())
			{
			  bw.append(str.getKey() + ":" + str.getValue());
			  bw.append(System.lineSeparator());
			}
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
	}
	
	private void loadImplicitOfferMapping() {
		// TODO Auto-generated method stub
		try {
		  BufferedReader br = new BufferedReader(new FileReader(inputPath + "/Implicit_Shared_Offer.csv"));
		  String line = "";
		  br.readLine();
		  while ((line = br.readLine()) != null) {
			String datas[] = line.split(",",-1);
			//if(LoadSubscriberMapping.CompleteListToMigrate.contains(datas[0]))
			{			
				if(LoadSubscriberMapping.implicitSharedOfferInputRelation.containsKey(datas[0]))
				{
					Set<String> child = new HashSet<String>(LoadSubscriberMapping.implicitSharedOfferInputRelation.get(datas[0]));
					child.add(line);
					LoadSubscriberMapping.implicitSharedOfferInputRelation.put(datas[0] + "," + datas[1] ,child);					
				}
				else
				{
					Set<String> child = new HashSet<String>();
					child.add(line);
					LoadSubscriberMapping.implicitSharedOfferInputRelation.put(datas[0] + "," + datas[1],child);
				}
				if(LoadSubscriberMapping.KPIimplicitSharedOffer.containsKey(datas[1]))
				{
					Long temp = LoadSubscriberMapping.KPIimplicitSharedOffer.get(datas[1]);
					LoadSubscriberMapping.KPIimplicitSharedOffer.put(datas[1], temp+1);
					//for UC and UT Value
					if( Long.parseLong(datas[1]) >= 3000000L)
					{
						//for UC and UT Value from input
						if(LoadSubscriberMapping.KPIUsageCounterInputCount.containsKey(datas[1]))
						{
							Long currentBalance = LoadSubscriberMapping.KPIUsageCounterInputCount.get(datas[1]) + 1;
							LoadSubscriberMapping.KPIUsageCounterInputCount.put(datas[1],currentBalance);					
						}
						else
						{
							LoadSubscriberMapping.KPIUsageCounterInputCount.put(datas[1],1L);
						}
						if(LoadSubscriberMapping.KPIUsageThresholdInputCount.containsKey(datas[1]))
						{
							Long currentBalance = LoadSubscriberMapping.KPIUsageThresholdInputCount.get(datas[1]) + 1;
							LoadSubscriberMapping.KPIUsageThresholdInputCount.put(datas[1],currentBalance);					
						}
						else
						{
							LoadSubscriberMapping.KPIUsageThresholdInputCount.put(datas[1],1L);
						}
						
						//for UC and UT valid Value from input
						if(!datas[4].isEmpty() && Long.parseLong(datas[4]) > 0)
						{
							if(LoadSubscriberMapping.KPIUsageCounterBalance.containsKey(datas[1]))
							{
								UsageCounterMSISDN.add(datas[0] + ":" + datas[1]);
								Long count = LoadSubscriberMapping.KPIUsageCounterCount.get(datas[1]);
								LoadSubscriberMapping.KPIUsageCounterCount.put(datas[1],count + 1L);
								
								Long currentBalance = LoadSubscriberMapping.KPIUsageCounterBalance.get(datas[1]) + Long.parseLong(datas[4]);
								LoadSubscriberMapping.KPIUsageCounterBalance.put(datas[1],currentBalance);					
							}
							else
							{
								UsageCounterMSISDN.add(datas[0] + ":" + datas[1]);
								LoadSubscriberMapping.KPIUsageCounterCount.put(datas[1], 1L);
								LoadSubscriberMapping.KPIUsageCounterBalance.put(datas[1],Long.parseLong(datas[4]));
							}
							LoadSubscriberMapping.KPIUsageCounterTotalBalance = LoadSubscriberMapping.KPIUsageCounterTotalBalance.add(new BigDecimal(datas[4]));
						}
						else
						{
							if(LoadSubscriberMapping.CompleteListToMigrate.contains(datas[0]))
							{
								if(LoadSubscriberMapping.KPIUsageCounterZeroCount.containsKey(datas[1]))
								{
									Long count = LoadSubscriberMapping.KPIUsageCounterZeroCount.get(datas[1]);
									LoadSubscriberMapping.KPIUsageCounterZeroCount.put(datas[1],count + 1L);					
								}
								else
								{
									LoadSubscriberMapping.KPIUsageCounterZeroCount.put(datas[1], 1L);						
								}
							}
						}
						if(!datas[5].isEmpty() && Long.parseLong(datas[5]) > 0)
						{
							if(LoadSubscriberMapping.KPIUsageThresholdBalance.containsKey(datas[1]))
							{
								UsageThresholdMSISDN.add(datas[0] + ":" + datas[1]);
								Long count = LoadSubscriberMapping.KPIUsageThresholdCount.get(datas[1]);
								LoadSubscriberMapping.KPIUsageThresholdCount.put(datas[1],count + 1L);
								
								Long currentBalance = LoadSubscriberMapping.KPIUsageThresholdBalance.get(datas[1]) + Long.parseLong(datas[5]);
								LoadSubscriberMapping.KPIUsageThresholdBalance.put(datas[1],currentBalance);					
							}
							else
							{
								UsageThresholdMSISDN.add(datas[0] + ":" + datas[1]);
								Long count = LoadSubscriberMapping.KPIUsageThresholdCount.get(datas[1]);
								LoadSubscriberMapping.KPIUsageThresholdCount.put(datas[1],count + 1L);
								
								LoadSubscriberMapping.KPIUsageThresholdBalance.put(datas[1],Long.parseLong(datas[5]));
							}
							LoadSubscriberMapping.KPIUsageThresholdTotalBalance = LoadSubscriberMapping.KPIUsageThresholdTotalBalance.add(new BigDecimal(datas[5].toString()));
						}
						else
						{
							if(LoadSubscriberMapping.KPIUsageThresholdZeroCount.containsKey(datas[1]))
							{
								Long count = LoadSubscriberMapping.KPIUsageThresholdZeroCount.get(datas[1]);
								LoadSubscriberMapping.KPIUsageThresholdZeroCount.put(datas[1],count + 1L);					
							}
							else
							{
								LoadSubscriberMapping.KPIUsageThresholdZeroCount.put(datas[1], 1L);						
							}
						}
					}
				}
				else
				{
					LoadSubscriberMapping.KPIimplicitSharedOffer.put(datas[1], 1L);
					if(Long.parseLong(datas[1]) >= 3000000L)
					{
						//for UC and UT Value from input
						if(LoadSubscriberMapping.KPIUsageCounterInputCount.containsKey(datas[1]))
						{
							Long currentBalance = LoadSubscriberMapping.KPIUsageCounterInputCount.get(datas[1]) + 1;
							LoadSubscriberMapping.KPIUsageCounterInputCount.put(datas[1],currentBalance);					
						}
						else
						{
							LoadSubscriberMapping.KPIUsageCounterInputCount.put(datas[1],1L);
						}
						if(LoadSubscriberMapping.KPIUsageThresholdInputCount.containsKey(datas[1]))
						{
							Long currentBalance = LoadSubscriberMapping.KPIUsageThresholdInputCount.get(datas[1]) + 1;
							LoadSubscriberMapping.KPIUsageThresholdInputCount.put(datas[1],currentBalance);					
						}
						else
						{
							LoadSubscriberMapping.KPIUsageThresholdInputCount.put(datas[1],1L);
						}
						
						//for UC and UT valid Value from input
						if(!datas[4].isEmpty() && Long.parseLong(datas[4]) > 0)
						{
							if(LoadSubscriberMapping.KPIUsageCounterBalance.containsKey(datas[1]))
							{
								UsageCounterMSISDN.add(datas[0] + ":" + datas[1]);
								Long count = LoadSubscriberMapping.KPIUsageCounterCount.get(datas[1]);
								LoadSubscriberMapping.KPIUsageCounterCount.put(datas[1],count + 1L);
								
								
								Long currentBalance = LoadSubscriberMapping.KPIUsageCounterBalance.get(datas[1]) + Long.parseLong(datas[4]);
								LoadSubscriberMapping.KPIUsageCounterBalance.put(datas[1],currentBalance);					
							}
							else
							{
								UsageCounterMSISDN.add(datas[0] + ":" + datas[1]);
								
								LoadSubscriberMapping.KPIUsageCounterCount.put(datas[1], 1L);
								LoadSubscriberMapping.KPIUsageCounterBalance.put(datas[1],Long.parseLong(datas[4]));
							}
							LoadSubscriberMapping.KPIUsageCounterTotalBalance = LoadSubscriberMapping.KPIUsageCounterTotalBalance.add(new BigDecimal(datas[4].toString()));
						}
						else
						{
							if(LoadSubscriberMapping.CompleteListToMigrate.contains(datas[0]))
							{
								if(LoadSubscriberMapping.KPIUsageCounterZeroCount.containsKey(datas[1]))
								{
									Long count = LoadSubscriberMapping.KPIUsageCounterZeroCount.get(datas[1]);
									LoadSubscriberMapping.KPIUsageCounterZeroCount.put(datas[1],count + 1L);					
								}
								else
								{
									LoadSubscriberMapping.KPIUsageCounterZeroCount.put(datas[1], 1L);						
								}
							}
						}
						if(!datas[5].isEmpty() && Long.parseLong(datas[5]) > 0)
						{
							if(LoadSubscriberMapping.KPIUsageThresholdBalance.containsKey(datas[1]))
							{
								UsageThresholdMSISDN.add(datas[0] + ":" + datas[1]);
								Long count = LoadSubscriberMapping.KPIUsageThresholdCount.get(datas[1]);
								LoadSubscriberMapping.KPIUsageThresholdCount.put(datas[1],count + 1L);
								
								Long currentBalance = LoadSubscriberMapping.KPIUsageThresholdBalance.get(datas[1]) + Long.parseLong(datas[5]);
								LoadSubscriberMapping.KPIUsageThresholdBalance.put(datas[1],currentBalance);					
							}
							else
							{
								UsageThresholdMSISDN.add(datas[0] + ":" + datas[1]);
								LoadSubscriberMapping.KPIUsageThresholdCount.put(datas[1], 1L);
								LoadSubscriberMapping.KPIUsageThresholdBalance.put(datas[1],Long.parseLong(datas[5]));
							}
							LoadSubscriberMapping.KPIUsageThresholdTotalBalance = LoadSubscriberMapping.KPIUsageThresholdTotalBalance.add(new BigDecimal(datas[5].toString()));
						}
						else
						{
							if(LoadSubscriberMapping.KPIUsageThresholdZeroCount.containsKey(datas[1]))
							{
								Long count = LoadSubscriberMapping.KPIUsageThresholdZeroCount.get(datas[1]);
								LoadSubscriberMapping.KPIUsageThresholdZeroCount.put(datas[1],count + 1L);					
							}
							else
							{
								LoadSubscriberMapping.KPIUsageThresholdZeroCount.put(datas[1], 1L);						
							}
						}
					}
				}	
			}			
		  }
		  br.close();
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
	private void loadCreditLimitInputRelation() {
		// TODO Auto-generated method stub
		long SERVICE_CLASS = 0;
		long SUBSCRIBER_BALANCE = 0;
		
		long MINPAY_FACTOR = 0;
		long REFERENCE_CREDIT_LIMIT_ZERO = 0;
		long SUBSCRIBER_BALANCE_ZERO = 0;
		long AVAILABLE_CREDIT_LIMIT_ZERO = 0;
		long MINPAY_FACTOR_ZERO = 0;
		long USAGE_VALUE_ZERO = 0;
		long REFERENCE_CREDIT_LIMIT99 = 0;
		long SUBSCRIBER_BALANCE99 = 0;
		long AVAILABLE_CREDIT_LIMIT99 = 0;
		long MINPAY_FACTOR99 = 0;
		long USAGE_VALUE99 = 0;
		
		long REFERENCE_CREDIT_LIMIT_SRC = 0;
		long AVAILABLE_CREDIT_LIMIT_SRC = 0;
		long USAGE_VALUE_SRC = 0;
		
		long REFERENCE_CREDIT_LIMIT_DUMMY = 0;
		long AVAILABLE_CREDIT_LIMIT_DUMMY = 0;
		long USAGE_VALUE_DUMMY = 0;
				
		long NOTIFICATION_70 = 0;
		long NOTIFICATION_80 = 0;
		long NOTIFICATION_90 = 0;
		
		BigDecimal REFERENCE_CREDIT_LIMIT_Balance = new BigDecimal("0");
		BigDecimal SUBSCRIBER_BALANCE_Balance = new BigDecimal("0");
		BigDecimal AVAILABLE_CREDIT_LIMIT_Balance = new BigDecimal("0");
		BigDecimal MINPAY_FACTOR_Balance = new BigDecimal("0");
		BigDecimal USAGE_VALUE_Balance = new BigDecimal("0");
		BigDecimal REFERENCE_CREDIT_LIMIT99_Balance = new BigDecimal("0");
		BigDecimal SUBSCRIBER_BALANCE99_Balance = new BigDecimal("0");
		BigDecimal AVAILABLE_CREDIT_LIMIT99_Balance = new BigDecimal("0");
		BigDecimal MINPAY_FACTOR99_Balance = new BigDecimal("0");
		BigDecimal USAGE_VALUE99_Balance = new BigDecimal("0");
		
		for(Entry<String, String> etr : LoadSubscriberMapping.mainCreditLimitInput.entrySet())
		{
			//if(LoadSubscriberMapping.mainAccountInput.contains(etr.getKey()))
			{
				Map<String, String> creditLimit = new HashMap<String, String>();
				String MSISDN = etr.getValue().split(",",-1)[0];
				
				creditLimit.put("MSISDN", MSISDN);
				//if("66394899".equals(MSISDN))
				//	System.out.println("VipinSIngh");
				
				creditLimit.put("SERVICE_CLASS", etr.getValue().split(",",-1)[1]);
				creditLimit.put("REFERENCE_CREDIT_LIMIT", etr.getValue().split(",",-1)[2]);
				creditLimit.put("SUBSCRIBER_BALANCE", etr.getValue().split(",",-1)[3]);
				creditLimit.put("AVAILABLE_CREDIT_LIMIT", etr.getValue().split(",",-1)[4]);
				creditLimit.put("MINPAY_FACTOR", etr.getValue().split(",",-1)[5]);
				creditLimit.put("Notification_70", etr.getValue().split(",",-1)[6]);
				creditLimit.put("Notification_80", etr.getValue().split(",",-1)[7]);
				creditLimit.put("Notification_90", etr.getValue().split(",",-1)[8]);
				creditLimit.put("USAGE_VALUE", etr.getValue().split(",",-1)[9]);
				
				
				if(creditLimit.size() !=0)
				{	
					if(creditLimit.containsKey("SERVICE_CLASS") && !creditLimit.get("SERVICE_CLASS").isEmpty())
					{
						if(SourceServiceClass.containsKey(etr.getValue().split(",",-1)[1]))
						{
							Long temp = SourceServiceClass.get(etr.getValue().split(",",-1)[1]);
							SourceServiceClass.put(etr.getValue().split(",",-1)[1], temp+1);
						}
						else
						{
							SourceServiceClass.put(etr.getValue().split(",",-1)[1], 1L);
						}					
					}
					if(creditLimit.containsKey("REFERENCE_CREDIT_LIMIT") && creditLimit.get("REFERENCE_CREDIT_LIMIT").equals("99999999"))
					{	
						REFERENCE_CREDIT_LIMIT_SRC +=1;
						REFERENCE_CREDIT_LIMIT99 += 1;
						if(!creditLimit.get("REFERENCE_CREDIT_LIMIT").isEmpty())
						{
							REFERENCE_CREDIT_LIMIT99_Balance = REFERENCE_CREDIT_LIMIT99_Balance.add(new BigDecimal(creditLimit.get("REFERENCE_CREDIT_LIMIT")));
						}
					}
					else
					{
						REFERENCE_CREDIT_LIMIT_SRC +=1;
						if(creditLimit.get("REFERENCE_CREDIT_LIMIT").equals("0") && MSISDN.startsWith("100"))
							REFERENCE_CREDIT_LIMIT_DUMMY +=1;
						else if(creditLimit.get("REFERENCE_CREDIT_LIMIT").equals("0"))
							REFERENCE_CREDIT_LIMIT_ZERO +=1;
						
						if(!creditLimit.get("REFERENCE_CREDIT_LIMIT").isEmpty())
						{
							REFERENCE_CREDIT_LIMIT_Balance = REFERENCE_CREDIT_LIMIT_Balance.add(new BigDecimal(creditLimit.get("REFERENCE_CREDIT_LIMIT")));
						}
					}
					
					if(creditLimit.containsKey("AVAILABLE_CREDIT_LIMIT") && creditLimit.get("AVAILABLE_CREDIT_LIMIT").equals("99999999.000"))
					{
						AVAILABLE_CREDIT_LIMIT_SRC +=1;
						AVAILABLE_CREDIT_LIMIT99 +=1;
						if(!creditLimit.get("AVAILABLE_CREDIT_LIMIT").isEmpty())
						{
							AVAILABLE_CREDIT_LIMIT99_Balance = AVAILABLE_CREDIT_LIMIT99_Balance.add(new BigDecimal(creditLimit.get("AVAILABLE_CREDIT_LIMIT")));
						}
					}
					else
					{
						AVAILABLE_CREDIT_LIMIT_SRC +=1;
						if(creditLimit.get("AVAILABLE_CREDIT_LIMIT").equals("0.000") && MSISDN.startsWith("100") && LoadSubscriberMapping.CompleteListToMigrate.contains(MSISDN))
							AVAILABLE_CREDIT_LIMIT_DUMMY +=1;
						else if(creditLimit.get("AVAILABLE_CREDIT_LIMIT").equals("0.000") && LoadSubscriberMapping.CompleteListToMigrate.contains(MSISDN))
							AVAILABLE_CREDIT_LIMIT_ZERO +=1;
						
						if(!creditLimit.get("AVAILABLE_CREDIT_LIMIT").isEmpty())
						{
							AVAILABLE_CREDIT_LIMIT_Balance = AVAILABLE_CREDIT_LIMIT_Balance.add(new BigDecimal(creditLimit.get("AVAILABLE_CREDIT_LIMIT")));
						}
					}
					
					if(creditLimit.containsKey("SUBSCRIBER_BALANCE") && creditLimit.get("SUBSCRIBER_BALANCE").equals("99999999.000"))
					{
						SUBSCRIBER_BALANCE99 +=1;
						if(!creditLimit.get("SUBSCRIBER_BALANCE").isEmpty())
						{
							SUBSCRIBER_BALANCE99_Balance = SUBSCRIBER_BALANCE99_Balance.add(new BigDecimal(creditLimit.get("SUBSCRIBER_BALANCE")));
						}
					}
					else
					{
						if(creditLimit.get("SUBSCRIBER_BALANCE").equals("0"))
							SUBSCRIBER_BALANCE_ZERO +=1;
						else
							SUBSCRIBER_BALANCE +=1;
						if(!creditLimit.get("SUBSCRIBER_BALANCE").isEmpty())
						{
							SUBSCRIBER_BALANCE_Balance = SUBSCRIBER_BALANCE_Balance.add(new BigDecimal(creditLimit.get("SUBSCRIBER_BALANCE")));
						}
					}
					
					if(creditLimit.containsKey("MINPAY_FACTOR") && creditLimit.get("MINPAY_FACTOR").equals("99999999.000"))
					{
						MINPAY_FACTOR99 +=1;
						if(!creditLimit.get("MINPAY_FACTOR").isEmpty())
						{
							MINPAY_FACTOR99_Balance = MINPAY_FACTOR99_Balance.add(new BigDecimal(creditLimit.get("MINPAY_FACTOR")));
						}
					}
					else
					{
						if(creditLimit.get("MINPAY_FACTOR").equals("0"))
							MINPAY_FACTOR_ZERO +=1;
						else
							MINPAY_FACTOR +=1;
						if(!creditLimit.get("MINPAY_FACTOR").isEmpty())
						{
							MINPAY_FACTOR_Balance = MINPAY_FACTOR_Balance.add(new BigDecimal(creditLimit.get("MINPAY_FACTOR")));
						}
					}
					if(creditLimit.containsKey("USAGE_VALUE") && creditLimit.get("USAGE_VALUE").equals("99999999.000"))
					{
						USAGE_VALUE99 += 1;
						if(!creditLimit.get("USAGE_VALUE").isEmpty())
						{
							USAGE_VALUE99_Balance = USAGE_VALUE99_Balance.add(new BigDecimal(creditLimit.get("USAGE_VALUE")));
						}
						USAGE_VALUE_SRC +=1;
					}
					else
					{
						if(creditLimit.get("USAGE_VALUE").equals("0.000") && MSISDN.startsWith("100") && LoadSubscriberMapping.CompleteListToMigrate.contains(MSISDN))
							USAGE_VALUE_DUMMY +=1;
						else if(creditLimit.get("USAGE_VALUE").equals("0.000"))
							USAGE_VALUE_ZERO +=1;
						
						USAGE_VALUE_SRC +=1;
						
						if(!creditLimit.get("USAGE_VALUE").isEmpty())
						{
							USAGE_VALUE_Balance = USAGE_VALUE_Balance.add(new BigDecimal(creditLimit.get("USAGE_VALUE")));
						}
					}				
					NOTIFICATION_70 +=1;
					NOTIFICATION_80 +=1;
					NOTIFICATION_90 +=1;				
				}
			}
		}
		//KPICreditLimitInputCount.put("SERVICE_CLASS", SERVICE_CLASS);
		
		KPICreditLimitInputCount.put("REFERENCE_CREDIT_LIMIT_SRC", REFERENCE_CREDIT_LIMIT_SRC);
		KPICreditLimitInputCount.put("REFERENCE_CREDIT_LIMIT_ZERO", REFERENCE_CREDIT_LIMIT_ZERO);
		KPICreditLimitInputCount.put("REFERENCE_CREDIT_LIMIT_DUMMY", REFERENCE_CREDIT_LIMIT_DUMMY);
		KPICreditLimitInputCount.put("REFERENCE_CREDIT_LIMIT99", REFERENCE_CREDIT_LIMIT99);
		
		KPICreditLimitInputCount.put("SUBSCRIBER_BALANCE", SUBSCRIBER_BALANCE);
		KPICreditLimitInputCount.put("SUBSCRIBER_BALANCE_ZERO", SUBSCRIBER_BALANCE_ZERO);
		KPICreditLimitInputCount.put("SUBSCRIBER_BALANCE99", SUBSCRIBER_BALANCE99);
		
		KPICreditLimitInputCount.put("AVAILABLE_CREDIT_LIMIT_SRC", AVAILABLE_CREDIT_LIMIT_SRC);
		KPICreditLimitInputCount.put("AVAILABLE_CREDIT_LIMIT_DUMMY", AVAILABLE_CREDIT_LIMIT_DUMMY);
		KPICreditLimitInputCount.put("AVAILABLE_CREDIT_LIMIT_ZERO", AVAILABLE_CREDIT_LIMIT_ZERO);
		KPICreditLimitInputCount.put("AVAILABLE_CREDIT_LIMIT99", AVAILABLE_CREDIT_LIMIT99);
		
		KPICreditLimitInputCount.put("MINPAY_FACTOR", MINPAY_FACTOR);
		KPICreditLimitInputCount.put("MINPAY_FACTOR_ZERO", MINPAY_FACTOR_ZERO);
		KPICreditLimitInputCount.put("MINPAY_FACTOR99", MINPAY_FACTOR99);
		
		KPICreditLimitInputCount.put("USAGE_VALUE_SRC", USAGE_VALUE_SRC);
		KPICreditLimitInputCount.put("USAGE_VALUE_ZERO", USAGE_VALUE_ZERO);
		KPICreditLimitInputCount.put("USAGE_VALUE99", USAGE_VALUE99);
		KPICreditLimitInputCount.put("USAGE_VALUE_DUMMY", USAGE_VALUE_DUMMY);
		
		
		KPICreditLimitInputCount.put("NOTIFICATION_70", NOTIFICATION_70);
		KPICreditLimitInputCount.put("NOTIFICATION_80", NOTIFICATION_80);
		KPICreditLimitInputCount.put("NOTIFICATION_90", NOTIFICATION_90);
		
		
		KPICreditLimitInputBalance.put("REFERENCE_CREDIT_LIMIT", REFERENCE_CREDIT_LIMIT_Balance);
		KPICreditLimitInputBalance.put("SUBSCRIBER_BALANCE", SUBSCRIBER_BALANCE_Balance);
		KPICreditLimitInputBalance.put("AVAILABLE_CREDIT_LIMIT", AVAILABLE_CREDIT_LIMIT_Balance);
		KPICreditLimitInputBalance.put("MINPAY_FACTOR", MINPAY_FACTOR_Balance);
		KPICreditLimitInputBalance.put("USAGE_VALUE", USAGE_VALUE_Balance);
		KPICreditLimitInputBalance.put("REFERENCE_CREDIT_LIMIT99", REFERENCE_CREDIT_LIMIT99_Balance);
		KPICreditLimitInputBalance.put("SUBSCRIBER_BALANCE99", SUBSCRIBER_BALANCE99_Balance);
		KPICreditLimitInputBalance.put("AVAILABLE_CREDIT_LIMIT99", AVAILABLE_CREDIT_LIMIT99_Balance);
		KPICreditLimitInputBalance.put("MINPAY_FACTOR99", MINPAY_FACTOR99_Balance);
		KPICreditLimitInputBalance.put("USAGE_VALUE99", USAGE_VALUE99_Balance);
		
	}

	private void loadMainOfferInputRelation() {
		// TODO Auto-generated method stub
	   try {
		  
		  BufferedReader br = new BufferedReader(new FileReader(inputPath + "/Main_Offer.csv"));
		  String line = "";
		  br.readLine();
		  while ((line = br.readLine()) != null) {
			String datas[] = line.split(",",-1);
			if(LoadSubscriberMapping.mainOfferInputRelation.containsKey(datas[0]))
			{
				Set<String> child = new HashSet<String>(LoadSubscriberMapping.mainOfferInputRelation.get(datas[0]));
				child.add(line);
				LoadSubscriberMapping.mainOfferInputRelation.put(datas[0],child);					
			}
			else
			{
				Set<String> child = new HashSet<String>();
				child.add(line);
				LoadSubscriberMapping.mainOfferInputRelation.put(datas[0],child);
			}
			
			if(LoadSubscriberMapping.KPImainOfferInput.containsKey(datas[1]))
			{
				Long temp = LoadSubscriberMapping.KPImainOfferInput.get(datas[1]);
				LoadSubscriberMapping.KPImainOfferInput.put(datas[1], temp+1);
				if( Long.parseLong(datas[1]) >= 3000000L)
				{
					//for UC and UT Value from input
					if(LoadSubscriberMapping.KPIUsageCounterInputCount.containsKey(datas[1]))
					{
						Long currentBalance = LoadSubscriberMapping.KPIUsageCounterInputCount.get(datas[1]) + 1;
						LoadSubscriberMapping.KPIUsageCounterInputCount.put(datas[1],currentBalance);					
					}
					else
					{
						LoadSubscriberMapping.KPIUsageCounterInputCount.put(datas[1],1L);
					}
					if(LoadSubscriberMapping.KPIUsageThresholdInputCount.containsKey(datas[1]))
					{
						Long currentBalance = LoadSubscriberMapping.KPIUsageThresholdInputCount.get(datas[1]) + 1;
						LoadSubscriberMapping.KPIUsageThresholdInputCount.put(datas[1],currentBalance);					
					}
					else
					{
						LoadSubscriberMapping.KPIUsageThresholdInputCount.put(datas[1],1L);
					}
					
					//for UC and UT valid Value from input
					if(!datas[4].isEmpty() && Long.parseLong(datas[4]) > 0 )
					{
						//LoadSubscriberMapping.KPIUsageThresholdBalance
						if(LoadSubscriberMapping.KPIUsageCounterBalance.containsKey(datas[1]))
						{
							UsageCounterMSISDN.add(datas[0] + ":" + datas[1]);
							
							Long count = LoadSubscriberMapping.KPIUsageCounterCount.get(datas[1]);
							LoadSubscriberMapping.KPIUsageCounterCount.put(datas[1],count + 1L);
							
							Long currentBalance = LoadSubscriberMapping.KPIUsageCounterBalance.get(datas[1]) + Long.parseLong(datas[4]);
							LoadSubscriberMapping.KPIUsageCounterBalance.put(datas[1],currentBalance);					
						}
						else
						{
							UsageCounterMSISDN.add(datas[0] + ":" + datas[1]);
							LoadSubscriberMapping.KPIUsageCounterCount.put(datas[1],1L);
							LoadSubscriberMapping.KPIUsageCounterBalance.put(datas[1],Long.parseLong(datas[4]));
						}
						
						LoadSubscriberMapping.KPIUsageCounterTotalBalance = LoadSubscriberMapping.KPIUsageCounterTotalBalance.add(new BigDecimal(datas[4].toString()));
					}
					else
					{
						if(LoadSubscriberMapping.CompleteListToMigrate.contains(datas[0]))
						{
							if(LoadSubscriberMapping.KPIUsageCounterZeroCount.containsKey(datas[1]))
							{
								Long count = LoadSubscriberMapping.KPIUsageCounterZeroCount.get(datas[1]);
								LoadSubscriberMapping.KPIUsageCounterZeroCount.put(datas[1],count + 1L);					
							}
							else
							{
								LoadSubscriberMapping.KPIUsageCounterZeroCount.put(datas[1], 1L);						
							}
						}
					}
					if(!datas[5].isEmpty() && Long.parseLong(datas[5]) > 0)
					{
						if(LoadSubscriberMapping.KPIUsageThresholdBalance.containsKey(datas[1]))
						{
							UsageThresholdMSISDN.add(datas[0] + ":" + datas[1]);
							Long count = LoadSubscriberMapping.KPIUsageThresholdCount.get(datas[1]);
							LoadSubscriberMapping.KPIUsageThresholdCount.put(datas[1],count + 1L);
							
							Long currentBalance = LoadSubscriberMapping.KPIUsageThresholdBalance.get(datas[1]) + Long.parseLong(datas[5]);
							LoadSubscriberMapping.KPIUsageThresholdBalance.put(datas[1],currentBalance);					
						}
						else
						{
							UsageThresholdMSISDN.add(datas[0] + ":" + datas[1]);
							LoadSubscriberMapping.KPIUsageThresholdCount.put(datas[1], 1L);
							
							LoadSubscriberMapping.KPIUsageThresholdBalance.put(datas[1],Long.parseLong(datas[5]));
						}
						LoadSubscriberMapping.KPIUsageThresholdTotalBalance = LoadSubscriberMapping.KPIUsageThresholdTotalBalance.add(new BigDecimal(datas[5].toString()));
					}
					else
					{
						if(LoadSubscriberMapping.KPIUsageThresholdZeroCount.containsKey(datas[1]))
						{
							Long count = LoadSubscriberMapping.KPIUsageThresholdZeroCount.get(datas[1]);
							LoadSubscriberMapping.KPIUsageThresholdZeroCount.put(datas[1],count + 1L);					
						}
						else
						{
							LoadSubscriberMapping.KPIUsageThresholdZeroCount.put(datas[1], 1L);						
						}
					}
				}
			}
			else
			{
				LoadSubscriberMapping.KPImainOfferInput.put(datas[1], 1L);
				if( Long.parseLong(datas[1]) >= 3000000L)
				{
					//for UC and UT Value from input
					if(LoadSubscriberMapping.KPIUsageCounterInputCount.containsKey(datas[1]))
					{
						Long currentBalance = LoadSubscriberMapping.KPIUsageCounterInputCount.get(datas[1]) + 1;
						LoadSubscriberMapping.KPIUsageCounterInputCount.put(datas[1],currentBalance);					
					}
					else
					{
						LoadSubscriberMapping.KPIUsageCounterInputCount.put(datas[1],1L);
					}
					if(LoadSubscriberMapping.KPIUsageThresholdInputCount.containsKey(datas[1]))
					{
						Long currentBalance = LoadSubscriberMapping.KPIUsageThresholdInputCount.get(datas[1]) + 1;
						LoadSubscriberMapping.KPIUsageThresholdInputCount.put(datas[1],currentBalance);					
					}
					else
					{
						LoadSubscriberMapping.KPIUsageThresholdInputCount.put(datas[1],1L);
					}
					
					//for UC and UT valid Value from input
					if(!datas[4].isEmpty() && Long.parseLong(datas[4]) > 0)
					{
						if(LoadSubscriberMapping.KPIUsageCounterBalance.containsKey(datas[1]))
						{
							UsageCounterMSISDN.add(datas[0] + ":" + datas[1]);
							
							Long count = LoadSubscriberMapping.KPIUsageCounterCount.get(datas[1]);
							LoadSubscriberMapping.KPIUsageCounterCount.put(datas[1],count + 1L);
							Long currentBalance = LoadSubscriberMapping.KPIUsageCounterBalance.get(datas[1]) + Long.parseLong(datas[4]);
							LoadSubscriberMapping.KPIUsageCounterBalance.put(datas[1],currentBalance);					
						}
						else
						{
							LoadSubscriberMapping.KPIUsageCounterCount.put(datas[1], 1L);
							UsageCounterMSISDN.add(datas[0] + ":" + datas[1]);
							LoadSubscriberMapping.KPIUsageCounterBalance.put(datas[1],Long.parseLong(datas[4]));
						}
						LoadSubscriberMapping.KPIUsageCounterTotalBalance = LoadSubscriberMapping.KPIUsageCounterTotalBalance.add(new BigDecimal(datas[4].toString()));
					}
					else
					{
						if(LoadSubscriberMapping.CompleteListToMigrate.contains(datas[0]))
						{
							if(LoadSubscriberMapping.KPIUsageCounterZeroCount.containsKey(datas[1]))
							{
								Long count = LoadSubscriberMapping.KPIUsageCounterZeroCount.get(datas[1]);
								LoadSubscriberMapping.KPIUsageCounterZeroCount.put(datas[1],count + 1L);					
							}
							else
							{
								LoadSubscriberMapping.KPIUsageCounterZeroCount.put(datas[1], 1L);						
							}
						}
					}
					if(!datas[5].isEmpty() && Long.parseLong(datas[5]) > 0)
					{
						if(LoadSubscriberMapping.KPIUsageThresholdBalance.containsKey(datas[1]))
						{
							UsageThresholdMSISDN.add(datas[0] + ":" + datas[1]);
							Long count = LoadSubscriberMapping.KPIUsageThresholdCount.get(datas[1]);
							LoadSubscriberMapping.KPIUsageThresholdCount.put(datas[1],count + 1L);
							
							Long currentBalance = LoadSubscriberMapping.KPIUsageThresholdBalance.get(datas[1]) + Long.parseLong(datas[5]);
							LoadSubscriberMapping.KPIUsageThresholdBalance.put(datas[1],currentBalance);					
						}
						else
						{
							LoadSubscriberMapping.KPIUsageThresholdCount.put(datas[1], 1L);
							LoadSubscriberMapping.KPIUsageThresholdCount.put(datas[1],1L);
							
							UsageThresholdMSISDN.add(datas[0] + ":" + datas[1]);
							LoadSubscriberMapping.KPIUsageThresholdBalance.put(datas[1],Long.parseLong(datas[5]));
						}
						LoadSubscriberMapping.KPIUsageThresholdTotalBalance = LoadSubscriberMapping.KPIUsageThresholdTotalBalance.add(new BigDecimal(datas[5].toString()));
					}
					else
					{
						if(LoadSubscriberMapping.KPIUsageThresholdZeroCount.containsKey(datas[1]))
						{
							Long count = LoadSubscriberMapping.KPIUsageThresholdZeroCount.get(datas[1]);
							LoadSubscriberMapping.KPIUsageThresholdZeroCount.put(datas[1],count + 1L);					
						}
						else
						{
							LoadSubscriberMapping.KPIUsageThresholdZeroCount.put(datas[1], 1L);						
						}
					}
				}
			}
		  }
		  br.close();
	   }
	   catch(Exception ex)
	   {
		   ex.printStackTrace();
	   }
	}

	private void loadingSharedOfferRelation() {
		// TODO Auto-generated method stub
		try {
			BufferedReader br = new BufferedReader(new FileReader(inputPath + "/Shared_Offer_Relation.csv"));
		    String line;
		    br.readLine();
		    while ((line = br.readLine()) != null) {
		    	String datas[] = line.split(",",-1);
		    	if(LoadSubscriberMapping.sharedInputRelation.containsKey(datas[2]))
				{
					Set<String> child = new HashSet<String>(LoadSubscriberMapping.sharedInputRelation.get(datas[2]));
					child.add(line);
					LoadSubscriberMapping.sharedInputRelation.put(datas[2],child);
					
				}
				else
				{
					Set<String> child = new HashSet<String>();
					child.add(line);
					LoadSubscriberMapping.sharedInputRelation.put(datas[2],child);
				}
				
				if(LoadSubscriberMapping.KPIsharedOfferInput.containsKey(datas[1]))
				{
					Long temp = LoadSubscriberMapping.KPIsharedOfferInput.get(datas[1]);
					LoadSubscriberMapping.KPIsharedOfferInput.put(datas[1], temp+1);
					/*if( Long.parseLong(datas[1]) >= 3000000L)
					{
						LoadSubscriberMapping.KPIUsageCounterCount.put(datas[1], temp+1);
						LoadSubscriberMapping.KPIUsageThresholdCount.put(datas[1], temp+1);
					}*/
				}
				else
				{
					LoadSubscriberMapping.KPIsharedOfferInput.put(datas[1], 1L);
					/*if( Long.parseLong(datas[1]) >= 3000000L)
					{
						LoadSubscriberMapping.KPIUsageCounterCount.put(datas[1], 1L);
						LoadSubscriberMapping.KPIUsageThresholdCount.put(datas[1], 1L);
					}*/
				}
			 }
		     br.close();		     
		  } 
		  catch (Exception e) {
		  // TODO Auto-generated catch block
		  e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		String configPath, dataFolderPath, workingMode = null;
		
		dataFolderPath = "C:\\Ericsson\\MyWorkingProject\\Charging_System\\dm_cs_2020_zain_bahrain\\dev\\src\\input\\";
		configPath = "C:\\Ericsson\\MyWorkingProject\\Charging_System\\dm_cs_2020_zain_bahrain\\dev\\src\\config\\";
		workingMode = "C:\\Ericsson\\MyWorkingProject\\Charging_System\\dm_cs_2020_zain_bahrain\\dev\\src\\working\\";
		String tempPath = "C:\\Ericsson\\MyWorkingProject\\Charging_System\\dm_cs_2020_zain_bahrain\\dev\\src\\";
		//String filesToBeInserted = "INFILE_Subscriber_Balances.csv.gz,INFILE_Subscriber_cug_cli.csv.gz,INFILE_Subscriber_USMS.csv.gz";
		//String filesToBeInserted = "sdp01_subscriber_balances_dump.csv.gz,sdp01_subscriber_cugcli_dump.csv.gz,sdp01_subscriber_usms_dump.csv.gz";
		String JsonFileName = "Zain_Bahrain_SDP.json";
		LoadSubscriberMapping lsm = new LoadSubscriberMapping(JsonFileName, tempPath,configPath,dataFolderPath);
		lsm.intializeMapping();
	}

	
	
}
