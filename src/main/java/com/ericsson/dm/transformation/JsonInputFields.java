package com.ericsson.dm.transformation;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.ericsson.dm.inititialization.LoadSubscriberMapping;
import com.ericsson.dm.transformation.JsonInputFields.ADDON_OFFER;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class JsonInputFields {
	private String inputJsonInput;
	public String MSISDN;
	public String OfferStartDateTime;
	public boolean isAddonProductPrivate;
	public boolean AccountSfeeFlag;
	public Map<String, Object>  MAIN_OFFER; 
	public Map<String, Object>  IMPLICIT_SHARED_OFFER;
	public Map<String, Object>  SHARED_OFFER_RELATION;
	public Map<String, String>  ACCOUNT;
	public Map<String, Object> ADDON_OFFER;
	public Map<String, String> CREDIT_LIMIT;
	
	
	final static Logger LOG = Logger.getLogger(LoadSubscriberMapping.class);
	
	public JsonInputFields(String inputJsonInput) {
		// TODO Auto-generated constructor stub
		this.inputJsonInput = inputJsonInput;
		this.MAIN_OFFER = new HashMap<String, Object>();
		this.IMPLICIT_SHARED_OFFER = new HashMap<String, Object>();
		this.SHARED_OFFER_RELATION = new HashMap<String, Object>();
		this.CREDIT_LIMIT = new HashMap<String, String>();
		this.ADDON_OFFER = new HashMap<String, Object>();
		this.ACCOUNT = new HashMap<String, String>();
		this.AccountSfeeFlag = false;
		this.isAddonProductPrivate = false;
	}
	
	public void intializeInputJson() {
		// TODO Auto-generated method stub
		try 
		  { 
			  Object jsonObject = new JSONParser().parse(inputJsonInput); 
			  JSONObject jo = (JSONObject) jsonObject;
		  
			  for (Object jsonKey : jo.keySet()) 
			  { 
				  String key = (String) jsonKey;
			      Object value = jo.get(key); 
			      if(key.toUpperCase().equals("BODY"))
			      { 
			    	  Object jsonObject1 = new JSONParser().parse(value.toString()); 
					  JSONObject jo1 = (JSONObject) jsonObject1;
				  
					  for (Object jsonKey1 : jo1.keySet()) 
					  { 
						  String key1 = (String) jsonKey1;
					      //System.out.println(key1); 
					      Object value1 = jo1.get(key1);
					      if(key1.toUpperCase().equals("ACCOUNT")) 
					      {
					    	  
					    	  //MAIN_OFFER.putAll(PopulateKeyValueMapping(value1.toString(), "ACCOUNT"));
					    	  JsonParser parser = new JsonParser();
					  		  String[] AccountData = value1.toString().split(",");//.replaceAll("[{", "").replaceAll("}]", "").split(",");
					  		  
					  		  //System.out.println(AccountData.length);
					  		  for (String str : AccountData){
					  			  //System.out.println(str);
					  			  String[] AccountValue = (str.replaceAll("\\[","").replaceAll("\\{","")
					  					  .replaceAll("\"", "").replaceAll("\\}", "").replaceAll("\\]", "")).split(":");
					  			  ACCOUNT.put(AccountValue[0], AccountValue[1]);
					  			  if(AccountValue[0].equals("sfee_expiry_date") && Integer.parseInt(AccountValue[1]) > 0)
					  			  {
					  				  this.AccountSfeeFlag = true;
					  			  }
						      } 					  		  
					      }
					      if(key1.toUpperCase().equals("ADDON_OFFER")) 
					      {
					    	  ADDON_OFFER.putAll(PopulateKeyValueMapping(value1.toString(), "ADDON_OFFER"));
					      }
					      if(key1.toUpperCase().equals("MAIN_OFFER")) 
					      {
					    	  MAIN_OFFER.putAll(PopulateKeyValueMapping(value1.toString(), "MAIN_OFFER"));
					    	  //System.out.println("MAIN_OFFER: " + MAIN_OFFER.size());
					      }		      
					      if(key1.toUpperCase().equals("IMPLICIT_SHARED_OFFER")) 
					      {
					    	  IMPLICIT_SHARED_OFFER.putAll(PopulateKeyValueMapping(value1.toString(), "IMPLICIT_SHARED_OFFER"));
					    	  //System.out.println("IMPLICIT_SHARED_OFFER: " + IMPLICIT_SHARED_OFFER.size());
					      }
					      if(key1.toUpperCase().equals("SHARED_OFFER_RELATION")) 
					      {
					    	  SHARED_OFFER_RELATION.putAll(PopulateKeyValueMapping(value1.toString(), "SHARED_OFFER_RELATION"));
					    	  //System.out.println("SHARED_OFFER_RELATION: " + SHARED_OFFER_RELATION.size());
					      } 
					      if(key1.toUpperCase().equals("CREDIT_LIMIT")) 
					      {
					    	  //CREDIT_LIMIT.putAll(PopulateKeyValueMapping(value1.toString(), "CREDIT_LIMIT"));
					    	  //System.out.println("SHARED_OFFER_RELATION: " + SHARED_OFFER_RELATION.size());
					    	  
					    	  JsonParser parser = new JsonParser();
					  		  String[] Data = value1.toString().split(",");//.replaceAll("[{", "").replaceAll("}]", "").split(",");
					  		  
					  		  //System.out.println(AccountData.length);
					  		  for (String str : Data){
					  			  //System.out.println(str);
					  			  String[] creditValue = (str.replaceAll("\\[","").replaceAll("\\{","")
					  					  .replaceAll("\"", "").replaceAll("\\}", "").replaceAll("\\]", "")).split(":",-1);
					  			  //System.out.println(str);
					  			  CREDIT_LIMIT.put(creditValue[0], creditValue[1]);
					  			  
						      }
					      } 
					  }
			      }			      
			  } 
		  }
		  catch (Exception e) 
		  {
			   // TODO Auto-generated catch block 
			 LOG.error("Exception occured ", e); 
		  }
	}
	
	public Map< String,IMPLICIT_SHARED_OFFER> PopulateKeyValueMapping_Actual(String value, String inputFileType) {
		// TODO Auto-generated method stub
		HashMap<String, IMPLICIT_SHARED_OFFER> OfferMap = new HashMap<String, IMPLICIT_SHARED_OFFER>();
		JsonParser parser = new JsonParser();
		JsonArray object = (JsonArray) parser.parse(value);
		
		 //since you know it's a JsonObject
		for (JsonElement element : object) {
			JsonObject obj = element.getAsJsonObject();
			//System.out.println(element);
			Set<Map.Entry<String, JsonElement>> entries = obj.entrySet();//will return members of your object
			if(inputFileType.equals("IMPLICIT_SHARED_OFFER"))
			{	
				IMPLICIT_SHARED_OFFER iso = new IMPLICIT_SHARED_OFFER();
				String Offer_ID_key = "";
				for (Map.Entry<String, JsonElement> entry: entries) {
				    if(entry.getKey().toUpperCase().equals("SHARED_OFFER_ID"))
			    		iso.Shared_Offer_ID = Offer_ID_key = entry.getValue().toString();
			    	if(entry.getKey().toUpperCase().equals("USAGE_THRESHOLD"))
			    		iso.Usage_Threshold = entry.getValue().toString();
			    	if(entry.getKey().toUpperCase().equals("START_DATE"))
			    		iso.Start_Date = entry.getValue().toString();
			    	if(entry.getKey().toUpperCase().equals("END_DATE"))
			    		iso.End_Date = entry.getValue().toString();
			    	if(entry.getKey().toUpperCase().equals("USAGE_VALUE"))
			    		iso.Usage_Value = entry.getValue().toString();
				}
				OfferMap.put(Offer_ID_key, iso);				
			}
		}
		return OfferMap;
	}
	
	public Map< String,Object> PopulateKeyValueMapping(String value, String inputFileType) {
		// TODO Auto-generated method stub
		HashMap<String, Object> OfferMap = new HashMap<String, Object>();
		JsonParser parser = new JsonParser();
		JsonArray object = (JsonArray) parser.parse(value);
		
		 //since you know it's a JsonObject
		for (JsonElement element : object) {
			JsonObject obj = element.getAsJsonObject();
			//System.out.println(element);
			Set<Map.Entry<String, JsonElement>> entries = obj.entrySet();//will return members of your object
			if(inputFileType.equals("CREDIT_LIMIT"))
			{				
				CREDIT_LIMIT iso = new CREDIT_LIMIT();
				String Account_Msisdn = "";				
				
				for (Map.Entry<String, JsonElement> entry: entries) {
					
					if(entry.getKey().toUpperCase().equals("ACCOUNT_MSISDN"))
			    		iso.Account_Msisdn = Account_Msisdn = entry.getValue().toString().replaceAll("\"", "");
				    if(entry.getKey().toUpperCase().equals("SERVICE_CLASS"))
			    		iso.service_class = entry.getValue().toString().replaceAll("\"", "");
			    	if(entry.getKey().toUpperCase().equals("SUBCCLIMIT"))
			    		iso.reference_credit_limit = entry.getValue().toString().replaceAll("\"", "");
			    	if(entry.getKey().toUpperCase().equals("SUBTOTAL"))
			    		iso.subscriber_balance = entry.getValue().toString().replaceAll("\"", "");
			    	if(entry.getKey().toUpperCase().equals("AVAILABLE_LIMIT"))
			    		iso.available_credit_limit = entry.getValue().toString().replaceAll("\"", "");
			    	if(entry.getKey().toUpperCase().equals("MIN_PAY_FACTOR"))
			    		iso.minpay_factor = entry.getValue().toString().replaceAll("\"", "");
			    	if(entry.getKey().toUpperCase().equals("NOTIFICATION_70"))
			    		iso.notification_7 = entry.getValue().toString().replaceAll("\"", "");
			    	if(entry.getKey().toUpperCase().equals("NOTIFICATION_80"))
			    		iso.notification_8 = entry.getValue().toString().replaceAll("\"", "");
			    	if(entry.getKey().toUpperCase().equals("NOTIFICATION_90"))
			    		iso.notification_9 = entry.getValue().toString().replaceAll("\"", "");
			    	if(entry.getKey().toUpperCase().equals("CDR_USAGE"))
			    		iso.usage_value = entry.getValue().toString().replaceAll("\"", "");
			    	
				}
				OfferMap.put(Account_Msisdn, iso);				
			}
			if(inputFileType.equals("ADDON_OFFER"))
			{				
				ADDON_OFFER iso = new ADDON_OFFER();
				String Offer_ID_key = "";
				for (Map.Entry<String, JsonElement> entry: entries) {
				    if(entry.getKey().toUpperCase().equals("OFFER_ID"))
			    		iso.Offer_ID = entry.getValue().toString().replaceAll("\"", "");
			    	if(entry.getKey().toUpperCase().equals("USAGE_THRESHOLD"))
			    		iso.Usage_Threshold = entry.getValue().toString().replaceAll("\"", "");
			    	if(entry.getKey().toUpperCase().equals("END_DATE_TIME"))
			    		iso.End_Date_Time = entry.getValue().toString().replaceAll("\"", "");
			    	if(entry.getKey().toUpperCase().equals("OFFER_SEQUENCE"))
			    	{
			    		iso.Offer_Sequence = entry.getValue().toString().replaceAll("\"", "");
			    		if(iso.Offer_Sequence.equals("99"))
			    			isAddonProductPrivate = false;
			    		else
			    			isAddonProductPrivate = true;
			    			
			    	}
			    	if(entry.getKey().toUpperCase().equals("USAGE_VALUE"))
			    		iso.Usage_Value = entry.getValue().toString().replaceAll("\"", "");
			    	if(entry.getKey().toUpperCase().equals("START_DATE_TIME"))
			    		iso.Start_Date_Time = entry.getValue().toString().replaceAll("\"", "");	
			    	if(entry.getKey().toUpperCase().equals("QUALITY"))
			    		iso.Quality = entry.getValue().toString().replaceAll("\"", "");
			    	
			    	Offer_ID_key = iso.Offer_ID + ";" + iso.Offer_Sequence;
				}
				OfferMap.put(Offer_ID_key  , iso);				
			}
			if(inputFileType.equals("IMPLICIT_SHARED_OFFER"))
			{				
				IMPLICIT_SHARED_OFFER iso = new IMPLICIT_SHARED_OFFER();
				String Offer_ID_key = "";
				for (Map.Entry<String, JsonElement> entry: entries) {
				    if(entry.getKey().toUpperCase().equals("SHARED_OFFER_ID"))
			    		iso.Shared_Offer_ID = Offer_ID_key = entry.getValue().toString().replaceAll("\"", "");
			    	if(entry.getKey().toUpperCase().equals("USAGE_THRESHOLD"))
			    		iso.Usage_Threshold = entry.getValue().toString().replaceAll("\"", "");
			    	if(entry.getKey().toUpperCase().equals("START_DATE"))
			    		iso.Start_Date = entry.getValue().toString().replaceAll("\"", "");
			    	if(entry.getKey().toUpperCase().equals("END_DATE"))
			    		iso.End_Date = entry.getValue().toString().replaceAll("\"", "");
			    	if(entry.getKey().toUpperCase().equals("USAGE_VALUE"))
			    		iso.Usage_Value = entry.getValue().toString().replaceAll("\"", "");
				}
				OfferMap.put(Offer_ID_key, iso);				
			}
			if(inputFileType.equals("SHARED_OFFER_RELATION"))
			{				
				SHARED_OFFER_RELATION iso = new SHARED_OFFER_RELATION();
				String Offer_ID_key = "";
				for (Map.Entry<String, JsonElement> entry: entries) {
				    if(entry.getKey().toUpperCase().equals("PROVIDER_OFFER_ID"))
			    		iso.Provider_Offer_ID = Offer_ID_key = entry.getValue().toString().replaceAll("\"", "");
			    	if(entry.getKey().toUpperCase().equals("PROVIDER_MSISDN"))
			    		iso.Provider_Msisdn = entry.getValue().toString().replaceAll("\"", "");
			    	
				}
				OfferMap.put(Offer_ID_key, iso);				
			}
			if(inputFileType.equals("MAIN_OFFER"))
			{
				
				MAIN_OFFER iso = new MAIN_OFFER();
				String Offer_ID_key = "";
				for (Map.Entry<String, JsonElement> entry: entries) {
				    if(entry.getKey().toUpperCase().equals("OFFER_ID"))
			    		iso.Offer_ID = Offer_ID_key = entry.getValue().toString().replaceAll("\"", "");
			    	if(entry.getKey().toUpperCase().equals("END_DATE_TIME"))
			    		iso.End_Date_Time = entry.getValue().toString().replaceAll("\"", "");
			    	if(entry.getKey().toUpperCase().equals("START_DATE_TIME"))
			    		iso.Start_Date_Time = this.OfferStartDateTime =  entry.getValue().toString().replaceAll("\"", "");
			    	if(entry.getKey().toUpperCase().equals("USAGE_THRESHOLD"))
			    		iso.Usage_Threshold = entry.getValue().toString().replaceAll("\"", "");
			    	if(entry.getKey().toUpperCase().equals("ACCOUNT_MSISDN"))
			    		this.MSISDN = entry.getValue().toString().replaceAll("\"", "");
			    	if(entry.getKey().toUpperCase().equals("BILL_CYCLE"))
			    		iso.Bill_Cycle = entry.getValue().toString().replaceAll("\"", "");
			    	if(entry.getKey().toUpperCase().equals("USAGE_VALUE"))
			    		iso.Usage_Value = entry.getValue().toString().replaceAll("\"", "");
				}
				OfferMap.put(Offer_ID_key, iso);				
			}
		}
		return OfferMap;
	}
	
	public class ADDON_OFFER{
		public String Offer_ID;
		public String Start_Date_Time;
		public String End_Date_Time;
		public String Usage_Value;
		public String Usage_Threshold;
		public String Offer_Sequence;
		public String Quality;
		
		public ADDON_OFFER() {
			// TODO Auto-generated constructor stub
			this.Offer_ID = "";
			this.Start_Date_Time = "";
			this.End_Date_Time = "";
			this.Usage_Value = "";
			this.Usage_Threshold = "";
			this.Offer_Sequence = "";
			this.Quality = "";
		}
	}
	
	public class SHARED_OFFER_RELATION{
		public String Provider_Offer_ID;
		public String Provider_Msisdn;
		
		public SHARED_OFFER_RELATION() {
			this.Provider_Offer_ID = "";
			this.Provider_Msisdn = "";
			// TODO Auto-generated constructor stub
		}
		
		/*public SHARED_OFFER_RELATION(String Provider_Offer_ID, String Account_Msisdn, String Provider_Msisdn) {
			// TODO Auto-generated constructor stub
			this.Provider_Offer_ID = Provider_Offer_ID;
			this.Account_Msisdn = Account_Msisdn;
			this.Provider_Msisdn = Provider_Msisdn;
		}*/
	}
	
	public class IMPLICIT_SHARED_OFFER{
		public String Shared_Offer_ID;
		public String Usage_Threshold;
		public String Account_Msisdn;
		public String Start_Date;
		public String End_Date;
		public String Usage_Value;
		
		public IMPLICIT_SHARED_OFFER() {
			// TODO Auto-generated constructor stub
			this.Shared_Offer_ID = "";
			this.Usage_Threshold = "";
			this.Account_Msisdn = "";
			this.Start_Date = "";
			this.End_Date = "";
			this.Usage_Value = "";
		}
	}
	
	public class CREDIT_LIMIT{
		public String Account_Msisdn;
		public String service_class;
		public String reference_credit_limit;
		public String subscriber_balance;
		public String available_credit_limit;
		public String minpay_factor;
		public String notification_7;
		public String notification_8;
		public String notification_9;
		public String usage_value;
		
		public CREDIT_LIMIT(){
			this.Account_Msisdn = "";
			this.service_class = "";
			this.reference_credit_limit = "";
			this.subscriber_balance = "";
			this.available_credit_limit = "";
			this.minpay_factor = "";
			this.notification_7 = "";
			this.notification_8 = "";
			this.notification_9 = "";
			this.usage_value = "";
		}
	}
	
	public class MAIN_OFFER{
		public String Offer_ID;
		public String End_Date_Time;
		public String Start_Date_Time;
		public String Usage_Threshold;
		public String Bill_Cycle;
		public String Usage_Value;
		
		public MAIN_OFFER() {
			// TODO Auto-generated constructor stub
			this.Offer_ID = "";
			this.End_Date_Time = "";
			this.Start_Date_Time = "";
			this.Usage_Threshold = "";
			this.Bill_Cycle = "";
			this.Usage_Value = "";
		}
	}

	public boolean isAddonProductPrivate() {
		// TODO Auto-generated method stub
		
		//ADDON_OFFER mo = inputObj.new ADDON_OFFER();
		//mo = (ADDON_OFFER)inputObj.ADDON_OFFER.get(entryKey);
		//String OfferID = entryKey.split(";")[0];
		return isAddonProductPrivate;
	}
	
}


