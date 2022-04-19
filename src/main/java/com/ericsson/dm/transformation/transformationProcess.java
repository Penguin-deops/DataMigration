package com.ericsson.dm.transformation;

import java.io.BufferedReader;
import java.io.BufferedWriter;

//com.ericsson.datamigration.js.transformation.

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import com.ericsson.dm.Utils.CommonUtilities;
import com.ericsson.dm.inititialization.LoadSubscriberMapping;
import com.ericsson.dm.transform.implementation.Account;
import com.ericsson.dm.transform.implementation.Credit_Limit_UC;
import com.ericsson.dm.transform.implementation.DedicatedAccount;
import com.ericsson.dm.transform.implementation.DedicatedAccountBatchFile;
import com.ericsson.dm.transform.implementation.Offer;
import com.ericsson.dm.transform.implementation.PamAccount;
import com.ericsson.dm.transform.implementation.ProviderOffer;
import com.ericsson.dm.transform.implementation.ServiceClass;
import com.ericsson.dm.transform.implementation.Subscriber;
import com.ericsson.dm.transform.implementation.SubscriberOffer;
import com.ericsson.dm.transform.implementation.UsageCounter;
import com.ericsson.dm.transform.implementation.UsageThreshold;
import com.ericsson.dm.transformation.JsonInputFields.ADDON_OFFER;
import com.ericsson.dm.transformation.JsonInputFields.IMPLICIT_SHARED_OFFER;
import com.ericsson.dm.transformation.JsonInputFields.SHARED_OFFER_RELATION;
import com.ericsson.dm.transformation.JsonInputFields.MAIN_OFFER;

public class transformationProcess {

	private String pathOfWorkingFolder;
	private String pathOfInputFolder;
	private String pathOfMigtool;
	private String pathOfLogFolder;
	private String JsonFileName;
	final static Logger LOG = Logger.getLogger(transformationProcess.class);
	private String pathtoApplicationContext;
	private int uniqueNumber;
	public HashMap<String, Long> RejectedException;
	
	boolean isProductPrivate;

	//All the input fields
	
	private List<String> AFBuffer;
	private List<String> accountBuffer;
	private List<String> subscriberBuffer;
	private List<String> subscriberDATBuffer;
	private Set<String> offerBuffer;
	private List<String> pamBuffer;
	private List<String> ucBuffer;
	private List<String> ucCreditLimitBuffer;
	private List<String> ucCreditLimitTempBuffer;
	private List<String> utBuffer;
	private List<String> daBuffer;
	private List<String> tempdaBatchBuffer;
	private List<String> scBuffer;
	private List<String> subscriberOfferBuffer;
	private List<String> providerOfferBuffer;
	private List<String> daBatchBuffer;
	private Set<String> rejectAndLog;
	private Set<String> onlyLog;
	private List<String> DuplicateUTLog;
	private Map<String, String> ProductIDLookUpMap;
	private List<String> MigrationPhase;
	private Boolean ExecuteOnce;
	
	//ListofMSISDNCompleted
	private Set<String> MSISDNCOMPLETED;
	
	private static final Object ThreadLock = new Object();
	
	public transformationProcess()
	{
		
	}
	
	public transformationProcess(String pathOfMigtool, String JsonFileName, final String pathtoApplicationContext, String dummy1, String dummy2) {
		this.pathOfWorkingFolder = pathOfMigtool + "/Working//";
		this.pathOfInputFolder = pathOfMigtool + "/Input//";
		this.pathOfLogFolder = "/" + pathOfLogFolder;
		this.pathOfMigtool = pathOfMigtool;
		this.JsonFileName = JsonFileName;
		this.RejectedException = new HashMap<String, Long>();
		this.ExecuteOnce = true;
		this.isProductPrivate = false;
		uniqueNumber = LoadSubscriberMapping.rand.nextInt(1000000);
		this.pathtoApplicationContext = pathtoApplicationContext;
				
		accountBuffer = new ArrayList<>();
		subscriberBuffer = new ArrayList<>();
		offerBuffer = new HashSet<String>();
		pamBuffer = new ArrayList<>();
		ucBuffer = new ArrayList<>();
		utBuffer = new ArrayList<>();
		daBuffer = new ArrayList<>();
		tempdaBatchBuffer = new ArrayList<String>();
		scBuffer = new ArrayList<>();
		subscriberDATBuffer = new ArrayList<>();
		subscriberOfferBuffer = new ArrayList<>();
		providerOfferBuffer = new ArrayList<>();
		MigrationPhase = new ArrayList<String>();
		accountBuffer = new ArrayList<>();
		daBatchBuffer = new ArrayList<>();
		ucCreditLimitBuffer = new ArrayList<>();
		ucCreditLimitTempBuffer = new ArrayList<>();
		//SortedBalanceInput = new CopyOnWriteArrayList<>(); //ArrayList<>();
		
		this.DuplicateUTLog = new ArrayList();
		rejectAndLog = Collections.synchronizedSet(new HashSet<String>());
		onlyLog = Collections.synchronizedSet(new HashSet<String>());
		ProductIDLookUpMap = Collections.synchronizedMap(new ConcurrentHashMap<>(10000, 0.75f, 100));
		AFBuffer = new ArrayList<>();
		MSISDNCOMPLETED = new HashSet<String>();
		//Initialize mapping sheet
		LoadSubscriberMapping lsm = new LoadSubscriberMapping(JsonFileName,pathOfMigtool, pathOfMigtool + "/config/", pathOfMigtool + "/Input/");
		lsm.intializeMapping();
	
	}

	private boolean IsSubuscriberDiscarded(String MSISDN, JsonInputFields inputObj)
	{
		boolean IsLifecycleNotDiscarded = true;
		
		//criteria for rejection 
		if(!LoadSubscriberMapping.AlreadyMigratedMsisdn.contains(MSISDN))
		{
			if(!MSISDN.substring(0, 3).equals("100") &&  (inputObj.ACCOUNT.size() == 0) )
			{
				//rejectAndLog.add("INC01:MSISDN=" + MSISDN + ";MSISDN NOT PRESENT IN CS:ACTION=DISCARD & LOG");
				if(!MSISDN.equals("ACCOUNT_MSISDN"))
				{ 
					/*Set<String> exceptionOffer = PopulateOfferMapForRejection(inputObj);
					String concatOffer = "";
					for(String s : exceptionOffer)
					{
						concatOffer = concatOffer.concat( s +  ","  );
					}
					rejectAndLog.add("INC01:MSISDN=" + MSISDN + ":OFFER_UV_UT=" +concatOffer + ":DESCRIPTION=MSISDN NOT PRESENT IN CS:ACTION=DISCARD & LOG");*/
					
					IsLifecycleNotDiscarded = false;
				}
				else
					IsLifecycleNotDiscarded = false;
				
				this.MSISDNCOMPLETED.add(inputObj.MSISDN);
			}
			else //if(MSISDN.substring(0, 3).equals("100"))
			{
				if(!LoadSubscriberMapping.CompleteListToMigrate.contains(MSISDN) )
				{
					Set<String> exceptionOffer = PopulateOfferMapForRejection(inputObj);
					String concatOffer = "";
					for(String s : exceptionOffer)
					{
						concatOffer = concatOffer.concat( s +  ","  );
					}
					rejectAndLog.add("INC01:MSISDN=" + MSISDN + ":OFFER_UV_UT=" +concatOffer + ":DESCRIPTION=MSISDN NOT PRESENT IN CS:ACTION=DISCARD & LOG");//IsLifecycleNotDiscarded = false;
					
					this.MSISDNCOMPLETED.add(inputObj.MSISDN);
					IsLifecycleNotDiscarded = false;
				}						
			}
		}
		else
		{
			Set<String> exceptionOffer = PopulateOfferMapForRejection(inputObj);
			String concatOffer = "";
			for(String s : exceptionOffer)
			{
				concatOffer = concatOffer.concat( s +  ","  );
			}
			rejectAndLog.add("INC01:MSISDN=" + MSISDN + ":OFFER_UV_UT=" +concatOffer + ":DESCRIPTION=MSISDN ALREADY MIGRATED:ACTION=DISCARD & LOG");//IsLifecycleNotDiscarded = false;
			
			this.MSISDNCOMPLETED.add(inputObj.MSISDN);
			//LoadSubscriberMapping.CompleteListToMigrate.add(inputObj.MSISDN);
			IsLifecycleNotDiscarded = false;
		}
		return IsLifecycleNotDiscarded;
	}
	
	
	private Set<String> PopulateOfferMapForRejection(JsonInputFields inputObj) {
		// TODO Auto-generated method stub
		//Use mainOffer
		Set<String> OfferValue = new HashSet<String>();
		for(Map.Entry<String,Object> entry : inputObj.MAIN_OFFER.entrySet())
		{
			MAIN_OFFER mo = inputObj.new MAIN_OFFER();
			mo = (MAIN_OFFER)entry.getValue();
			String OfferID = mo.Offer_ID;
			//RejectedException
			if(RejectedException.containsKey(OfferID))
    		{
    			Long counter = RejectedException.get(OfferID) + 1L;
    			RejectedException.put(OfferID, counter);
    			OfferValue.add(OfferID + "-" + mo.Usage_Value + "-" + mo.Usage_Threshold );
    		}
    		else
    		{
    			RejectedException.put(OfferID, 1L);
    			OfferValue.add(OfferID + "-" + mo.Usage_Value + "-" + mo.Usage_Threshold);
    		}
		}
		for(Map.Entry<String,Object> entry : inputObj.IMPLICIT_SHARED_OFFER.entrySet())
		{
			IMPLICIT_SHARED_OFFER mo = inputObj.new IMPLICIT_SHARED_OFFER();
			mo = (IMPLICIT_SHARED_OFFER)entry.getValue();
			String OfferID = mo.Shared_Offer_ID;
			//RejectedException
			if(RejectedException.containsKey(OfferID))
    		{
    			Long counter = RejectedException.get(OfferID) + 1L;
    			RejectedException.put(OfferID, counter);
    			OfferValue.add(OfferID + "-" + mo.Usage_Value + "-" + mo.Usage_Threshold);
    		}
    		else
    		{
    			RejectedException.put(OfferID, 1L);
    			OfferValue.add(OfferID + "-" + mo.Usage_Value + "-" + mo.Usage_Threshold);
    		}
		}
		for(Map.Entry<String,Object> entry : inputObj.SHARED_OFFER_RELATION.entrySet())
		{
			SHARED_OFFER_RELATION mo = inputObj.new SHARED_OFFER_RELATION();
			mo = (SHARED_OFFER_RELATION)entry.getValue();
			String OfferID = mo.Provider_Offer_ID;
			//RejectedException
			if(RejectedException.containsKey(OfferID))
    		{
    			Long counter = RejectedException.get(OfferID) + 1L;
    			RejectedException.put(OfferID, counter);
    			OfferValue.add(OfferID + "--" );
    		}
    		else
    		{
    			RejectedException.put(OfferID, 1L);
    			OfferValue.add(OfferID + "--" );
    		}
		}
		TreeSet<String> sortedAddonKey = new TreeSet<String>(inputObj.ADDON_OFFER.keySet());
		
		//for(Map.Entry<String,Object> entry : inputObj.ADDON_OFFER.entrySet())
		for(String entryKey  : sortedAddonKey)
		{
			ADDON_OFFER mo = inputObj.new ADDON_OFFER();
			mo = (ADDON_OFFER)inputObj.ADDON_OFFER.get(entryKey);
			String OfferID = entryKey.split(";")[0];
			if(RejectedException.containsKey(OfferID))
    		{
    			Long counter = RejectedException.get(OfferID) + 1L;
    			RejectedException.put(OfferID, counter);
    			OfferValue.add(OfferID + "-" + mo.Usage_Value + "-" + mo.Usage_Threshold);
    		}
    		else
    		{
    			RejectedException.put(OfferID, 1L);
    			OfferValue.add(OfferID + "-" + mo.Usage_Value + "-" + mo.Usage_Threshold);
    		}
		}
		return OfferValue;
	}
	
	public synchronized void process(String json) throws IOException {
		long start = Calendar.getInstance().getTimeInMillis();
		//performanceLog.add("starting File at: " + LocalTime.now());
		{		
			try 
			{
				//loading the input file. 
				JsonInputFields inputObj = new JsonInputFields(json); 
				inputObj.intializeInputJson();
				isProductPrivate = inputObj.isAddonProductPrivate();
				
				
				if(IsSubuscriberDiscarded(inputObj.MSISDN, inputObj))
				{
					this.MSISDNCOMPLETED.add(inputObj.MSISDN);
					this.MigrationPhase = Arrays.asList(LoadSubscriberMapping.CommonConfigMapping.get("MIGRATION_PHASE").toString().split(","));
					
					if(MigrationPhase.contains("phase2") && (MigrationPhase.contains("phase1")))
					{
						TransformPhase1(inputObj);
						TransformPhase2(inputObj);
					}
					else if(MigrationPhase.size() == 1 &&  MigrationPhase.contains("phase1"))
					{
						TransformPhase1(inputObj);
					}
					else if(MigrationPhase.size() == 1 &&  MigrationPhase.contains("phase2"))
					{
						TransformPhase2(inputObj);
					}	
					
					//Create DA/UT/SubuscriberOffer/ProviderOffer/Offer.CSV from AddonOffer
					//TransformAdditionalRecord();
				}
	
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}		
	}	
	
	public void TransformAdditionalRecord() {
		// TODO Auto-generated method stub
		
		//load the input file Implicit_Shared_Offer.csv,Addon_Offer.csv,Shared_Offer_Relation.csv
		LoadSubscriberMapping.loadImplicitSharedOfferFile(this.pathOfInputFolder);
		LoadSubscriberMapping.loadExtraAddonOfferFile(this.pathOfInputFolder);
		LoadSubscriberMapping.loadSharedOfferRelationFile(this.pathOfInputFolder);
		
		Offer offer = new Offer(rejectAndLog, onlyLog, MSISDNCOMPLETED, ProductIDLookUpMap);
		offerBuffer.addAll(offer.executeAdditionalRecord());
		
		UsageThreshold ut = new UsageThreshold(rejectAndLog, onlyLog, MSISDNCOMPLETED, ProductIDLookUpMap,DuplicateUTLog);
		utBuffer.addAll(ut.executeAdditionalRecord());
		
		SubscriberOffer so = new SubscriberOffer(rejectAndLog, onlyLog, MSISDNCOMPLETED, ProductIDLookUpMap);
		subscriberOfferBuffer.addAll(so.executeAdditionalRecord());
		
		ProviderOffer po = new ProviderOffer(rejectAndLog, onlyLog, MSISDNCOMPLETED, ProductIDLookUpMap);
		providerOfferBuffer.addAll(po.executeAdditionalRecord());
		
		DedicatedAccountBatchFile dab = new DedicatedAccountBatchFile(rejectAndLog, onlyLog, MSISDNCOMPLETED);
		tempdaBatchBuffer.addAll(dab.executeAdditionalRecord());
		
		UsageCounter uc = new UsageCounter(rejectAndLog, onlyLog, MSISDNCOMPLETED, ProductIDLookUpMap);
		ucBuffer.addAll(uc.executeAdditionalRecord());
	}

	private void TransformPhase2(JsonInputFields inputObj) {
		// TODO Auto-generated method stub
		ProductIDLookUpMap.clear();
		if(scBuffer.size() == 0)
		{
			ServiceClass sc = new ServiceClass(rejectAndLog, onlyLog, inputObj, ProductIDLookUpMap);
			scBuffer.addAll(sc.executePhase2());
			
			//UsageCounter uc = new UsageCounter(rejectAndLog, onlyLog, inputObj, ProductIDLookUpMap);
			//ucBuffer.addAll(uc.executePhase2());
			
			Offer offer = new Offer(rejectAndLog, onlyLog, inputObj, ProductIDLookUpMap, isProductPrivate);
			offerBuffer.addAll(offer.executePhase2());	
			
			UsageThreshold ut = new UsageThreshold(rejectAndLog, onlyLog, inputObj, ProductIDLookUpMap, isProductPrivate, DuplicateUTLog);
			utBuffer.addAll(ut.executePhase2());
			
			Credit_Limit_UC cluc = new Credit_Limit_UC(rejectAndLog, onlyLog);
			ucCreditLimitTempBuffer.addAll(cluc.executePhase2());
		}
	}

	public void TransformPhase1(JsonInputFields inputObj)
	{
		ProductIDLookUpMap.clear();
		Subscriber subscriber = new Subscriber(rejectAndLog, onlyLog, inputObj);
		Account account = new Account(rejectAndLog, onlyLog, inputObj);
		DedicatedAccount da = new DedicatedAccount(rejectAndLog, onlyLog, inputObj);
		PamAccount pam = new PamAccount(rejectAndLog, onlyLog, inputObj);
		Offer offer = new Offer(rejectAndLog, onlyLog, inputObj, ProductIDLookUpMap, isProductPrivate);
		
		if(inputObj.MSISDN.substring(0, 3).equals("100") &&  !inputObj.AccountSfeeFlag )
		{
			//System.out.println("MSISDN: " + inputObj.MSISDN);
		    //Populate Subscriber output				
			subscriberBuffer.addAll(subscriber.execute());
			//add code for SUBSCRIBER_00001 file.
			subscriberDATBuffer.add("1," + inputObj.MSISDN);
			
			//Populate Account output
			accountBuffer.addAll(account.execute());
			
			//Populate DedicatedAccount output
			Map<String, String> daMap = CommonUtilities.SortDABasedOnValue(da.execute());				
			daBuffer.addAll(CommonUtilities.GenerateDABasedOnSDP(inputObj.MSISDN, daMap));
			
			//Populate Pam output
			pamBuffer.addAll(pam.execute());
			
			AFBuffer.add("973" + inputObj.MSISDN);						
			
			//Populate Offer output
			offerBuffer.addAll(offer.execute("Master"));						
		}
		//Populate Offer output
		offerBuffer.addAll(offer.execute());					 
		
		//Populate UsageCounter output
		UsageCounter uc = new UsageCounter(rejectAndLog, onlyLog, inputObj, ProductIDLookUpMap,isProductPrivate);
		ucBuffer.addAll(uc.execute());
		
		UsageThreshold ut = new UsageThreshold(rejectAndLog, onlyLog, inputObj, ProductIDLookUpMap, isProductPrivate,DuplicateUTLog);
		utBuffer.addAll(ut.execute());
		
		SubscriberOffer so = new SubscriberOffer(rejectAndLog, onlyLog, inputObj, ProductIDLookUpMap);
		subscriberOfferBuffer.addAll(so.execute());
		
		ProviderOffer po = new ProviderOffer(rejectAndLog, onlyLog, inputObj, ProductIDLookUpMap);
		providerOfferBuffer.addAll(po.execute());
		
		DedicatedAccountBatchFile dab = new DedicatedAccountBatchFile(rejectAndLog, onlyLog, inputObj);
		tempdaBatchBuffer.addAll(dab.execute());
		
		account = null;
		subscriber = null;
		offer = null;
		da = null;
		uc = null;
		ut = null;
		so = null;
		po = null;
		dab = null;
	}

	public static void main(String args[]) throws IOException, Exception {
		System.out.println("start the Execution!!!");
		
		String json = "{\"luw_id\":\"5344136\",\"BODY\":{\"MAIN_OFFER\":[{\"Account_Msisdn\":\"13610515\",\"Offer_ID\":\"3000002\",\"Start_Date_Time\":\"2011-02-13 00:00:00\",\"End_Date_Time\":\"\",\"Usage_Value\":\"240\",\"Usage_Threshold\":\"1800\",\"Bill_Cycle\":\"11\"}],\"ADDON_OFFER\":[{\"Account_Msisdn\":\"13610515\",\"Offer_ID\":\"30294\",\"Start_Date_Time\":\"2020-10-01 00:00:00\",\"End_Date_Time\":\"\",\"Usage_Value\":\"0\",\"Usage_Threshold\":\"0\",\"Offer_Sequence\":\"99\",\"Quality\":\"0\"}],\"CREDIT_LIMIT\":[{\"Account_Msisdn\":\"13610515\",\"Service_Class\":\"3300\",\"REFERENCE_CREDIT_LIMIT\":\"50\",\"SUBSCRIBER_BALANCE\":\"0.000\",\"AVAILABLE_CREDIT_LIMIT\":\"50.000\",\"MINPAY_FACTOR\":\"0.0\",\"Notification_70\":\"\",\"Notification_80\":\"0.000\",\"Notification_90\":\"\",\"CDR_Usage\":\"0.000\"}]}}";
		
		//String json = "{\"luw_id\":\"3945023\",\"BODY\":{\"MAIN_OFFER\":[{\"Account_Msisdn\":\"36566288\",\"Offer_ID\":\"3000002\",\"Start_Date_Time\":\"2016-07-20 00:00:00\",\"End_Date_Time\":\"\",\"Usage_Value\":\"8100\",\"Usage_Threshold\":\"36000\",\"Bill_Cycle\":\"11\"}],\"ACCOUNT\":[{\"Account_MSISDN\":\"36566288\",\"account_class\":\"61\",\"orig_account_class\":\"61\",\"account_class_expiry\":\"NULL\",\"units\":\"0\",\"activated\":\"16800\",\"sfee_expiry_date\":\"18553\",\"sup_expiry_date\":\"18553\",\"sfee_done_date\":\"0\",\"previous_sfee_done_date\":\"0\",\"sfee_status\":\"92\",\"sup_status\":\"92\",\"neg_balance_start\":\"0\",\"neg_balance_barred\":\"0\",\"account_disconnect\":\"0\",\"account_status\":\"1\",\"prom_notification\":\"0\",\"service_offerings\":\"0\",\"account_group_id\":\"0\",\"community_id1\":\"1556\",\"community_id2\":\"0\",\"community_id3\":\"0\",\"account_home_region\":\"0\",\"product_id_counter\":\"NULL\",\"account_lock\":\"NULL\",\"account_prepaid_empty_limit\":\"NULL\"}],\"ADDON_OFFER\":[{\"Account_Msisdn\":\"36566288\",\"Offer_ID\":\"30057\",\"Start_Date_Time\":\"2020-09-01 00:00:00\",\"End_Date_Time\":\"2020-10-30 23:59:59\",\"Usage_Value\":\"3000\",\"Usage_Threshold\":\"3000\",\"Offer_Sequence\":\"99\",\"Quality\":\"0\"},{\"Account_Msisdn\":\"36566288\",\"Offer_ID\":\"30057\",\"Start_Date_Time\":\"2020-10-01 00:00:00\",\"End_Date_Time\":\"2020-12-30 23:59:59\",\"Usage_Value\":\"3000\",\"Usage_Threshold\":\"3000\",\"Offer_Sequence\":\"99\",\"Quality\":\"0\"}],\"CREDIT_LIMIT\":[{\"Account_Msisdn\":\"36566288\",\"Service_Class\":\"3200\",\"REFERENCE_CREDIT_LIMIT\":\"10\",\"SUBSCRIBER_BALANCE\":\"0.000\",\"AVAILABLE_CREDIT_LIMIT\":\"10.000\",\"MINPAY_FACTOR\":\"0.7\",\"Notification_70\":\"\",\"Notification_80\":\"8.000\",\"Notification_90\":\"\",\"CDR_Usage\":\"0.000\"}]}}";
		
		//String json = "{\"luw_id\":\"7602045\",\"BODY\":{\"MAIN_OFFER\":[{\"Account_Msisdn\":\"36888892\",\"Offer_ID\":\"3000002\",\"Start_Date_Time\":\"2017-01-18 00:00:00\",\"End_Date_Time\":\"\",\"Usage_Value\":\"27000\",\"Usage_Threshold\":\"27000\",\"Bill_Cycle\":\"11\"}],\"ACCOUNT\":[{\"Account_MSISDN\":\"36888892\",\"account_class\":\"3001\",\"orig_account_class\":\"3001\",\"account_class_expiry\":\"NULL\",\"units\":\"0\",\"activated\":\"17206\",\"sfee_expiry_date\":\"24836\",\"sup_expiry_date\":\"24836\",\"sfee_done_date\":\"0\",\"previous_sfee_done_date\":\"0\",\"sfee_status\":\"27\",\"sup_status\":\"132\",\"neg_balance_start\":\"0\",\"neg_balance_barred\":\"0\",\"account_disconnect\":\"0\",\"account_status\":\"1\",\"prom_notification\":\"0\",\"service_offerings\":\"0\",\"account_group_id\":\"0\",\"community_id1\":\"0\",\"community_id2\":\"0\",\"community_id3\":\"0\",\"account_home_region\":\"0\",\"product_id_counter\":\"4\",\"account_lock\":\"NULL\",\"account_prepaid_empty_limit\":\"NULL\"}],\"ADDON_OFFER\":[{\"Account_Msisdn\":\"36888892\",\"Offer_ID\":\"3502702\",\"Start_Date_Time\":\"2020-09-01 00:00:00\",\"End_Date_Time\":\"2020-09-30 23:59:59\",\"Usage_Value\":\"12000\",\"Usage_Threshold\":\"12000\",\"Offer_Sequence\":\"1\",\"Quality\":\"0\"},{\"Account_Msisdn\":\"36888892\",\"Offer_ID\":\"3502702\",\"Start_Date_Time\":\"2020-08-30 00:00:00\",\"End_Date_Time\":\"2020-10-01 23:59:59\",\"Usage_Value\":\"12000\",\"Usage_Threshold\":\"12000\",\"Offer_Sequence\":\"2\",\"Quality\":\"0\"},{\"Account_Msisdn\":\"36888892\",\"Offer_ID\":\"3502702\",\"Start_Date_Time\":\"2020-08-02 00:00:00\",\"End_Date_Time\":\"2020-09-30 23:50:50\",\"Usage_Value\":\"12000\",\"Usage_Threshold\":\"12000\",\"Offer_Sequence\":\"3\",\"Quality\":\"0\"},{\"Account_Msisdn\":\"36888892\",\"Offer_ID\":\"30112\",\"Start_Date_Time\":\"2020-08-02 00:00:00\",\"End_Date_Time\":\"2020-09-30 23:50:50\",\"Usage_Value\":\"12000\",\"Usage_Threshold\":\"12000\",\"Offer_Sequence\":\"1\",\"Quality\":\"0\"},{\"Account_Msisdn\":\"36888892\",\"Offer_ID\":\"30129\",\"Start_Date_Time\":\"2020-09-01 00:00:00\",\"End_Date_Time\":\"\",\"Usage_Value\":\"0\",\"Usage_Threshold\":\"0\",\"Offer_Sequence\":\"99\",\"Quality\":\"0\"},{\"Account_Msisdn\":\"36888892\",\"Offer_ID\":\"3501302\",\"Start_Date_Time\":\"2020-09-01 00:00:00\",\"End_Date_Time\":\"2020-09-30 23:59:59\",\"Usage_Value\":\"12000\",\"Usage_Threshold\":\"12000\",\"Offer_Sequence\":\"99\",\"Quality\":\"0\"}],\"CREDIT_LIMIT\":[{\"Account_Msisdn\":\"36888892\",\"Service_Class\":\"3100\",\"REFERENCE_CREDIT_LIMIT\":\"60\",\"SUBSCRIBER_BALANCE\":\"2.646\",\"AVAILABLE_CREDIT_LIMIT\":\"57.354\",\"MINPAY_FACTOR\":\"0.7\",\"Notification_70\":\"\",\"Notification_80\":\"45.883\",\"Notification_90\":\"\",\"CDR_Usage\":\"2.646\"}]}}";
		
		//String json = "{\"luw_id\":\"8750053\",\"BODY\":{\"MAIN_OFFER\":[{\"Account_Msisdn\":\"32182128\",\"Offer_ID\":\"3000002\",\"Start_Date_Time\":\"2014-09-04 00:00:00\",\"End_Date_Time\":\"\",\"Usage_Value\":\"1380\",\"Usage_Threshold\":\"21000\",\"Bill_Cycle\":\"10\"}],\"ACCOUNT\":[{\"Account_MSISDN\":\"32182128\",\"account_class\":\"3001\",\"orig_account_class\":\"3001\",\"account_class_expiry\":\"NULL\",\"units\":\"0\",\"activated\":\"17206\",\"sfee_expiry_date\":\"24836\",\"sup_expiry_date\":\"24836\",\"sfee_done_date\":\"0\",\"previous_sfee_done_date\":\"0\",\"sfee_status\":\"27\",\"sup_status\":\"132\",\"neg_balance_start\":\"0\",\"neg_balance_barred\":\"0\",\"account_disconnect\":\"0\",\"account_status\":\"1\",\"prom_notification\":\"0\",\"service_offerings\":\"0\",\"account_group_id\":\"0\",\"community_id1\":\"0\",\"community_id2\":\"0\",\"community_id3\":\"0\",\"account_home_region\":\"0\",\"product_id_counter\":\"NULL\",\"account_lock\":\"NULL\",\"account_prepaid_empty_limit\":\"NULL\"}],\"ADDON_OFFER\":[{\"Account_Msisdn\":\"32182128\",\"Offer_ID\":\"3000502\",\"Start_Date_Time\":\"2020-09-10 00:00:00\",\"End_Date_Time\":\"2020-10-09 23:59:59\",\"Usage_Value\":\"0\",\"Usage_Threshold\":\"6000\",\"Offer_Sequence\":\"99\",\"Quality\":\"1\"}],\"CREDIT_LIMIT\":[{\"Account_Msisdn\":\"32182128\",\"Service_Class\":\"3100\",\"REFERENCE_CREDIT_LIMIT\":\"40\",\"SUBSCRIBER_BALANCE\":\"4.446\",\"AVAILABLE_CREDIT_LIMIT\":\"35.554\",\"MINPAY_FACTOR\":\"0.7\",\"Notification_70\":\"\",\"Notification_80\":\"28.443\",\"Notification_90\":\"\",\"CDR_Usage\":\"0.000\"}]}}";
		
		String sdpid, configPath, dataFolderPath, workingPath, output, pathtoApplicationContext, pathOfLogFolder, MasterFolder = null;
		
		dataFolderPath = "C:\\Ericsson\\MyWorkingProject\\Charging_System\\dm_cs_2020_zain_bahrain\\dev\\src\\input";
		configPath = "C:\\Ericsson\\MyWorkingProject\\Charging_System\\dm_cs_2020_zain_bahrain\\dev\\src\\config\\config";
		workingPath = "C:\\Ericsson\\MyWorkingProject\\Charging_System\\dm_cs_2020_zain_bahrain\\dev\\src\\working\\";
		output = "C:\\Ericsson\\MyWorkingProject\\Charging_System\\dm_cs_2020_zain_bahrain\\dev\\src\\output\\";
		pathtoApplicationContext = "C:\\Ericsson\\MyWorkingProject\\dm_cs_2020_zain_bahrain\\dm_cs_2018_du_dubai\\dev\\src\\";
		pathOfLogFolder = "C:\\Ericsson\\MyWorkingProject\\Charging_System\\dm_cs_2020_zain_bahrain\\dev\\src\\logs\\";
		MasterFolder = "C:\\Ericsson\\MyWorkingProject\\Charging_System\\dm_cs_2020_zain_bahrain\\dev\\src\\";
		//String filesToBeInserted = "INFILE_Subscriber_Balances.csv.gz,INFILE_Subscriber_cug_cli.csv.gz,INFILE_Subscriber_USMS.csv.gz";
		//String filesToBeInserted = "sdp01_subscriber_balances_dump.csv.gz,sdp01_subscriber_cugcli_dump.csv.gz,sdp01_subscriber_usms_dump.csv.gz";
		
		
		transformationProcess et = new transformationProcess(MasterFolder, "Zain_Bahrain_SDP.json", pathtoApplicationContext, "asbc", "lmn");
		et.process(json);
		//et.TransformAdditionalRecord();
		et.generateCsv();
		et.clearBuffer();
		
		System.out.println("Completed the Execution!!!");
	}
		
	public void generateCsv() {
		//populate all the output if not present in mainOffer.csv and present only in account.csv
		TransformAdditionalRecord();
		
		//synchronized (ThreadLock)
		Map<String, Integer> DAMapping = new HashMap<String, Integer>();
		
		for (String str : tempdaBatchBuffer) {
			String msisdn = str.split(",")[1];
			if(!DAMapping.containsKey(msisdn))
			{
				DAMapping.put(msisdn, 1);
			}
			else
			{
				int temp = DAMapping.get(msisdn) + 1;
				DAMapping.put(msisdn, temp);
			}			
		}
		
		for(String str: tempdaBatchBuffer)
		{
			String msisdn = str.split(",")[1];
			String temp = str.replace("RULE4", String.valueOf(DAMapping.get(msisdn)));
			daBatchBuffer.add(temp);
		}
		
		Long Counter = 100000000L;
		for(String str: ucCreditLimitTempBuffer)
		{
			Counter++;
			String temp = str.replace("SeqNo", String.valueOf(Counter));
			ucCreditLimitBuffer.add(temp);
		}
		
		try {
			File accountFile = new File(this.pathOfWorkingFolder + "Account_" + uniqueNumber + ".csv");
			File subscriberFile = new File(this.pathOfWorkingFolder + "Subscriber_" + uniqueNumber + ".csv");
			File subscriberDATFile = new File(this.pathOfWorkingFolder + "SubscriberDAT_" + uniqueNumber + ".csv");
			File offerFile = new File(this.pathOfWorkingFolder + "Offer_" + uniqueNumber + ".csv");
			File utFile = new File(this.pathOfWorkingFolder + "UsageThreshold_" + uniqueNumber + ".csv");
			File pamFile = new File(this.pathOfWorkingFolder + "PamAccount_" + uniqueNumber + ".csv");
			File subscriberOfferFile = new File(this.pathOfWorkingFolder + "SubscriberOffer_" + uniqueNumber + ".csv");
			File providerOfferFile = new File(this.pathOfWorkingFolder + "ProviderOffer_" + uniqueNumber + ".csv");
			File ucFile = new File(this.pathOfWorkingFolder + "UsageCounter_" + uniqueNumber + ".csv");
			File daFile = new File(this.pathOfWorkingFolder + "DedicatedAccount_" + uniqueNumber + ".csv");
			File scFile = new File(this.pathOfWorkingFolder + "ServiceClass_" + uniqueNumber + ".csv");
			File dabFile = new File(this.pathOfWorkingFolder + "DedicatedAccountBatch_" + uniqueNumber + ".csv");
			File AFFile = new File(this.pathOfWorkingFolder + "AF_" + uniqueNumber + ".csv");
			File rejectedFile = new File(this.pathOfWorkingFolder + "Rejected_" + uniqueNumber +".log");
			File onlyLogFile = new File(this.pathOfWorkingFolder + "Exception_" + uniqueNumber + ".log");
			File ucCreditLimitFile = new File(this.pathOfWorkingFolder + "Credit_Limit_UC_" + uniqueNumber + ".csv");
			
			
			if (!accountFile.exists()) {
				accountFile.createNewFile();
			}
			if (!subscriberFile.exists()) {
				subscriberFile.createNewFile();
			}
			if (!subscriberDATFile.exists()) {
				subscriberDATFile.createNewFile();
			}
			if (!offerFile.exists()) {
				offerFile.createNewFile();
			}
			if (!utFile.exists()){ 
				utFile.createNewFile(); 
			}				 
			if (!pamFile.exists()) {
				pamFile.createNewFile();
			}
			if (!ucFile.exists()) {
				ucFile.createNewFile();
			}
			if (!daFile.exists()) {
				daFile.createNewFile();
			}
			if (!subscriberOfferFile.exists()) {
				subscriberOfferFile.createNewFile();
			}
			if (!providerOfferFile.exists()) {
				providerOfferFile.createNewFile();
			}
			if (!rejectedFile.exists()) {
				rejectedFile.createNewFile();
			}
			if (!onlyLogFile.exists()) {
				onlyLogFile.createNewFile();
			}
			if (!rejectedFile.exists()) {
				rejectedFile.createNewFile();
			}
			if(!AFFile.exists()){
				AFFile.createNewFile();
			}
			if(!dabFile.exists()){
				dabFile.createNewFile();
			}
			if(!scFile.exists()){
				scFile.createNewFile();
			}
			if(!ucCreditLimitFile.exists()){
				ucCreditLimitFile.createNewFile();
			}
			
			FileUtils.writeLines(accountFile, this.accountBuffer, true);
			FileUtils.writeLines(subscriberFile, this.subscriberBuffer, true);
			FileUtils.writeLines(subscriberDATFile, this.subscriberDATBuffer, true);
			FileUtils.writeLines(offerFile, this.offerBuffer, true);
			FileUtils.writeLines(utFile, this.utBuffer, true);
			FileUtils.writeLines(pamFile, this.pamBuffer, true);
			FileUtils.writeLines(ucFile, this.ucBuffer, true);
			FileUtils.writeLines(scFile, this.scBuffer, true);
			FileUtils.writeLines(subscriberOfferFile, this.subscriberOfferBuffer, true);
			FileUtils.writeLines(providerOfferFile, this.providerOfferBuffer, true);
			FileUtils.writeLines(daFile, this.daBuffer, true);
			FileUtils.writeLines(dabFile, this.daBatchBuffer, true);
			FileUtils.writeLines(AFFile, this.AFBuffer, true);
			FileUtils.writeLines(rejectedFile, this.rejectAndLog, true);
			FileUtils.writeLines(onlyLogFile, this.onlyLog, true);
			FileUtils.writeLines(ucCreditLimitFile, this.ucCreditLimitBuffer, true);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LOG.error("IO Exception ", e);
		}
		
		//populating the INCLogs
		//Read MSISDN_MisMatch.csv
		HashMap<String, Long> AddonException = new HashMap<String, Long>();
		try {
			  
			  BufferedReader br = new BufferedReader(new FileReader(pathOfMigtool + "/Temp/MSISDN_MisMatch.csv"));
			  String line = "";
			  while ((line = br.readLine()) != null) {
				 //INC50:MSISDN=34478848:OFFER_UV_UT=3000031--::ACTION=DISCARD & LOG
				String datas[] = line.split(":",-1);
				String offerID = datas[2].split("=")[1].split("-")[0];
				if(AddonException.containsKey(offerID))
				{
					Long count = AddonException.get(offerID) + 1L;
					
					AddonException.put(offerID,count);					
				}
				else
				{
					AddonException.put(offerID,1L);
				}
			}
		}
		catch (Exception e) {
			// TODO: handle exception
			e.getStackTrace();
		}
		/*try(BufferedWriter bw = new BufferedWriter(new FileWriter(pathOfMigtool +  "/Temp/targetNotInCSRejection.csv")))
		{
			HashMap<String, Long> notInCSLog = new HashMap<String, Long>();
			
			for(Entry<String,Long> str : RejectedException.entrySet())
			{
			  bw.append(str.getKey() + ":" + str.getValue());
			  bw.append(System.lineSeparator());
			}
			for(Entry<String,Long> str : AddonException.entrySet())
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
		generateLogs();
	}
	
	public synchronized void generateLogs(){
		try 
		{
			File DuplicateUTFile = new File(this.pathOfMigtool + "/logs/DuplicateUT.log");
			if (!DuplicateUTFile.exists()) {
				DuplicateUTFile.createNewFile();
			}
			FileUtils.writeLines(DuplicateUTFile, DuplicateUTLog, true);
		
			clearLogs();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LOG.error("IO Exception ", e);
		}	
				
	}
	
	public synchronized void clearLogs() throws IOException{
		rejectAndLog.clear();
		onlyLog.clear();		
	}

	public synchronized void  clearBuffer() {
		this.accountBuffer.clear();
		this.subscriberBuffer.clear();
		this.subscriberDATBuffer.clear();
		this.offerBuffer.clear();
		this.pamBuffer.clear();
		this.daBatchBuffer.clear();
		this.utBuffer.clear();
		this.ucBuffer.clear();		
		this.daBuffer.clear();
		this.subscriberOfferBuffer.clear();
		this.providerOfferBuffer.clear();
		this.rejectAndLog.clear();
		this.onlyLog.clear();
		this.AFBuffer.clear();
		this.scBuffer.clear();
	}
}
