package com.ericsson.dm.transform.implementation;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;

import com.ericsson.dm.Utils.CommonUtilities;
import com.ericsson.dm.inititialization.LoadSubscriberMapping;
import com.ericsson.dm.transformation.JsonInputFields;

public class DedicatedAccount {

	private int indx;
	//private ReadWriteLock rwl = new ReentrantReadWriteLock();
	Set<String> rejectAndLog;
	Set<String> onlyLog;
	Set<String> trackLog;
	CommonFunctions commonfunction;
	JsonInputFields inputObj;
	public DedicatedAccount()
	{
		
	}
	
	public DedicatedAccount(Set<String> rejectAndLog, Set<String> onlyLog, JsonInputFields inputObj) {
		
		// TODO Auto-generated constructor stub
		this.rejectAndLog=rejectAndLog;
		this.onlyLog=onlyLog;
		this.inputObj = inputObj;
		this.indx = 0;
	}

	public Map<String,String> execute() {
		// TODO Auto-generated method stub
		//Map<String, Map<String, String>> DAmap = new HashMap<>();		
		Map<String, String> DAmap = new HashMap<>();
		DAmap.putAll(generateDAFromMapping());
		return DAmap;
	}

	private Map<? extends String, ? extends String> generateDAFromMapping() {
		// TODO Auto-generated method stub
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date dateobj = new Date();
		
		Map<String,String> DAValues = new ConcurrentHashMap<>(1000, 0.75f, 30);
		this.indx ++;
		DAValues.put("ID_" + indx, LoadSubscriberMapping.CommonConfigMapping.get("Default_1000"));
		DAValues.put("BALANCE_" + indx, LoadSubscriberMapping.CommonConfigMapping.get("Default_Balance"));
		DAValues.put("START_DATE_" + indx,String.valueOf(CommonUtilities.convertDateToEpoch(df.format(dateobj))));
		DAValues.put("EXPIRY_DATE_" + indx,LoadSubscriberMapping.CommonConfigMapping.get("Default_NULL"));
		DAValues.put("PAM_SERVICE_ID_" + indx,LoadSubscriberMapping.CommonConfigMapping.get("Default_NULL"));
		DAValues.put("PRODUCT_ID_" + indx, LoadSubscriberMapping.CommonConfigMapping.get("Default_0"));
		
		return DAValues;
	}

}
