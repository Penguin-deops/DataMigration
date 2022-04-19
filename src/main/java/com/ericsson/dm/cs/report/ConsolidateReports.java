package com.ericsson.dm.cs.report;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Stream;


public class ConsolidateReports{
	public static Set<String> defaultOffer = new HashSet<String>(Arrays.asList("5001","7201"));
	
	public static Set<String> SharedOffer = new HashSet<String>(Arrays.asList("3000015","3000016","3000017","3000018","3000019","3000020","3000021","3000022","3000023","3000024","3000025","3000026","3000027","3000028","3000029","3000030"));
	
	String migToolPath = "";
	String tempDirectoryPath = "";
	String reportDirectoryPath = "";
	String SourceInputPath = "";
	HashMap<String, ReportStructure> OfferSourceReport;
	HashMap<String, ReportStructure> UCReport;
	HashMap<String, ReportStructure> UTReport;
	HashMap<String, ReportStructure> OfferImplicitReport;
	HashMap<String, ReportStructure> UCBalanceReport;
	HashMap<String, ReportStructure> UTBalanceReport;
	
	HashMap<String, ReportStructure> CLCountReport;
	HashMap<String, ReportStructure> CLBalanceReport;
	
    Set<String> OfferMSISDN;
    Set<String> UCMSISDN;
    Set<String> DAMSISDN;
    Set<String> UTMSISDN;
    Set<String> SubOfferMSISDN;
	
	public ConsolidateReports(String MigtoolPath) {
		// TODO Auto-generated constructor stub
		this.migToolPath = MigtoolPath;
		this.reportDirectoryPath = MigtoolPath + "/Reports/";
		this.tempDirectoryPath = MigtoolPath + "/Temp/";
		this.SourceInputPath = MigtoolPath + "/config/OfferDetails/";
		this.OfferSourceReport = new HashMap<String, ReportStructure>();
		this.OfferImplicitReport = new HashMap<String, ReportStructure>();
		
		this.UCReport = new HashMap<String, ReportStructure>();
		this.UTReport = new HashMap<String, ReportStructure>();
		this.UCBalanceReport = new HashMap<String, ReportStructure>();
		this.UTBalanceReport = new HashMap<String, ReportStructure>();
		
		this.OfferMSISDN = new HashSet<String>();
		this.UCMSISDN = new HashSet<String>();
		this.UTMSISDN = new HashSet<String>();
		this.DAMSISDN = new HashSet<String>();
		this.SubOfferMSISDN = new HashSet<String>();
		
		this.CLCountReport = new HashMap<String, ReportStructure>();
		this.CLBalanceReport = new HashMap<String, ReportStructure>();
	}
	
	public void execute()
    {	
		//Create Folder Structure
		CreateFolderStructure();
		
		//Offer in KPI format
		consolidateMainOfferReports();
		consolidateOfferImplicitReports();
		
		//UC in KPI format
		consolidateUsageCounterCountReports();
		consolidateUsageCounterBalanceReports();
		
		//UT in KPI format
		consolidateUsageThresholdCountReports();
		consolidateUsageThresholdBalanceReports();
				
		//consolidate summary View
		consolidateUsageCounterBalanceSummary();
		consolidateUsageThresholdBalanceSummary();
				
		//consolidate summary View for count
		consolidateUsageCounterCountSummary();
		consolidateUsageThresholdCountSummary();
		consolidateOfferCountSummary();
		
		//Move all DA files to reports
		consolidateDAReports("postSnapShotDABalance.csv","DAPostSnapShotDABalance.csv");
		consolidateDAReports("postSnapShotDACount.csv","DAPostSnapShotDACount.csv");
		consolidateDAReports("preSnapShotDABalance.csv","DAPreSnapShotDABalance.csv");
		consolidateDAReports("preSnapShotDACount.csv","DAPreSnapShotDACount.csv");
		consolidateDAReports("targetDedicatedAccountDatCount.csv","DADatCount.csv");
		consolidateDAReports("targetSUBSCRPTDATADatBalance.csv","DATargetBalance.csv");
		
		
		TimeBasedReporting tbf = new TimeBasedReporting(migToolPath);
		tbf.execute();
    
		//Phase2 KPI
		consolidateCreditLimitCountSummary();
		consolidateCreditLimitBalanceSummary();
		ConsolidateServiceClass();
    }

	private void CreateFolderStructure() {
		// TODO Auto-generated method stub
		try {

            Path path = Paths.get(this.reportDirectoryPath +"/Phase2/");
            Files.createDirectories(path);
            
            path = Paths.get(this.reportDirectoryPath +"/UC/");
            Files.createDirectories(path);
            
            path = Paths.get(this.reportDirectoryPath +"/UT/");
            Files.createDirectories(path);
            
            path = Paths.get(this.reportDirectoryPath +"/Offer/");
            Files.createDirectories(path);
            //Files.createDirectory(path);
        } 
		catch (IOException e) {
        	System.err.println("Failed to create directory!" + e.getMessage());
        }
	}
	
	private void ConsolidateServiceClass() {
		// TODO Auto-generated method stub
		HashMap<String, Long> SourceServiceClassCount = readFile(tempDirectoryPath+"Phase2sourceServiceClass.csv");
		HashMap<String, Long> TargetServiceClassCount = readFile(tempDirectoryPath+"Phase2TargetServiceClassCount.csv");
		HashMap<String, Long> RejectedServiceClassCount = readFile(tempDirectoryPath+"SCOrphanRejectionCount.csv");
		HashMap<String, Long> PreSnapSCCount = readFile(tempDirectoryPath+"preSubuscriberCount.csv");
		HashMap<String, Long> PostSnapSCCount = readFile(tempDirectoryPath+"postSubuscriberCount.csv");
		
		
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/Phase2/Phase2SCSource.csv"))){
			for(Entry<String, Long> SCDetails : SourceServiceClassCount.entrySet())
			{
				if(SCDetails.getKey().isEmpty())
					bw.append(SCDetails.getKey() + ",0");
				else
					bw.append(SCDetails.getKey() + "," + SCDetails.getValue());
				bw.append(System.lineSeparator());
				
			}
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		//Write discarded {4001=1, 3200=1, 3100=3}
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/Phase2/Phase2SCTarget.csv"))){
			for(Entry<String, Long> SCDetails : TargetServiceClassCount.entrySet())
			{
				long orphan = 0l;
				if(RejectedServiceClassCount.containsKey(SCDetails.getKey()))
					orphan = RejectedServiceClassCount.get(SCDetails.getKey());
				if(SCDetails.getKey().isEmpty())
					bw.append(SCDetails.getKey() + ",0,0");
				else
					bw.append(SCDetails.getKey() + "," + SCDetails.getValue() + ","+ orphan );
				bw.append(System.lineSeparator());
				
			}
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/Phase2/Phase2SCPostSnapshot.csv"))){
			for(Entry<String, Long> SCDetails : PostSnapSCCount.entrySet())
			{
				if(SCDetails.getKey().isEmpty())
					bw.append(SCDetails.getKey() + ",0");
				else
					bw.append(SCDetails.getKey() + "," + SCDetails.getValue());
				bw.append(System.lineSeparator());
				
			}
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/Phase2/Phase2SCPreSnapshot.csv"))){
			for(Entry<String, Long> SCDetails : PreSnapSCCount.entrySet())
			{
				if(SCDetails.getKey().isEmpty())
					bw.append(SCDetails.getKey() + ",0");
				else
					bw.append(SCDetails.getKey() + "," + SCDetails.getValue());
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

	private void consolidateCreditLimitBalanceSummary() {
		// TODO Auto-generated method stub
		HashMap<String, BigDecimal> SourceCLCount = readCLFileBalanceName(tempDirectoryPath+"Phase2sourceCreditLimitBalance.csv");
		
		HashMap<String, BigDecimal> SourceCL99Count = readCLFile99Balance(tempDirectoryPath+"Phase2sourceCreditLimitBalance.csv");
		//populate target stat
		HashMap<String, BigDecimal> targetMigrated = readCLBalance(tempDirectoryPath+"targetUsageThresholdBalance.csv");
		targetMigrated.putAll(readCLBalance(tempDirectoryPath+"Phase2targetCreditLimitUCBalance.csv"));
				
		//Rejection Count
		HashMap<String, BigDecimal> NotInCSRejection = readCLFileBalance(tempDirectoryPath+"targetRejectedCLBalance.csv");
		//NotInCSRejection.putAll(readFile(tempDirectoryPath+"targetNotInCSRejection.csv"));
		
		HashMap<String, BigDecimal> targetOutOfScope = readCLBalance(tempDirectoryPath+"targetOutOfScopeUCException.csv");
		HashMap<String, BigDecimal> targetExpiry = readCLBalance(tempDirectoryPath+"targetExpiryUCException.csv");
		
		//populatePreSnapShot
		HashMap<String, BigDecimal> preSnapShot = readCLBalance(tempDirectoryPath+"preSnapShotUTBalance.csv");
		preSnapShot.putAll(readCLBalance(tempDirectoryPath+"preSnapShotUCBalance.csv"));
		//populatePostSnapShot
		HashMap<String, BigDecimal> postSnapShot = readFileBigDecimal(tempDirectoryPath+"postSnapShotUTBalance.csv");
		postSnapShot.putAll(readFileBigDecimal(tempDirectoryPath+"postSnapShotUCBalance.csv"));
		List<String> CompleteSrcCLList = readFile(this.SourceInputPath ,"credit_limit");
		
		
		for(String UTDetails : CompleteSrcCLList)
		{
			String offerID = UTDetails.split(",")[0];
			if(CLBalanceReport.containsKey(String.valueOf(offerID)))
			{
				
			}
			else
			{
				ReportStructure rs = new ReportStructure();
				
				if( SourceCLCount.containsKey(String.valueOf(offerID)))
				{
					ReportStructure.SourceBalanceMetrics sm = rs.new SourceBalanceMetrics();
					BigDecimal OfferCount = new BigDecimal("0");
					BigDecimal OfferZeroCount = new BigDecimal("0");
					BigDecimal Offer99Count = new BigDecimal("0");
					if(SourceCLCount.get(String.valueOf(offerID)) != null);
						OfferCount = SourceCLCount.get(String.valueOf(offerID));
					if(SourceCL99Count.get(String.valueOf(offerID)) != null)
						Offer99Count = SourceCL99Count.get(String.valueOf(offerID));
					sm.populateSourceBalanceMetrics(offerID, OfferCount, OfferZeroCount, Offer99Count);
					rs.SrcBalanceMetric = sm;
				}				
				//populate from addonOffer
				else
				{
					ReportStructure.SourceBalanceMetrics sm = rs.new SourceBalanceMetrics();
					sm.populateSourceBalanceMetrics(String.valueOf(offerID), new BigDecimal("0"), new BigDecimal("0"), new BigDecimal("0"));
					rs.SrcBalanceMetric = sm;
				}
				
				String tempOfferId = offerID;
				
				if( targetMigrated.containsKey(String.valueOf(tempOfferId)) || NotInCSRejection.containsKey(String.valueOf(offerID)) 
						|| targetExpiry.containsKey(String.valueOf(offerID)) || targetOutOfScope.containsKey(String.valueOf(offerID)))
				{
					BigDecimal CountMigrated = new BigDecimal("0"); 
					CountMigrated =	targetMigrated.containsKey(String.valueOf(tempOfferId))?targetMigrated.get(String.valueOf(tempOfferId)):new BigDecimal("0");
					
					//Long CountNotInCS = NotInCSRejection.containsKey(String.valueOf(offerID))?NotInCSRejection.get(String.valueOf(offerID)):0;
					//Long OfferCountRejection = mainOffer.get(String.valueOf(offerID));
					//Long CountExpired = targetExpiry.containsKey(String.valueOf(offerID))?targetExpiry.get(String.valueOf(offerID)):0;
					BigDecimal CountoutofScope = new BigDecimal("0");
					CountoutofScope = NotInCSRejection.containsKey(String.valueOf(offerID))?NotInCSRejection.get(String.valueOf(offerID)):new BigDecimal("0");
					
					//String TargetID,Long TargetCount,Long TargetOutofscope,Long TargetExpired,Long TargetOrphans) {
					ReportStructure.TargetBalanceMetrics st = rs.new TargetBalanceMetrics();
					st.populateTargetBalanceMetrics(String.valueOf(offerID), CountMigrated, CountoutofScope);
					rs.TrgtBalanceMetric = st;
				}				
				
				//populatepreSnapShot
				if( preSnapShot.containsKey(String.valueOf(offerID)))
				{
					ReportStructure.SnapShotBeforeMetrics sbm = rs.new SnapShotBeforeMetrics();
					BigDecimal OfferCount = preSnapShot.get(String.valueOf(offerID));
					sbm.populateSnapShotBeforeMetricsBigDecimal(String.valueOf(offerID), OfferCount);	
					rs.preSnapMetic = sbm;
				}
				
				//populatePostSnapShot
				if( postSnapShot.containsKey(String.valueOf(offerID)))
				{
					ReportStructure.SnapShotAfterMetrics sam = rs.new SnapShotAfterMetrics();
					BigDecimal OfferCount = postSnapShot.get(String.valueOf(offerID));
					sam.populateSnapShotAfterMetricsBalance(String.valueOf(offerID), OfferCount);
					rs.postSnapMetic = sam;
				}
				
				CLBalanceReport.put(String.valueOf(offerID), rs);
			}
		}
		//Write source Offer
		HashMap<String, BigDecimal> totalSource  = new HashMap<String, BigDecimal>();
	
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/Phase2/Phase2CLSourceBalance.csv"))){
			for(String UTDetails : CompleteSrcCLList)
			{
				String offerID = UTDetails.split(",")[0];
				if(CLBalanceReport.containsKey(String.valueOf(offerID))) {
					ReportStructure rs =  CLBalanceReport.get(String.valueOf(offerID));
					ReportStructure.SourceBalanceMetrics sm = rs.new SourceBalanceMetrics();
					sm = rs.SrcBalanceMetric;
					if(sm.SourceID.isEmpty())
						bw.append(UTDetails + ",0,0,0");
					else
					{
						BigDecimal tempSource = new BigDecimal(String.valueOf(sm.SourceCount));
						
						tempSource = tempSource.add(new BigDecimal(String.valueOf(sm.SourceValidCount)));
						totalSource.put(offerID,tempSource);
						bw.append(UTDetails + "," + sm.SourceCount + "," + sm.SourceValidCount + "," + tempSource);
					}
					bw.append(System.lineSeparator());
				}
				else
				{
					bw.append(UTDetails + ",0,0,0" );
					bw.append(System.lineSeparator());
				}
			}			
			
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		//Write discarded
		HashMap<String, BigDecimal> totalForSnapSource  = new HashMap<String, BigDecimal>();
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/Phase2/Phase2CLTargetBalance.csv"))){
			for(String UTDetails : CompleteSrcCLList)
			{
				String offerID = UTDetails.split(",")[0];
				
				if(CLBalanceReport.containsKey(String.valueOf(offerID))) {
					ReportStructure rs =  CLBalanceReport.get(String.valueOf(offerID));
					ReportStructure.TargetBalanceMetrics sm = rs.new TargetBalanceMetrics();
					sm = rs.TrgtBalanceMetric;
					if(sm.TargetID.isEmpty())
						bw.append(offerID + ",0,0,0,0");
					else
					{
						
						//totalTarget = new BigDecimal(String.valueOf(sm.TargetCount)).divide(new BigDecimal("1000000"));
						BigDecimal totalTarget = new BigDecimal("0");
						if(offerID.equals("1000002"))
						{
							BigDecimal onlyBalance = new BigDecimal(String.valueOf(sm.TargetCount)).divide(new BigDecimal("1000"));
							totalTarget = onlyBalance.add(new BigDecimal(String.valueOf(sm.TargetOutofscope)));
							totalForSnapSource.put(offerID, onlyBalance);
						}
						else
						{
							BigDecimal onlyBalance = new BigDecimal(String.valueOf(sm.TargetCount)).divide(new BigDecimal("1000000"));
							totalTarget = onlyBalance.add(new BigDecimal(String.valueOf(sm.TargetOutofscope)));
							totalForSnapSource.put(offerID, onlyBalance);
						}
						BigDecimal finalResult = new BigDecimal("0");
						finalResult = totalSource.get(offerID).subtract(totalTarget);
						
						bw.append(offerID + "," + sm.TargetCount + "," + sm.TargetOutofscope + "," + totalTarget + "," + finalResult);
					}
					bw.append(System.lineSeparator());
				}
				else
				{
					bw.append(offerID + ",0,0,0,0" );
					bw.append(System.lineSeparator());
				}
			}			
			
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		//presnapshot
		HashMap<String, BigDecimal> PreSnapBalance = new HashMap<String, BigDecimal>();
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/Phase2/Phase2CLPreSnapshotBalance.csv"))){
			for(String UTDetails : CompleteSrcCLList)
			{
				String offerID = UTDetails.split(",")[0];
				if(CLBalanceReport.containsKey(String.valueOf(offerID))) {
					ReportStructure rs =  CLBalanceReport.get(String.valueOf(offerID));
					ReportStructure.SnapShotBeforeMetrics sm = rs.new SnapShotBeforeMetrics();
					sm = rs.preSnapMetic;
					if(sm.beforeSnapshotID.isEmpty() || sm.beforeSnapshotID == null)
					{
						PreSnapBalance.put(offerID, new BigDecimal("0"));
						bw.append(offerID + ",0");
					}
					else
					{
						PreSnapBalance.put(offerID, sm.beforeSnapshotCountUsage);
						bw.append(offerID+ "," + sm.beforeSnapshotCountUsage);
					}
					bw.append(System.lineSeparator());
				}
				else
				{
					bw.append(offerID + ",0" );
					bw.append(System.lineSeparator());
				}
			}			
			
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		//postsnapshot
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/Phase2/Phase2CLPostSnapshotBalance.csv"))){
			
			for(String UTDetails : CompleteSrcCLList)
			{
				String offerID = UTDetails.split(",")[0];
				if(CLBalanceReport.containsKey(String.valueOf(offerID))) {
					ReportStructure rs =  CLBalanceReport.get(String.valueOf(offerID));
					ReportStructure.SnapShotAfterMetrics sm = rs.new SnapShotAfterMetrics();
					sm = rs.postSnapMetic;
					if(sm.afterSnapshotID.isEmpty())
						bw.append(offerID + ",0");
					else
					{
						BigDecimal Result = new BigDecimal(String.valueOf(sm.afterSnapshotBalanceUsage)).subtract(PreSnapBalance.get(offerID));
						BigDecimal SourceTarget = totalForSnapSource.get(offerID).subtract(Result);
						
						bw.append(offerID + "," + sm.afterSnapshotBalanceUsage + "," + Result + "," + SourceTarget);
					}
					bw.append(System.lineSeparator());
				}
				else
				{
					bw.append(offerID + ",0" );
					bw.append(System.lineSeparator());
				}
			}			
			
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

	private void consolidateCreditLimitCountSummary() {
		// TODO Auto-generated method stub
		HashMap<String, Long> SourceCLCount = readCLFileNames(tempDirectoryPath+"Phase2sourceCreditLimitCount.csv");
		
		HashMap<String, Long> SourceCLZeroCount = readCLZeroFile(tempDirectoryPath+"Phase2sourceCreditLimitCount.csv");
		
		HashMap<String, Long> SourceCLDUMMYCount = readCLDummyFile(tempDirectoryPath+"Phase2sourceCreditLimitCount.csv");
		
		HashMap<String, Long> SourceCL99Count = readCL99File(tempDirectoryPath+"Phase2sourceCreditLimitCount.csv");
		//populate target stat
		HashMap<String, Long> targetMigrated = readFile(tempDirectoryPath+"targetUsageThresholdCount.csv");
		targetMigrated.putAll(readFile(tempDirectoryPath+"Phase2targetCreditLimitUCCount.csv"));
				
		//Rejection Count
		HashMap<String, Long> NotInCSRejection = readCLFile(tempDirectoryPath+"targetRejectedCLCount.csv");
		//NotInCSRejection.putAll(readFile(tempDirectoryPath+"targetNotInCSRejection.csv"));
		
		
		HashMap<String, Long> targetOutOfScope = readFile(tempDirectoryPath+"targetCLDummyRejectionCount.csv");
		HashMap<String, Long> targetExpiry = readFile(tempDirectoryPath+"targetExpiryUCException.csv");
		
		//populatePreSnapShot
		HashMap<String, Long> preSnapShot = readFile(tempDirectoryPath+"preSnapShotUTCount.csv");
		preSnapShot.putAll(readFile(tempDirectoryPath+"preSnapShotUCCount.csv"));
		//populatePostSnapShot
		HashMap<String, Long> postSnapShot = readFile(tempDirectoryPath+"postSnapShotUTCount.csv");
		postSnapShot.putAll(readFile(tempDirectoryPath+"postSnapShotUCCount.csv"));
		List<String> CompleteSrcCLList = readFile(this.SourceInputPath ,"credit_limit");
		
		
		for(String UTDetails : CompleteSrcCLList)
		{
			String offerID = UTDetails.split(",")[0];
			if(CLCountReport.containsKey(String.valueOf(offerID)))
			{
				
			}
			else
			{
				ReportStructure rs = new ReportStructure();
				
				if( SourceCLCount.containsKey(String.valueOf(offerID)))
				{
					ReportStructure.SourceMetrics sm = rs.new SourceMetrics();
					Long OfferCount = 0L;
					Long OfferZeroCount = 0L;
					Long Offer99Count = 0L;
					Long OfferDummyCount = 0L;
					if(SourceCLCount.get(String.valueOf(offerID)) != null);
						OfferCount = SourceCLCount.get(String.valueOf(offerID));
					if(SourceCLZeroCount.get(String.valueOf(offerID)) != null)
						OfferZeroCount = SourceCLZeroCount.get(String.valueOf(offerID));
					if(SourceCL99Count.get(String.valueOf(offerID)) != null)
						Offer99Count = SourceCL99Count.get(String.valueOf(offerID));
					if(SourceCLDUMMYCount.get(String.valueOf(offerID)) != null)
						OfferDummyCount = SourceCLDUMMYCount.get(String.valueOf(offerID));
					sm.populateSourceMetrics(String.valueOf(offerID), OfferCount, OfferZeroCount, OfferDummyCount, Offer99Count);
					rs.SrcMetric = sm;
				}				
				//populate from addonOffer
				else
				{
					ReportStructure.SourceMetrics sm = rs.new SourceMetrics();
					sm.populateSourceMetrics(String.valueOf(offerID), 0L, 0L, 0L);
					rs.SrcMetric = sm;
				}
				
				String tempOfferId = offerID;
				
				if( targetMigrated.containsKey(String.valueOf(tempOfferId)) || NotInCSRejection.containsKey(String.valueOf(offerID)) 
						|| targetExpiry.containsKey(String.valueOf(offerID)) || targetOutOfScope.containsKey(String.valueOf(offerID)))
				{
					Long CountMigrated = targetMigrated.containsKey(String.valueOf(tempOfferId))?targetMigrated.get(String.valueOf(tempOfferId)):0;
					
					//Long CountNotInCS = NotInCSRejection.containsKey(String.valueOf(offerID))?NotInCSRejection.get(String.valueOf(offerID)):0;
					//Long OfferCountRejection = mainOffer.get(String.valueOf(offerID));
					//Long CountExpired = targetExpiry.containsKey(String.valueOf(offerID))?targetExpiry.get(String.valueOf(offerID)):0;
					Long CountoutofScope = NotInCSRejection.containsKey(String.valueOf(offerID))?NotInCSRejection.get(String.valueOf(offerID)):0;
					
					//String TargetID,Long TargetCount,Long TargetOutofscope,Long TargetExpired,Long TargetOrphans) {
					ReportStructure.TargetMetrics st = rs.new TargetMetrics();
					st.populateTargetMetrics(String.valueOf(offerID), CountMigrated, CountoutofScope);
					rs.TrgtMetric = st;
				}				
				
				//populatepreSnapShot
				if( preSnapShot.containsKey(String.valueOf(offerID)))
				{
					ReportStructure.SnapShotBeforeMetrics sbm = rs.new SnapShotBeforeMetrics();
					Long OfferCount = preSnapShot.get(String.valueOf(offerID));
					sbm.populateSnapShotBeforeMetrics(String.valueOf(offerID), OfferCount);	
					rs.preSnapMetic = sbm;
				}
				
				//populatePostSnapShot
				if( postSnapShot.containsKey(String.valueOf(offerID)))
				{
					ReportStructure.SnapShotAfterMetrics sam = rs.new SnapShotAfterMetrics();
					Long OfferCount = postSnapShot.get(String.valueOf(offerID));
					sam.populateSnapShotAfterMetrics(String.valueOf(offerID), OfferCount);
					rs.postSnapMetic = sam;
				}
				
				CLCountReport.put(String.valueOf(offerID), rs);
			}
		}
		//Write source Offer
		HashMap<String,BigDecimal> totalSource = new HashMap<String,BigDecimal>();
		HashMap<String,BigDecimal> totalTarget = new HashMap<String,BigDecimal>();
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/Phase2/Phase2CLSource.csv"))){
			for(String UTDetails : CompleteSrcCLList)
			{
				String offerID = UTDetails.split(",")[0];
				if(CLCountReport.containsKey(String.valueOf(offerID))) {
					ReportStructure rs =  CLCountReport.get(String.valueOf(offerID));
					ReportStructure.SourceMetrics sm = rs.new SourceMetrics();
					sm = rs.SrcMetric;
					if(sm.SourceID.isEmpty())
						bw.append(UTDetails + ",0,0,0");
					else
					{
						
						BigDecimal tempSource = new BigDecimal(String.valueOf(sm.SourceCount));
						tempSource = tempSource.add(new BigDecimal(String.valueOf(sm.SourceValidCount))); //.subtract(new BigDecimal(String.valueOf(sm.SourceInValidCount)));
						bw.append(UTDetails + "," + sm.SourceCount + "," + sm.SourceInValidCount  + "," + sm.SourceValidCount + "," + sm.SourceDummyCount);
						totalSource.put(offerID, tempSource);
					}
					bw.append(System.lineSeparator());
				}
				else
				{
					bw.append(UTDetails + ",0,0,0" );
					bw.append(System.lineSeparator());
				}
			}			
			
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		//Write discarded
		HashMap<String, BigDecimal> totalForSnapSource  = new HashMap<String, BigDecimal>();
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/Phase2/Phase2CLTarget.csv"))){
			for(String UTDetails : CompleteSrcCLList)
			{
				String offerID = UTDetails.split(",")[0];
				
				if(CLCountReport.containsKey(String.valueOf(offerID))) {
					ReportStructure rs =  CLCountReport.get(String.valueOf(offerID));
					ReportStructure.TargetMetrics sm = rs.new TargetMetrics();
					sm = rs.TrgtMetric;
					if(sm.TargetID.isEmpty())
						bw.append(offerID + ",0,0,0");
					else
					{
						
						BigDecimal tempTarget = new BigDecimal(String.valueOf(sm.TargetCount));
						tempTarget = tempTarget.add(new BigDecimal(String.valueOf(sm.TargetOutofscope)));
						
						Long dummyMsisdn = 0l;
						if(targetOutOfScope.containsKey(offerID))
							dummyMsisdn = targetOutOfScope.get(offerID);
						
						BigDecimal finalResult = new BigDecimal("0");
						finalResult = totalSource.get(offerID).subtract(tempTarget).subtract(new BigDecimal(String.valueOf( dummyMsisdn)));
						totalForSnapSource.put(offerID,new BigDecimal(sm.TargetCount));
						
						//bw.append(offerID + "," + sm.TargetCount + "," + sm.TargetOutofscope + "," + dummyMsisdn + "," + tempTarget + "," + finalResult);
						bw.append(offerID + "," + sm.TargetCount + "," + sm.TargetOutofscope  );
					}
					bw.append(System.lineSeparator());
				}
				else
				{
					bw.append(offerID + ",0,0,0" );
					bw.append(System.lineSeparator());
				}
			}			
			
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		//presnapshot
		HashMap<String, BigDecimal> PreSnapCount = new HashMap<String, BigDecimal>();
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/Phase2/Phase2CLPreSnapshot.csv"))){
			for(String UTDetails : CompleteSrcCLList)
			{
				String offerID = UTDetails.split(",")[0];
				if(CLCountReport.containsKey(String.valueOf(offerID))) {
					ReportStructure rs =  CLCountReport.get(String.valueOf(offerID));
					ReportStructure.SnapShotBeforeMetrics sm = rs.new SnapShotBeforeMetrics();
					sm = rs.preSnapMetic;
					if(sm.beforeSnapshotID == null || sm.beforeSnapshotID.isEmpty())
					{
						PreSnapCount.put(offerID, new BigDecimal("0"));
						bw.append(offerID + ",0");
					}
					else
					{
						PreSnapCount.put(offerID, new BigDecimal(String.valueOf(sm.beforeSnapshotCount)));
						bw.append(offerID + "," + sm.beforeSnapshotCount);
					}
					bw.append(System.lineSeparator());
				}
				else
				{
					bw.append(offerID + ",0" );
					bw.append(System.lineSeparator());
				}
			}			
			
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		//postsnapshot
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/Phase2/Phase2CLPostSnapshot.csv"))){
			
			for(String UTDetails : CompleteSrcCLList)
			{
				String offerID = UTDetails.split(",")[0];
				if(CLCountReport.containsKey(String.valueOf(offerID))) {
					ReportStructure rs =  CLCountReport.get(String.valueOf(offerID));
					ReportStructure.SnapShotAfterMetrics sm = rs.new SnapShotAfterMetrics();
					sm = rs.postSnapMetic;
					
					ReportStructure rsource =  CLCountReport.get(String.valueOf(offerID));
					ReportStructure.SourceMetrics smSource = rsource.new SourceMetrics();
					smSource = rsource.SrcMetric;
					if(sm.afterSnapshotID.isEmpty())
						bw.append(offerID + ",0");
					else
					{
						//BigDecimal Result = new BigDecimal(String.valueOf(sm.afterSnapshotCount)).subtract(PreSnapCount.get(offerID));
						//BigDecimal SourceTarget = totalForSnapSource.get(offerID).subtract(Result);
						
						//if(offerID.equals("1000001"))
							bw.append(offerID + "," + sm.afterSnapshotCount + "," + SourceCLZeroCount.get(offerID));	
						//else
						//	bw.append(offerID + "," + sm.afterSnapshotCount + "," + smSource.SourceDummyCount);
					}
					bw.append(System.lineSeparator());
				}
				else
				{
					bw.append(offerID + ",0" );
					bw.append(System.lineSeparator());
				}
			}			
			
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	private void consolidateDAReports(String tempFile, String reportFile) {
		// TODO Auto-generated method stub
		File file = new File(this.tempDirectoryPath + tempFile); 
		file.renameTo(new File(reportDirectoryPath + reportFile));
	}

	private void consolidateOfferCountSummary() {
		// TODO Auto-generated method stub
		BigDecimal OfferCount = readFileSource(reportDirectoryPath+"/Offer/OfferSource.csv");
		BigDecimal implicitCount = readFileSource(reportDirectoryPath+"/Offer/ImplicitOfferSource.csv");
		BigDecimal DefaultOffer = readFileDefaultTarget(reportDirectoryPath+"/Offer/OfferTarget.csv");
		
		String SummaryValue = readFileTarget(reportDirectoryPath+"/Offer/OfferTarget.csv");
		BigDecimal OffTargetCount = new BigDecimal(SummaryValue.split(":")[0]);
		BigDecimal OffOutOfScopeTotalBalance = new BigDecimal(SummaryValue.split(":")[1]);
		BigDecimal OffExpiryTotalBalance = new BigDecimal(SummaryValue.split(":")[2]);
		BigDecimal OffRejectedTotalBalance = new BigDecimal(SummaryValue.split(":")[3]);
		
		String SummaryValueimplicit = readFileTarget(reportDirectoryPath+"/Offer/ImplicitOfferTarget.csv");
		BigDecimal implictTargetCount = new BigDecimal(SummaryValueimplicit.split(":")[0]);
		BigDecimal implictOutOfScopeTotalBalance = new BigDecimal(SummaryValueimplicit.split(":")[1]);
		BigDecimal implictExpiryTotalBalance = new BigDecimal(SummaryValueimplicit.split(":")[2]);
		BigDecimal implictRejectedTotalBalance = new BigDecimal(SummaryValueimplicit.split(":")[3]);
		
		BigDecimal SourceCount = OfferCount.add(implicitCount);
		BigDecimal targetCount = OffTargetCount.add(implictTargetCount);
		BigDecimal targetOutOfScope = OffOutOfScopeTotalBalance.add(implictOutOfScopeTotalBalance);
		BigDecimal targetExpiry = OffExpiryTotalBalance.add(implictExpiryTotalBalance);
		BigDecimal targetRejected = OffRejectedTotalBalance.add(implictRejectedTotalBalance);
		
		BigDecimal BeforeOffer = readFileSource(reportDirectoryPath+"/Offer/OfferPreSnapshot.csv");
		BigDecimal AfterOffer = readFileSource(reportDirectoryPath+"/Offer/OfferPostSnapshot.csv");
		BigDecimal BeforeImpOffer = readFileSource(reportDirectoryPath+"/Offer/ImplicitOfferPreSnapshot.csv");
		BigDecimal AfterImpOffer = readFileSource(reportDirectoryPath+"/Offer/ImplicitOfferPostSnapshot.csv");
		
		BigDecimal before = BeforeOffer.add(BeforeImpOffer);
		BigDecimal after = AfterOffer.add(AfterImpOffer);
		
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/Offer/OfferCountSummary.csv"))){
			bw.append(SourceCount + "," + targetCount + "," + targetOutOfScope + "," + targetExpiry + "," + targetRejected + "," + DefaultOffer);
			bw.append(System.lineSeparator());			
			bw.append(before + "," + after);
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	private void consolidateUsageThresholdCountSummary() {
		// TODO Auto-generated method stub
		HashMap<String, BigDecimal> UTSource = readFileSourceWithUnit(reportDirectoryPath+"/UT/UTSource.csv");
		HashMap<String, BigDecimal> UTSourceZero = readFileSourceWithUnitZero(reportDirectoryPath+"/UT/UTSource.csv");
		HashMap<String, String> SummaryValue = readFileTarget(reportDirectoryPath+"/UT/UTTarget.csv","UT_list_source");
		
		HashMap<String, BigDecimal> BeforeUCBalance = readFileSource(reportDirectoryPath+"/UT/UTPreSnapshot.csv","UT_list_source");
		HashMap<String, BigDecimal> AfterUCBalance = readFileSource(reportDirectoryPath+"/UT/UTPostSnapshot.csv","UT_list_source");
		
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/UT/UTCountSummary.csv"))){
			for(Entry<String,BigDecimal> entry : UTSource.entrySet())
			{
				String SummaryValue1 = SummaryValue.get(entry.getKey());
				BigDecimal UCTargetBalance = new BigDecimal(SummaryValue1.split(":")[0]);
				BigDecimal UCOutOfScopeTotalBalance = new BigDecimal(SummaryValue1.split(":")[1]);
				BigDecimal UCExpiryTotalBalance = new BigDecimal(SummaryValue1.split(":")[2]);
				BigDecimal UCRejectedTotalBalance = new BigDecimal(SummaryValue1.split(":")[3]);
				
				BigDecimal BeforeUC = new BigDecimal("0");
				BigDecimal AfterUC = new BigDecimal("0");
				if(BeforeUCBalance.containsKey(entry.getKey()))
					BeforeUC = BeforeUCBalance.get(entry.getKey());
				if(AfterUCBalance.containsKey(entry.getKey()))
					AfterUC = AfterUCBalance.get(entry.getKey());
				
				bw.append(entry.getKey() + "," + UTSource.get(entry.getKey()) + "," + UTSourceZero.get(entry.getKey()));
				bw.append(System.lineSeparator());			
				bw.append(UCTargetBalance + "," + UCOutOfScopeTotalBalance + "," + UCExpiryTotalBalance + "," + UCRejectedTotalBalance );
				bw.append(System.lineSeparator());
				bw.append(BeforeUC + "," + AfterUC);
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

	private void consolidateUsageCounterCountSummary() {
		// TODO Auto-generated method stub
		HashMap<String, BigDecimal> UCSource = readFileSourceWithUnit(reportDirectoryPath+"/UC/UCSource.csv");
		HashMap<String, BigDecimal> UCSourceZero = readFileSourceWithUnitZero(reportDirectoryPath+"/UC/UCSource.csv");
		HashMap<String, String> SummaryValue = readFileTarget(reportDirectoryPath+"/UC/UCTarget.csv","UC_list_source");
		
		//read snapshot
		HashMap<String, BigDecimal> BeforeUCBalance = readFileSource(reportDirectoryPath+"/UC/UCPreSnapshot.csv","UC_list_source");
		HashMap<String, BigDecimal> AfterUCBalance = readFileSource(reportDirectoryPath+"/UC/UCPostSnapshot.csv","UC_list_source");
		
		//Long UCSharedOfferBalance = readFileCommulativeSummary(tempDirectoryPath+"targetSUBSCRPTDATADat.csv");
		HashMap<String, Long> UCDABalance = readFileCommulativeSummary(tempDirectoryPath+"targetDedicatedAccountDatCount.csv", "DA_list.txt");
		
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/UC/UCCountSummary.csv"))){
			
			for(Entry<String,BigDecimal> entry : UCSource.entrySet())
			{
				String SummaryValue1 = SummaryValue.get(entry.getKey());
				BigDecimal UCTargetBalance = new BigDecimal(SummaryValue1.split(":")[0]);
				BigDecimal UCOutOfScopeTotalBalance = new BigDecimal(SummaryValue1.split(":")[1]);
				BigDecimal UCExpiryTotalBalance = new BigDecimal(SummaryValue1.split(":")[2]);
				BigDecimal UCRejectedTotalBalance = new BigDecimal(SummaryValue1.split(":")[3]);
				Long UCDA = 0l;
				if(UCDABalance.containsKey(entry.getKey()))
					UCDA = UCDABalance.get(entry.getKey());
				
				BigDecimal BeforeUC = new BigDecimal("0");
				BigDecimal AfterUC = new BigDecimal("0");
				if(BeforeUCBalance.containsKey(entry.getKey()))
					BeforeUC = BeforeUCBalance.get(entry.getKey());
				if(AfterUCBalance.containsKey(entry.getKey()))
					AfterUC = AfterUCBalance.get(entry.getKey());
				
				bw.append(entry.getKey() + "," + UCSource.get(entry.getKey()) + "," + UCSourceZero.get(entry.getKey()));
				bw.append(System.lineSeparator());			
				bw.append(UCTargetBalance + "," + UCOutOfScopeTotalBalance + "," + UCExpiryTotalBalance + "," + UCRejectedTotalBalance + "," + UCDA );
				bw.append(System.lineSeparator());
				bw.append(BeforeUC + "," + AfterUC);
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
	
	private String readFileTarget(String filename) {
		// TODO Auto-generated method stub
		String Result = "";
		BigDecimal readTargetValue = new BigDecimal("0");
		BigDecimal readOutOfScopeValue = new BigDecimal("0");
		BigDecimal readExpiryValue = new BigDecimal("0");
		BigDecimal readRejectedValue = new BigDecimal("0");
		try {
			  
		  BufferedReader br = new BufferedReader(new FileReader(filename));
		  String line = "";
		  while ((line = br.readLine()) != null) {
			  readTargetValue = readTargetValue.add(new BigDecimal(line.split(",")[1]));
			  readOutOfScopeValue = readOutOfScopeValue.add(new BigDecimal(line.split(",")[2]));
			  readExpiryValue = readExpiryValue.add(new BigDecimal(line.split(",")[3]));
			  readRejectedValue = readRejectedValue.add(new BigDecimal(line.split(",")[4]));
		  }
		}
		catch(Exception ex)
		   {
			   ex.printStackTrace();
		   }
		return readTargetValue + ":" + readOutOfScopeValue + ":" + readExpiryValue + ":" + readRejectedValue;
	}
	
	
	private void consolidateUsageThresholdBalanceSummary() {
		// TODO Auto-generated method stub
		HashMap<String, BigDecimal> UTSourceBalance = readFileSourceWithUnit(reportDirectoryPath+"/UT/UTSourceBalance.csv");
		//BigDecimal UTSourceZeroBalance = readFileSourceZero(reportDirectoryPath+"UTSourceBalance.csv");
		HashMap<String, String> SummaryValue = readFileTarget(reportDirectoryPath+"/UT/UTTargetBalance.csv","UT_list_source");
		
		HashMap<String, BigDecimal> BeforeUCBalance = readFileSource(reportDirectoryPath+"/UT/UTPreSnapshotBalance.csv","UT_list_source");
		HashMap<String, BigDecimal> AfterUCBalance = readFileSource(reportDirectoryPath+"/UT/UTPostSnapshotBalance.csv","UT_list_source");
		
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/UT/UTBalanceSummary.csv"))){
			//bw.append(UTSourceBalance + "," + UTSourceZeroBalance);
			//bw.append(System.lineSeparator());
			for(Entry<String,BigDecimal> entry : UTSourceBalance.entrySet())
			{
				String SummaryValue1  = SummaryValue.get(entry.getKey());
				BigDecimal UTTargetBalance = new BigDecimal(SummaryValue1.split(":")[0]);
				BigDecimal UTOutOfScopeTotalBalance = new BigDecimal(SummaryValue1.split(":")[1]);
				BigDecimal UTExpiryTotalBalance = new BigDecimal(SummaryValue1.split(":")[2]);
				BigDecimal UTRejectedTotalBalance = new BigDecimal(SummaryValue1.split(":")[3]);
				
				BigDecimal BeforeUC = new BigDecimal("0");
				BigDecimal AfterUC = new BigDecimal("0");
				if(BeforeUCBalance.containsKey(entry.getKey()))
					BeforeUC = BeforeUCBalance.get(entry.getKey());
				if(AfterUCBalance.containsKey(entry.getKey()))
					AfterUC = AfterUCBalance.get(entry.getKey());
				
				bw.append(entry.getKey() + "," + entry.getValue() + "," + UTTargetBalance + "," + UTOutOfScopeTotalBalance + "," + UTExpiryTotalBalance + "," + UTRejectedTotalBalance);
				bw.append(System.lineSeparator());	
				bw.append(BeforeUC + "," + AfterUC +"," + AfterUC.subtract(BeforeUC));
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

	private void consolidateUsageCounterBalanceSummary() {
		// TODO Auto-generated method stub
		HashMap<String, BigDecimal> UCSourceBalance = readFileSourceWithUnit(reportDirectoryPath+"/UC/UCSourceBalance.csv");
		HashMap<String, String> SummaryValue = readFileTarget(reportDirectoryPath+"/UC/UCTargetBalance.csv", "UC_list_source");
		
		HashMap<String, Long> UCSharedOfferBalance = readFileCommulativeSummary(tempDirectoryPath+"targetSUBSCRPTDATADatBalance.csv","DA_list.txt");
		HashMap<String, BigDecimal> UCSharedUTBalance = readFileSharedSummary(tempDirectoryPath+"targetUsageThresholdBalance.csv");
		
		HashMap<String, BigDecimal> AfterDA = readFileSourceForDA(tempDirectoryPath+"postSnapShotDABalance.csv");
		HashMap<String, BigDecimal> BeforeUC = readFileSource(reportDirectoryPath+"/UC/UCPreSnapshotBalance.csv", "UC_list_source");
		HashMap<String, BigDecimal> AfterUC = readFileSource(reportDirectoryPath+"/UC/UCPostSnapshotBalance.csv", "UC_list_source");
		
		//HashMap<String, BigDecimal> AfterUC = readFileSource(reportDirectoryPath+"UCPostSnapshotBalance.csv", "UC_list_source").add(AfterDA);
		
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/UC/UCBalanceSummary.csv"))){
			//bw.append(UCSourceBalance + "," + UCSourceZeroBalance);
			for(Entry<String,BigDecimal> entry : UCSourceBalance.entrySet())
			{
				String SummaryValue1 = SummaryValue.get(entry.getKey());
				BigDecimal UCTargetBalance = new BigDecimal(SummaryValue1.split(":")[0]);
				BigDecimal UCOutOfScopeTotalBalance = new BigDecimal(SummaryValue1.split(":")[1]);
				BigDecimal UCExpiryTotalBalance = new BigDecimal(SummaryValue1.split(":")[2]);
				BigDecimal UCRejectedTotalBalance = new BigDecimal(SummaryValue1.split(":")[3]);
				Long UCSharedOfferValue = 0L;
				if(UCSharedOfferBalance.containsKey(entry.getKey()))
					UCSharedOfferValue = UCSharedOfferBalance.get(entry.getKey());
				
				BigDecimal UCSharedUT = new BigDecimal("0");				
				if(UCSharedUTBalance.containsKey(entry.getKey()))
					UCSharedUT = UCSharedUTBalance.get(entry.getKey());
				
				BigDecimal BeforeUCBalance = new BigDecimal("0");
				if(BeforeUC.containsKey(entry.getKey()))
					BeforeUCBalance = BeforeUC.get(entry.getKey());
					
				BigDecimal AfterUCBalance = new BigDecimal("0");
				if(AfterUC.containsKey(entry.getKey()) && AfterDA.containsKey(entry.getKey()))
					AfterUCBalance = AfterUC.get(entry.getKey()).add(AfterDA.get(entry.getKey()));	
				else if(!AfterDA.containsKey(entry.getKey()))
					AfterUCBalance = AfterUC.get(entry.getKey());
				
				bw.append(entry.getKey() + "," + entry.getValue() + "," + UCTargetBalance + "," + UCOutOfScopeTotalBalance + "," + UCExpiryTotalBalance + "," + UCRejectedTotalBalance + "," + UCSharedOfferValue + "," + UCSharedUT);
				bw.append(System.lineSeparator());			
				bw.append(BeforeUCBalance + "," + AfterUCBalance +"," + AfterUCBalance.subtract(BeforeUCBalance));
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

	private void consolidateUsageThresholdBalanceReports() {
		// TODO Auto-generated method stub
		HashMap<String, Long> SourceUCCount = readFile(tempDirectoryPath+"sourceUsageThresholdBalances.csv");
		
		//populate target stat
		HashMap<String, Long> targetMigrated = readFile(tempDirectoryPath+"targetUsageThresholdBalance.csv");
				
		//Rejection Count
		HashMap<String, Long> NotInCSRejection = readFile(tempDirectoryPath+"targetUTRejectionBalance.csv");
		HashMap<String, Long> targetOutOfScope = readFile(tempDirectoryPath+"targetOutOfScopeUTExceptionBalance.csv");
		HashMap<String, Long> targetExpiry = readFile(tempDirectoryPath+"targetExpiryUTExceptionBalance.csv");
		
		//populatePreSnapShot
		HashMap<String, BigDecimal> preSnapShot = readFileUT(tempDirectoryPath+"preSnapShotUTBalance.csv");
		//populatePostSnapShot
		HashMap<String, BigDecimal> postSnapShot = readFileUT(tempDirectoryPath+"postSnapShotUTBalance.csv");
		List<String> CompleteSrcUTList = readFile(this.SourceInputPath ,"UT_list_source");
		List<String> CompleteTargetUTList = readFile(this.SourceInputPath ,"UT_list");
		
		HashMap<String, Long> targetSharedOfferMigrated = readFile(tempDirectoryPath+"targetSUBSCRPTDATADatBalance.csv");
		
		for(String SrcUT : CompleteSrcUTList)
		{
			/*
			 * if(UCBalanceReport.containsKey(String.valueOf(offerID))) {
			 * 
			 * } else
			 */
			{
				//populateMainOfferSource
				String offerID = SrcUT.split(",")[0];
				ReportStructure rs = new ReportStructure();
				
				if( SourceUCCount.containsKey(String.valueOf(offerID)))
				{
					ReportStructure.SourceMetrics sm = rs.new SourceMetrics();
					Long OfferCount = SourceUCCount.get(String.valueOf(offerID));
					sm.populateSourceMetrics(String.valueOf(offerID), OfferCount, 0L, 0L);
					rs.SrcMetric = sm;
				}				
				//populate from addonOffer
				else
				{
					ReportStructure.SourceMetrics sm = rs.new SourceMetrics();
					sm.populateSourceMetrics(String.valueOf(offerID), 0L, 0L, 0L);
					rs.SrcMetric = sm;
				}
				
				if( targetMigrated.containsKey(String.valueOf(offerID)) || NotInCSRejection.containsKey(String.valueOf(offerID)) 
						|| targetExpiry.containsKey(String.valueOf(offerID)) || targetOutOfScope.containsKey(String.valueOf(offerID)))
				{
					String tempOfferId = offerID;
					if(offerID.equals("35046") || offerID.equals("35047") || offerID.equals("35048"))
					{
						tempOfferId = tempOfferId.concat("09");
					}
					
					Long CountMigrated = targetMigrated.containsKey(String.valueOf(tempOfferId))?targetMigrated.get(String.valueOf(tempOfferId)):0;					
					Long CountNotInCS = NotInCSRejection.containsKey(String.valueOf(offerID))?NotInCSRejection.get(String.valueOf(offerID)):0;
					//Long OfferCountRejection = mainOffer.get(String.valueOf(offerID));
					Long CountExpired = targetExpiry.containsKey(String.valueOf(offerID))?targetExpiry.get(String.valueOf(offerID)):0;
					Long CountoutofScope = targetOutOfScope.containsKey(String.valueOf(offerID))?targetOutOfScope.get(String.valueOf(offerID)):0;
					
					//String TargetID,Long TargetCount,Long TargetOutofscope,Long TargetExpired,Long TargetOrphans) {
					ReportStructure.TargetMetrics st = rs.new TargetMetrics();
					st.populateTargetMetrics(String.valueOf(offerID), CountMigrated, CountoutofScope, CountExpired,CountNotInCS);
					rs.TrgtMetric = st;
				}				
				
				//populatepreSnapShot
				if( preSnapShot.containsKey(String.valueOf(offerID)))
				{
					ReportStructure.SnapShotBeforeMetrics sbm = rs.new SnapShotBeforeMetrics();
					BigDecimal OfferCount = preSnapShot.get(String.valueOf(offerID));
					sbm.populateSnapShotBeforeMetricsBigDecimal(String.valueOf(offerID), OfferCount);	
					rs.preSnapMetic = sbm;
				}
				
				//populatePostSnapShot
				if( postSnapShot.containsKey(String.valueOf(offerID)))
				{
					ReportStructure.SnapShotAfterMetrics sam = rs.new SnapShotAfterMetrics();
					BigDecimal OfferCount = postSnapShot.get(String.valueOf(offerID));
					sam.populateSnapShotAfterMetricsBigDecimal(String.valueOf(offerID),(OfferCount));
					rs.postSnapMetic = sam;
				}
				
				UTBalanceReport.put(String.valueOf(offerID), rs);
			}
		}
		//Write source Offer
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/UT/UTSourceBalance.csv"))){
			for(String SrcUT : CompleteSrcUTList)
			{
				String offerID = SrcUT.split(",")[0];
				if(UTBalanceReport.containsKey(String.valueOf(offerID))) {
					ReportStructure rs =  UTBalanceReport.get(String.valueOf(offerID));
					ReportStructure.SourceMetrics sm = rs.new SourceMetrics();
					sm = rs.SrcMetric;
					if(sm.SourceID.isEmpty())
						bw.append(SrcUT + ",0,0");
					else
						bw.append(SrcUT + "," + sm.SourceCount + "," + sm.SourceInValidCount);
					bw.append(System.lineSeparator());
				}
				else
				{
					bw.append(offerID + ",0,0" );
					bw.append(System.lineSeparator());
				}
			}			
			
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		//Write discarded
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/UT/UTTargetBalance.csv"))){
			for(String offerID : CompleteTargetUTList)
			{
				if(offerID.equals("3504609") || offerID.equals("3504709") || offerID.equals("3504809"))
				{
					offerID = offerID.replace("09", "");
				}
				if(UTBalanceReport.containsKey(String.valueOf(offerID))) {
					ReportStructure rs =  UTBalanceReport.get(String.valueOf(offerID));
					ReportStructure.TargetMetrics sm = rs.new TargetMetrics();
					sm = rs.TrgtMetric;
					if(sm.TargetID.isEmpty())
						bw.append(offerID + ",0,0,0,0");
					else
						bw.append(sm.TargetID + "," + sm.TargetCount + "," + sm.TargetOutofscope + "," + sm.TargetExpired + "," + sm.TargetOrphans);
					bw.append(System.lineSeparator());
				}
				else
				{
					bw.append(offerID + ",0" );
					bw.append(System.lineSeparator());
				}
			}			
			
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		//presnapshot
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/UT/UTPreSnapshotBalance.csv"))){
			for(String offerID : CompleteTargetUTList)
			{
				if(UTBalanceReport.containsKey(String.valueOf(offerID))) {
					ReportStructure rs =  UTBalanceReport.get(String.valueOf(offerID));
					ReportStructure.SnapShotBeforeMetrics sm = rs.new SnapShotBeforeMetrics();
					sm = rs.preSnapMetic;
					if(sm.beforeSnapshotID.isEmpty())
						bw.append(offerID + ",0");
					else
						bw.append(sm.beforeSnapshotID + "," + sm.beforeSnapshotCountUsage);
					bw.append(System.lineSeparator());
				}
				else
				{
					bw.append(offerID + ",0" );
					bw.append(System.lineSeparator());
				}
			}			
			
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		//postsnapshot
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/UT/UTPostSnapshotBalance.csv"))){
			for(String offerID : CompleteTargetUTList)
			{				
				if(UTBalanceReport.containsKey(String.valueOf(offerID))) {
					ReportStructure rs =  UTBalanceReport.get(String.valueOf(offerID));
					ReportStructure.SnapShotAfterMetrics sm = rs.new SnapShotAfterMetrics();
					sm = rs.postSnapMetic;
					if(sm.afterSnapshotID.isEmpty())
						bw.append(offerID + ",0");
					else
						bw.append(sm.afterSnapshotID + "," + sm.afterSnapshotCountUsage);
					bw.append(System.lineSeparator());
				}
				else
				{
					bw.append(offerID + ",0" );
					bw.append(System.lineSeparator());
				}
			}			
			
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	
	private void consolidateUsageThresholdCountReports() {
		// TODO Auto-generated method stub
		HashMap<String, Long> SourceUCCount = readFile(tempDirectoryPath+"SourceInputUsageThresholdCount.csv");
		HashMap<String, Long> SourceUCZeroCount = readFile(tempDirectoryPath+"SourceZeroUsageThresholdCount.csv");
		
		//populate target stat
		HashMap<String, Long> targetMigrated = readFile(tempDirectoryPath+"targetUsageThresholdCount.csv");
				
		//Rejection Count
		HashMap<String, Long> NotInCSRejection = readFile(tempDirectoryPath+"targetUTRejectionCount.csv");
		HashMap<String, Long> targetOutOfScope = readFile(tempDirectoryPath+"targetOutOfScopeUTException.csv");
		HashMap<String, Long> targetExpiry = readFile(tempDirectoryPath+"targetExpiryUTException.csv");
		
		//populatePreSnapShot
		HashMap<String, Long> preSnapShot = readFile(tempDirectoryPath+"preSnapShotUTCount.csv");
		//populatePostSnapShot
		HashMap<String, Long> postSnapShot = readFile(tempDirectoryPath+"postSnapShotUTCount.csv");
		List<String> CompleteSrcUTList = readFile(this.SourceInputPath ,"UT_list_source");
		List<String> CompleteTargetUTList = readFile(this.SourceInputPath ,"UT_list");
		
		for(String SrcUT : CompleteSrcUTList)
		{
			String offerID = SrcUT.split(",")[0];
			if(UTReport.containsKey(String.valueOf(offerID)))
			{
				
			}
			else
			{
				//populateMainOfferSource
				ReportStructure rs = new ReportStructure();
				
				if( SourceUCCount.containsKey(String.valueOf(offerID)))
				{
					ReportStructure.SourceMetrics sm = rs.new SourceMetrics();
					Long OfferCount = SourceUCCount.get(String.valueOf(offerID));
					Long OfferZeroCount = SourceUCZeroCount.containsKey(String.valueOf(offerID))?SourceUCZeroCount.get(String.valueOf(offerID)):0;					
					sm.populateSourceMetrics(String.valueOf(offerID), OfferCount, OfferZeroCount, 0L);
					rs.SrcMetric = sm;
				}				
				//populate from addonOffer
				else
				{
					ReportStructure.SourceMetrics sm = rs.new SourceMetrics();
					sm.populateSourceMetrics(String.valueOf(offerID), 0L, 0L, 0L);
					rs.SrcMetric = sm;
				}
				
				if( targetMigrated.containsKey(String.valueOf(offerID)) || NotInCSRejection.containsKey(String.valueOf(offerID)) 
						|| targetExpiry.containsKey(String.valueOf(offerID)) || targetOutOfScope.containsKey(String.valueOf(offerID)))
				{
					String tempOfferId = offerID;
					if(offerID.equals("35046") || offerID.equals("35047") || offerID.equals("35048"))
					{
						tempOfferId = tempOfferId.concat("09");
					}
					Long CountMigrated = targetMigrated.containsKey(String.valueOf(tempOfferId))?targetMigrated.get(String.valueOf(tempOfferId)):0;
					
					Long CountNotInCS = NotInCSRejection.containsKey(String.valueOf(offerID))?NotInCSRejection.get(String.valueOf(offerID)):0;
					//Long OfferCountRejection = mainOffer.get(String.valueOf(offerID));
					Long CountExpired = targetExpiry.containsKey(String.valueOf(offerID))?targetExpiry.get(String.valueOf(offerID)):0;
					Long CountoutofScope = targetOutOfScope.containsKey(String.valueOf(offerID))?targetOutOfScope.get(String.valueOf(offerID)):0;
					
					//String TargetID,Long TargetCount,Long TargetOutofscope,Long TargetExpired,Long TargetOrphans) {
					ReportStructure.TargetMetrics st = rs.new TargetMetrics();
					st.populateTargetMetrics(String.valueOf(offerID), CountMigrated, CountoutofScope, CountExpired,CountNotInCS);
					rs.TrgtMetric = st;
				}				
				
				//populatepreSnapShot
				if( preSnapShot.containsKey(String.valueOf(offerID)))
				{
					ReportStructure.SnapShotBeforeMetrics sbm = rs.new SnapShotBeforeMetrics();
					Long OfferCount = preSnapShot.get(String.valueOf(offerID));
					sbm.populateSnapShotBeforeMetrics(String.valueOf(offerID), OfferCount);	
					rs.preSnapMetic = sbm;
				}
				
				//populatePostSnapShot
				if( postSnapShot.containsKey(String.valueOf(offerID)))
				{
					ReportStructure.SnapShotAfterMetrics sam = rs.new SnapShotAfterMetrics();
					Long OfferCount = postSnapShot.get(String.valueOf(offerID));
					sam.populateSnapShotAfterMetrics(String.valueOf(offerID), OfferCount);
					rs.postSnapMetic = sam;
				}
				
				UTReport.put(String.valueOf(offerID), rs);
			}
		}
		//Write source Offer
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/UT/UTSource.csv"))){
			for(String SrcUT : CompleteSrcUTList)
			{
				String offerID = SrcUT.split(",")[0];
				if(UTReport.containsKey(String.valueOf(offerID))) {
					ReportStructure rs =  UTReport.get(String.valueOf(offerID));
					ReportStructure.SourceMetrics sm = rs.new SourceMetrics();
					sm = rs.SrcMetric;
					if(sm.SourceID.isEmpty())
						bw.append(SrcUT + ",0,0");
					else
						bw.append(SrcUT + "," + sm.SourceCount + "," + sm.SourceInValidCount);
					bw.append(System.lineSeparator());
				}
				else
				{
					bw.append(offerID + ",0,0" );
					bw.append(System.lineSeparator());
				}
			}			
			
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		//Write discarded
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/UT/UTTarget.csv"))){
			for(String offerID : CompleteTargetUTList)
			{
				if(offerID.equals("3504609") || offerID.equals("3504709") || offerID.equals("3504809"))
				{
					offerID = offerID.replace("09", "");
				}
				
				if(UTReport.containsKey(String.valueOf(offerID))) {
					ReportStructure rs =  UTReport.get(String.valueOf(offerID));
					ReportStructure.TargetMetrics sm = rs.new TargetMetrics();
					sm = rs.TrgtMetric;
					if(sm.TargetID.isEmpty())
						bw.append(offerID + ",0,0,0,0");
					else
						bw.append(sm.TargetID + "," + sm.TargetCount + "," + sm.TargetOutofscope + "," + sm.TargetExpired + "," + sm.TargetOrphans);
					bw.append(System.lineSeparator());
				}
				else
				{
					bw.append(offerID + ",0" );
					bw.append(System.lineSeparator());
				}
			}			
			
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		//presnapshot
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/UT/UTPreSnapshot.csv"))){
			for(String offerID : CompleteTargetUTList)
			{
				if(UTReport.containsKey(String.valueOf(offerID))) {
					ReportStructure rs =  UTReport.get(String.valueOf(offerID));
					ReportStructure.SnapShotBeforeMetrics sm = rs.new SnapShotBeforeMetrics();
					sm = rs.preSnapMetic;
					if(sm.beforeSnapshotID.isEmpty())
						bw.append(offerID + ",0");
					else
						bw.append(sm.beforeSnapshotID + "," + sm.beforeSnapshotCount);
					bw.append(System.lineSeparator());
				}
				else
				{
					bw.append(offerID + ",0" );
					bw.append(System.lineSeparator());
				}
			}			
			
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		//postsnapshot
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/UT/UTPostSnapshot.csv"))){
			for(String offerID : CompleteTargetUTList)
			{
				
				if(UTReport.containsKey(String.valueOf(offerID))) {
					ReportStructure rs =  UTReport.get(String.valueOf(offerID));
					ReportStructure.SnapShotAfterMetrics sm = rs.new SnapShotAfterMetrics();
					sm = rs.postSnapMetic;
					if(sm.afterSnapshotID.isEmpty())
						bw.append(offerID + ",0");
					else
						bw.append(sm.afterSnapshotID + "," + sm.afterSnapshotCount);
					bw.append(System.lineSeparator());
				}
				else
				{
					bw.append(offerID + ",0" );
					bw.append(System.lineSeparator());
				}
			}			
			
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	private void consolidateUsageCounterBalanceReports() {
		// TODO Auto-generated method stub
		HashMap<String, Long> SourceUCCount = readFile(tempDirectoryPath+"sourceUsageCounterBalances.csv");
		
		//populate target stat
		HashMap<String, Long> targetMigrated = readFile(tempDirectoryPath+"targetUsageCounterBalance.csv");
				
		//Rejection Count
		HashMap<String, Long> NotInCSRejection = readFile(tempDirectoryPath+"targetUCRejectionBalance.csv");
		HashMap<String, Long> targetOutOfScope = readFile(tempDirectoryPath+"targetOutOfScopeUCExceptionBalance.csv");
		HashMap<String, Long> targetExpiry = readFile(tempDirectoryPath+"targetExpiryUCExceptionBalance.csv");
		
		//populatePreSnapShot
		HashMap<String, BigDecimal> preSnapShot = readFileBigDecimal(tempDirectoryPath+"preSnapShotUCBalance.csv");
		//populatePostSnapShot
		HashMap<String, BigDecimal> postSnapShot = readFileBigDecimal(tempDirectoryPath+"postSnapShotUCBalance.csv");
		List<String> CompleteSrcUCList = readFile(this.SourceInputPath ,"UC_list_source");
		List<String> CompleteTargetUCList = readFile(this.SourceInputPath ,"UC_list");
		
		HashMap<String, Long> targetSharedOfferMigrated = readFile(tempDirectoryPath+"targetSUBSCRPTDATADatBalance.csv");
		
		for(String srcUC : CompleteSrcUCList)
		{
			{
				//populateMainOfferSource
				String offerID = srcUC.split(",")[0];
				ReportStructure rs = new ReportStructure();
				
				if( SourceUCCount.containsKey(String.valueOf(offerID)))
				{
					ReportStructure.SourceMetrics sm = rs.new SourceMetrics();
					Long OfferCount = SourceUCCount.get(String.valueOf(offerID));
					sm.populateSourceMetrics(String.valueOf(offerID), OfferCount, 0L, 0L);
					rs.SrcMetric = sm;
				}				
				//populate from addonOffer
				else
				{
					ReportStructure.SourceMetrics sm = rs.new SourceMetrics();
					sm.populateSourceMetrics(String.valueOf(offerID), 0L, 0L, 0L);
					rs.SrcMetric = sm;
				}
				
				String tempOfferId = offerID;
				if(offerID.equals("35046") || offerID.equals("35047") || offerID.equals("35048"))
				{
					tempOfferId = tempOfferId.concat("09");
				}
				
				
				if( targetMigrated.containsKey(String.valueOf(tempOfferId)) || NotInCSRejection.containsKey(String.valueOf(offerID)) 
						|| targetExpiry.containsKey(String.valueOf(offerID)) || targetOutOfScope.containsKey(String.valueOf(offerID)))
				{
										
					Long CountMigrated = targetMigrated.containsKey(String.valueOf(tempOfferId))?targetMigrated.get(String.valueOf(tempOfferId)):0;					
					Long CountNotInCS = NotInCSRejection.containsKey(String.valueOf(offerID))?NotInCSRejection.get(String.valueOf(offerID)):0;
					//Long OfferCountRejection = mainOffer.get(String.valueOf(offerID));
					Long CountExpired = targetExpiry.containsKey(String.valueOf(offerID))?targetExpiry.get(String.valueOf(offerID)):0;
					Long CountoutofScope = targetOutOfScope.containsKey(String.valueOf(offerID))?targetOutOfScope.get(String.valueOf(offerID)):0;
					
					//String TargetID,Long TargetCount,Long TargetOutofscope,Long TargetExpired,Long TargetOrphans) {
					ReportStructure.TargetMetrics st = rs.new TargetMetrics();
					st.populateTargetMetrics(String.valueOf(offerID), CountMigrated, CountoutofScope, CountExpired,CountNotInCS);
					rs.TrgtMetric = st;
				}				
				
				//populatepreSnapShot
				if( preSnapShot.containsKey(String.valueOf(offerID)))
				{
					ReportStructure.SnapShotBeforeMetrics sbm = rs.new SnapShotBeforeMetrics();
					BigDecimal ucBalance = preSnapShot.get(String.valueOf(offerID));
					sbm.populateSnapShotBeforeMetricsBalance(String.valueOf(offerID), ucBalance);	
					rs.preSnapMetic = sbm;
				}
				
				//populatePostSnapShot
				if( postSnapShot.containsKey(String.valueOf(tempOfferId)))
				{
					ReportStructure.SnapShotAfterMetrics sam = rs.new SnapShotAfterMetrics();
					BigDecimal ucBalance = postSnapShot.get(String.valueOf(tempOfferId));
					sam.populateSnapShotAfterMetricsBalance(String.valueOf(offerID), ucBalance);
					rs.postSnapMetic = sam;
				}				
				UCBalanceReport.put(String.valueOf(offerID), rs);
			}
		}
		//Write source Offer
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/UC/UCSourceBalance.csv"))){
			for(String srcUC : CompleteSrcUCList)
			{
				String offerID = srcUC.split(",")[0];
				if(UCBalanceReport.containsKey(String.valueOf(offerID))) {
					ReportStructure rs =  UCBalanceReport.get(String.valueOf(offerID));
					ReportStructure.SourceMetrics sm = rs.new SourceMetrics();
					sm = rs.SrcMetric;
					if(sm.SourceID.isEmpty())
						bw.append(srcUC + ",0,0");
					else
						bw.append(srcUC + "," + sm.SourceCount + "," + sm.SourceInValidCount);
					bw.append(System.lineSeparator());
				}
				else
				{
					bw.append(offerID + ",0,0" );
					bw.append(System.lineSeparator());
				}
			}			
			
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		//Write discarded
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/UC/UCTargetBalance.csv"))){
			for(String offerID : CompleteTargetUCList)
			{
				String tempOfferID = offerID;
				if(offerID.equals("3504609") || offerID.equals("3504709") || offerID.equals("3504809"))
				{
					offerID = offerID.replace("09", "");
				}
				if(UCBalanceReport.containsKey(String.valueOf(offerID))) {
					ReportStructure rs =  UCBalanceReport.get(String.valueOf(offerID));
					ReportStructure.TargetMetrics sm = rs.new TargetMetrics();
					sm = rs.TrgtMetric;
					if(sm.TargetID.isEmpty())
						bw.append(tempOfferID + ",0,0,0,0");
					else
						bw.append(tempOfferID + "," + sm.TargetCount + "," + sm.TargetOutofscope + "," + sm.TargetExpired + "," + sm.TargetOrphans);
					bw.append(System.lineSeparator());
				}
				else
				{
					bw.append(tempOfferID + ",0" );
					bw.append(System.lineSeparator());
				}
			}			
			
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		//presnapshot
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/UC/UCPreSnapshotBalance.csv"))){
			for(String offerID : CompleteTargetUCList)
			{
				if(UCBalanceReport.containsKey(String.valueOf(offerID))) {
					ReportStructure rs =  UCBalanceReport.get(String.valueOf(offerID));
					ReportStructure.SnapShotBeforeMetrics sm = rs.new SnapShotBeforeMetrics();
					sm = rs.preSnapMetic;
					if(sm.beforeSnapshotID.isEmpty())
						bw.append(offerID + ",0");
					else
						bw.append(sm.beforeSnapshotID + "," + sm.beforeSnapshotBalanceUsage);
					bw.append(System.lineSeparator());
				}
				else
				{
					bw.append(offerID + ",0" );
					bw.append(System.lineSeparator());
				}
			}			
			
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		//postsnapshot
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/UC/UCPostSnapshotBalance.csv"))){
			for(String offerID : CompleteTargetUCList)
			{
				String tempOfferID = offerID;
				if(offerID.equals("3504609") || offerID.equals("3504709") || offerID.equals("3504809"))
				{
					tempOfferID = tempOfferID.replace("09", "");
				}
				if(UCBalanceReport.containsKey(String.valueOf(tempOfferID))) {
					ReportStructure rs =  UCBalanceReport.get(String.valueOf(tempOfferID));
					ReportStructure.SnapShotAfterMetrics sm = rs.new SnapShotAfterMetrics();
					sm = rs.postSnapMetic;
					if(sm.afterSnapshotID.isEmpty())
						bw.append(offerID + ",0");
					else
						bw.append(offerID + "," + sm.afterSnapshotBalanceUsage);
					bw.append(System.lineSeparator());
				}
				else
				{
					bw.append(offerID + ",0" );
					bw.append(System.lineSeparator());
				}
			}			
			
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		//SharedOfffer migrated
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/UC/UCTargetSharedOfferBalance.csv"))){
			for(Entry<String, Long> str: targetSharedOfferMigrated.entrySet())
			{
				bw.append(str.getKey() + "," + str.getValue() );
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
	
	private void consolidateUsageCounterCountReports() {
		// TODO Auto-generated method stub
		HashMap<String, Long> SourceUCCount = readFile(tempDirectoryPath+"SourceInputUsageCounterCount.csv");
		HashMap<String, Long> SourceUCZeroCount = readFile(tempDirectoryPath+"SourceZeroUsageCounterCount.csv");
		
		//populate target stat
		HashMap<String, Long> targetMigrated = readFile(tempDirectoryPath+"targetUsageCounterCount.csv");
				
		//Rejection Count
		HashMap<String, Long> NotInCSRejection = readFile(tempDirectoryPath+"targetUCRejectionCount.csv");
		//NotInCSRejection.putAll(readFile(tempDirectoryPath+"targetNotInCSRejection.csv"));
		
		
		HashMap<String, Long> targetOutOfScope = readFile(tempDirectoryPath+"targetOutOfScopeUCException.csv");
		HashMap<String, Long> targetExpiry = readFile(tempDirectoryPath+"targetExpiryUCException.csv");
		
		//populatePreSnapShot
		HashMap<String, Long> preSnapShot = readFile(tempDirectoryPath+"preSnapShotUCCount.csv");
		//populatePostSnapShot
		HashMap<String, Long> postSnapShot = readFile(tempDirectoryPath+"postSnapShotUCCount.csv");
		List<String> CompleteSrcUCList = readFile(this.SourceInputPath ,"UC_list_source");
		List<String> CompleteTargetUCList = readFile(this.SourceInputPath ,"UC_list");
		
		HashMap<String, Long> targetSharedOfferMigrated = readFile(tempDirectoryPath+"targetDedicatedAccountDatCount.csv");
		
		for(String SrcUC : CompleteSrcUCList)
		{
			String offerID = SrcUC.split(",")[0];
			if(UCReport.containsKey(String.valueOf(offerID)))
			{
				
			}
			else
			{
				//populateMainOfferSource
				//if("35047".equals(offerID))
				//	System.out.println("Vipin Singh");
				ReportStructure rs = new ReportStructure();
				
				if( SourceUCCount.containsKey(String.valueOf(offerID)))
				{
					ReportStructure.SourceMetrics sm = rs.new SourceMetrics();
					Long OfferCount = 0L;
					Long OfferZeroCount = 0L;
					if(SourceUCCount.get(String.valueOf(offerID)) != null);
						OfferCount = SourceUCCount.get(String.valueOf(offerID));
					if(SourceUCZeroCount.get(String.valueOf(offerID)) != null)
						OfferZeroCount = SourceUCZeroCount.get(String.valueOf(offerID));
					sm.populateSourceMetrics(String.valueOf(offerID), OfferCount, OfferZeroCount, 0L);
					rs.SrcMetric = sm;
				}				
				//populate from addonOffer
				else
				{
					ReportStructure.SourceMetrics sm = rs.new SourceMetrics();
					sm.populateSourceMetrics(String.valueOf(offerID), 0L, 0L, 0L);
					rs.SrcMetric = sm;
				}
				
				String tempOfferId = offerID;
				if(offerID.equals("35046") || offerID.equals("35047") || offerID.equals("35048"))
				{
					tempOfferId = tempOfferId.concat("09");
				}
				
				if( targetMigrated.containsKey(String.valueOf(tempOfferId)) || NotInCSRejection.containsKey(String.valueOf(offerID)) 
						|| targetExpiry.containsKey(String.valueOf(offerID)) || targetOutOfScope.containsKey(String.valueOf(offerID)))
				{
					Long CountMigrated = targetMigrated.containsKey(String.valueOf(tempOfferId))?targetMigrated.get(String.valueOf(tempOfferId)):0;
					
					Long CountNotInCS = NotInCSRejection.containsKey(String.valueOf(offerID))?NotInCSRejection.get(String.valueOf(offerID)):0;
					//Long OfferCountRejection = mainOffer.get(String.valueOf(offerID));
					Long CountExpired = targetExpiry.containsKey(String.valueOf(offerID))?targetExpiry.get(String.valueOf(offerID)):0;
					Long CountoutofScope = targetOutOfScope.containsKey(String.valueOf(offerID))?targetOutOfScope.get(String.valueOf(offerID)):0;
					
					//String TargetID,Long TargetCount,Long TargetOutofscope,Long TargetExpired,Long TargetOrphans) {
					ReportStructure.TargetMetrics st = rs.new TargetMetrics();
					st.populateTargetMetrics(String.valueOf(offerID), CountMigrated, CountoutofScope, CountExpired,CountNotInCS);
					rs.TrgtMetric = st;
				}				
				
				//populatepreSnapShot
				if( preSnapShot.containsKey(String.valueOf(offerID)))
				{
					ReportStructure.SnapShotBeforeMetrics sbm = rs.new SnapShotBeforeMetrics();
					Long OfferCount = preSnapShot.get(String.valueOf(offerID));
					sbm.populateSnapShotBeforeMetrics(String.valueOf(offerID), OfferCount);	
					rs.preSnapMetic = sbm;
				}
				
				//populatePostSnapShot
				if( postSnapShot.containsKey(String.valueOf(tempOfferId)))
				{
					ReportStructure.SnapShotAfterMetrics sam = rs.new SnapShotAfterMetrics();
					Long OfferCount = postSnapShot.get(String.valueOf(tempOfferId));
					sam.populateSnapShotAfterMetrics(String.valueOf(offerID), OfferCount);
					rs.postSnapMetic = sam;
				}
				
				UCReport.put(String.valueOf(offerID), rs);
			}
		}
		//Write source Offer
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/UC/UCSource.csv"))){
			for(String SrcUC : CompleteSrcUCList)
			{
				String offerID = SrcUC.split(",")[0];
				if(UCReport.containsKey(String.valueOf(offerID))) {
					ReportStructure rs =  UCReport.get(String.valueOf(offerID));
					ReportStructure.SourceMetrics sm = rs.new SourceMetrics();
					sm = rs.SrcMetric;
					if(sm.SourceID.isEmpty())
						bw.append(SrcUC + ",0,0");
					else
						bw.append(SrcUC + "," + sm.SourceCount + "," + sm.SourceInValidCount);
					bw.append(System.lineSeparator());
				}
				else
				{
					bw.append(offerID + ",0,0" );
					bw.append(System.lineSeparator());
				}
			}			
			
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		//Write discarded
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/UC/UCTarget.csv"))){
			for(String offerID : CompleteTargetUCList)
			{
				String tempOfferID = offerID;
				if(offerID.equals("3504609") || offerID.equals("3504709") || offerID.equals("3504809"))
				{
					offerID = offerID.replace("09", "");
				}
				
				if(UCReport.containsKey(String.valueOf(offerID))) {
					ReportStructure rs =  UCReport.get(String.valueOf(offerID));
					ReportStructure.TargetMetrics sm = rs.new TargetMetrics();
					sm = rs.TrgtMetric;
					if(sm.TargetID.isEmpty())
						bw.append(tempOfferID + ",0,0,0,0");
					else
						bw.append(tempOfferID + "," + sm.TargetCount + "," + sm.TargetOutofscope + "," + sm.TargetExpired + "," + sm.TargetOrphans);
					bw.append(System.lineSeparator());
				}
				else
				{
					bw.append(tempOfferID + ",0" );
					bw.append(System.lineSeparator());
				}
			}			
			
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		//presnapshot
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/UC/UCPreSnapshot.csv"))){
			for(String offerID : CompleteTargetUCList)
			{
				if(UCReport.containsKey(String.valueOf(offerID))) {
					ReportStructure rs =  UCReport.get(String.valueOf(offerID));
					ReportStructure.SnapShotBeforeMetrics sm = rs.new SnapShotBeforeMetrics();
					sm = rs.preSnapMetic;
					if(sm.beforeSnapshotID.isEmpty())
						bw.append(offerID + ",0");
					else
						bw.append(sm.beforeSnapshotID + "," + sm.beforeSnapshotCount);
					bw.append(System.lineSeparator());
				}
				else
				{
					bw.append(offerID + ",0" );
					bw.append(System.lineSeparator());
				}
			}			
			
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		//postsnapshot
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/UC/UCPostSnapshot.csv"))){
			
			for(String offerID : CompleteTargetUCList)
			{
				String tempOfferID = offerID;
				if(offerID.equals("3504609") || offerID.equals("3504709") || offerID.equals("3504809"))
				{
					tempOfferID = tempOfferID.replace("09", "");
				}
				if(UCReport.containsKey(String.valueOf(tempOfferID))) {
					ReportStructure rs =  UCReport.get(String.valueOf(tempOfferID));
					ReportStructure.SnapShotAfterMetrics sm = rs.new SnapShotAfterMetrics();
					sm = rs.postSnapMetic;
					if(sm.afterSnapshotID.isEmpty())
						bw.append(offerID + ",0");
					else
						bw.append(offerID + "," + sm.afterSnapshotCount);
					bw.append(System.lineSeparator());
				}
				else
				{
					bw.append(offerID + ",0" );
					bw.append(System.lineSeparator());
				}
			}			
			
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		//SharedOfffer migrated
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/UC/UCTargetSharedOffer.csv"))){
			for(Entry<String, Long> str: targetSharedOfferMigrated.entrySet())
			{
				bw.append(str.getKey() + "," + str.getValue() );
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

	private void consolidateOfferImplicitReports() {
		// TODO Auto-generated method stub
		HashMap<String, Long> SharedOffer = readFile(tempDirectoryPath+"sourceSharedOfferStat.csv");
		HashMap<String, Long> implicitOffer = readFile(tempDirectoryPath+"sourceImplicitOfferStat.csv");
		
		HashMap<String, Long> targetSubuscriberOfferMigrated = readFile(tempDirectoryPath+"targetSubscriberOfferCount.csv");
		//populate target stat
		HashMap<String, Long> targetMigrated = readFile(tempDirectoryPath+"targetOfferCount.csv");
		//Rejection Count
		HashMap<String, Long> NotInCSRejection = readFile(tempDirectoryPath+"targetNotInCSRejection.csv");
		HashMap<String, Long> targetOfferRejectionCount = readFile(tempDirectoryPath+"targetOfferRejectionCount.csv");
		HashMap<String, Long> targetOutOfScope = readFile(tempDirectoryPath+"targetOutOfScopeOfferException.csv");
		//targetOutOfScope.putAll(readFile(tempDirectoryPath+"targetOfferRejectionCount.csv"));
		HashMap<String, Long> targetExpiry = readFile(tempDirectoryPath+"targetExpiryOfferException.csv");
		
		//populatePreSnapShot
		HashMap<String, Long> preSnapShotOffer = readFile(tempDirectoryPath+"preSnapShotOfferCount.csv");
		HashMap<String, Long> preSnapShotSubsOffer = readFile(tempDirectoryPath+"preSnapShotSubOfferCount.csv");
		//populatePostSnapShot
		HashMap<String, Long> postSnapShotOffer = readFile(tempDirectoryPath+"postSnapShotOfferCount.csv");
		HashMap<String, Long> postSnapShotSubsOffer = readFile(tempDirectoryPath+"postSnapShotSubOfferCount.csv");
		List<String> CompleteMainOfferList = readFile(this.SourceInputPath ,"shared_offer_list.txt");
		
		for(String offerID : CompleteMainOfferList)
		{
			if(OfferImplicitReport.containsKey(String.valueOf(offerID)))
			{
				
			}
			else
			{
				//populateMainOfferSource
				ReportStructure rs = new ReportStructure();
				
				if( SharedOffer.containsKey(String.valueOf(offerID)))
				{
					ReportStructure.SourceMetrics sm = rs.new SourceMetrics();
					Long OfferCount = SharedOffer.get(String.valueOf(offerID));					 
					sm.populateSourceMetrics(String.valueOf(offerID), OfferCount, 0L, 0L);
					rs.SrcMetric = sm;
				}				
				//populate from addonOffer
				else if( implicitOffer.containsKey(String.valueOf(offerID)))
				{
					ReportStructure.SourceMetrics sm = rs.new SourceMetrics();
					Long OfferCount = implicitOffer.get(String.valueOf(offerID));
					sm.populateSourceMetrics(String.valueOf(offerID), OfferCount, 0L, 0L);
					rs.SrcMetric = sm;
				}
				else
				{
					ReportStructure.SourceMetrics sm = rs.new SourceMetrics();
					sm.populateSourceMetrics(String.valueOf(offerID), 0L, 0L, 0L);
					rs.SrcMetric = sm;
				}
				
				
				//populateMainOffer/Addon Rejection
				if( targetMigrated.containsKey(String.valueOf(offerID)) || NotInCSRejection.containsKey(String.valueOf(offerID)) ||  
						targetOfferRejectionCount.containsKey(String.valueOf(offerID)) || targetExpiry.containsKey(String.valueOf(offerID))
						|| targetOutOfScope.containsKey(String.valueOf(offerID)) || targetSubuscriberOfferMigrated.containsKey(String.valueOf(offerID)))
				{
					Long OfferCountMigrated = 0L; 
					if(targetMigrated.containsKey(String.valueOf(offerID)))
						OfferCountMigrated = targetMigrated.get(String.valueOf(offerID));
					else if(targetSubuscriberOfferMigrated.containsKey(String.valueOf(offerID)))
						OfferCountMigrated = targetSubuscriberOfferMigrated.get(String.valueOf(offerID));
					else
						OfferCountMigrated = 0L;					
					Long OfferCountNotInCS = NotInCSRejection.containsKey(String.valueOf(offerID))?NotInCSRejection.get(String.valueOf(offerID)):0;
					//Long OfferCountRejection = mainOffer.get(String.valueOf(offerID));
					Long OfferCountExpired = targetExpiry.containsKey(String.valueOf(offerID))?targetExpiry.get(String.valueOf(offerID)):0;
					Long OfferCountoutofScope = targetOutOfScope.containsKey(String.valueOf(offerID))?targetOutOfScope.get(String.valueOf(offerID)):0;
					
					//String TargetID,Long TargetCount,Long TargetOutofscope,Long TargetExpired,Long TargetOrphans) {
					ReportStructure.TargetMetrics st = rs.new TargetMetrics();
					st.populateTargetMetrics(String.valueOf(offerID), OfferCountMigrated, OfferCountoutofScope, OfferCountExpired,OfferCountNotInCS);
					rs.TrgtMetric = st;
				}				
				
				//populatepreSnapShot
				if( preSnapShotOffer.containsKey(String.valueOf(offerID)))
				{
					ReportStructure.SnapShotBeforeMetrics sbm = rs.new SnapShotBeforeMetrics();
					Long OfferCount = preSnapShotOffer.get(String.valueOf(offerID));
					sbm.populateSnapShotBeforeMetrics(String.valueOf(offerID), OfferCount);	
					rs.preSnapMetic = sbm;
				}
				else if( preSnapShotSubsOffer.containsKey(String.valueOf(offerID)))
				{
					ReportStructure.SnapShotBeforeMetrics sbm = rs.new SnapShotBeforeMetrics();
					Long OfferCount = preSnapShotSubsOffer.get(String.valueOf(offerID));
					sbm.populateSnapShotBeforeMetrics(String.valueOf(offerID), OfferCount);	
					rs.preSnapMetic = sbm;
				}
				
				//populatePostSnapShot
				if( postSnapShotOffer.containsKey(String.valueOf(offerID)))
				{
					ReportStructure.SnapShotAfterMetrics sam = rs.new SnapShotAfterMetrics();
					Long OfferCount = postSnapShotOffer.get(String.valueOf(offerID));
					sam.populateSnapShotAfterMetrics(String.valueOf(offerID), OfferCount);
					rs.postSnapMetic = sam;
				}
				else if( postSnapShotSubsOffer.containsKey(String.valueOf(offerID)))
				{
					ReportStructure.SnapShotAfterMetrics sbm = rs.new SnapShotAfterMetrics();
					Long OfferCount = postSnapShotSubsOffer.get(String.valueOf(offerID));
					sbm.populateSnapShotAfterMetrics(String.valueOf(offerID), OfferCount);	
					rs.postSnapMetic = sbm;
				}
				
				OfferImplicitReport.put(String.valueOf(offerID), rs);
			}
		}		
		//write the output file.
		
		//Write source Offer
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/Offer/ImplicitOfferSource.csv"))){
			for(String offerID : CompleteMainOfferList)
			{
				if(OfferImplicitReport.containsKey(String.valueOf(offerID))) {
					ReportStructure rs =  OfferImplicitReport.get(String.valueOf(offerID));
					ReportStructure.SourceMetrics sm = rs.new SourceMetrics();
					sm = rs.SrcMetric;
					if(sm.SourceID.isEmpty())
						bw.append(offerID + ",0");
					else
						bw.append(sm.SourceID + "," + sm.SourceCount);
					bw.append(System.lineSeparator());
				}
				else
				{
					bw.append(offerID + ",0" );
					bw.append(System.lineSeparator());
				}
			}			
			
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		//Write discarded
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/Offer/ImplicitOfferTarget.csv"))){
			for(String offerID : CompleteMainOfferList)
			{
				if(OfferImplicitReport.containsKey(String.valueOf(offerID))) {
					ReportStructure rs =  OfferImplicitReport.get(String.valueOf(offerID));
					ReportStructure.TargetMetrics sm = rs.new TargetMetrics();
					sm = rs.TrgtMetric;
					if(sm.TargetID.isEmpty())
						bw.append(offerID + ",0,0,0,0");
					else
						bw.append(sm.TargetID + "," + sm.TargetCount + "," + sm.TargetOutofscope + "," + sm.TargetExpired + "," + sm.TargetOrphans);
					bw.append(System.lineSeparator());
				}
				else
				{
					bw.append(offerID + ",0" );
					bw.append(System.lineSeparator());
				}
			}			
			
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		//presnapshot
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/Offer/ImplicitOfferPreSnapshot.csv"))){
			for(String offerID : CompleteMainOfferList)
			{
				if(OfferImplicitReport.containsKey(String.valueOf(offerID))) {
					ReportStructure rs =  OfferImplicitReport.get(String.valueOf(offerID));
					ReportStructure.SnapShotBeforeMetrics sm = rs.new SnapShotBeforeMetrics();
					sm = rs.preSnapMetic;
					if(sm.beforeSnapshotID.isEmpty())
						bw.append(offerID + ",0");
					else
						bw.append(sm.beforeSnapshotID + "," + sm.beforeSnapshotCount);
					bw.append(System.lineSeparator());
				}				
				else
				{
					bw.append(offerID + ",0" );
					bw.append(System.lineSeparator());
				}
			}
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		//postsnapshot
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/Offer/ImplicitOfferPostSnapshot.csv"))){
			for(String offerID : CompleteMainOfferList)
			{
				if(OfferImplicitReport.containsKey(String.valueOf(offerID))) {
					ReportStructure rs =  OfferImplicitReport.get(String.valueOf(offerID));
					ReportStructure.SnapShotAfterMetrics sm = rs.new SnapShotAfterMetrics();
					sm = rs.postSnapMetic;
					if(sm.afterSnapshotID.isEmpty())
						bw.append(offerID + ",0");
					else
						bw.append(sm.afterSnapshotID + "," + sm.afterSnapshotCount);
					bw.append(System.lineSeparator());
				}
				else
				{
					bw.append(offerID + ",0" );
					bw.append(System.lineSeparator());
				}
			}			
			
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
	}

	private void consolidateMainOfferReports() {
		// TODO Auto-generated method stub
		HashMap<String, Long> mainOffer = readFile(tempDirectoryPath+"sourceMainOfferStat.csv");
		HashMap<String, Long> addonOffer = readFile(tempDirectoryPath+"sourceAddonOfferStat.csv");
		
		//populate target stat
		HashMap<String, Long> targetMigrated = readFile(tempDirectoryPath+"targetOfferCount.csv");
		//Rejection Count
		HashMap<String, Long> NotInCSRejection = readFile(tempDirectoryPath+"targetNotInCSRejection.csv");
		HashMap<String, Long> targetOfferRejectionCount = readFile(tempDirectoryPath+"targetOfferRejectionCount.csv");
		HashMap<String, Long> targetOutOfScope = readFile(tempDirectoryPath+"targetOutOfScopeOfferException.csv");
		HashMap<String, Long> targetExpiry = readFile(tempDirectoryPath+"targetExpiryOfferException.csv");
		
		//populatePreSnapShot
		HashMap<String, Long> preSnapShotOffer = readFile(tempDirectoryPath+"preSnapShotOfferCount.csv");
		//populatePostSnapShot
		HashMap<String, Long> postSnapShotOffer = readFile(tempDirectoryPath+"postSnapShotOfferCount.csv");
		List<String> CompleteMainOfferList = readFile(this.SourceInputPath ,"main_offer_list.txt");
		
		for(String offerID : CompleteMainOfferList)
		{
			if(OfferSourceReport.containsKey(String.valueOf(offerID)))
			{
				
			}
			else
			{
				//populateMainOfferSource
				//if("35047".equals(offerID))
				//	System.out.println("Vipin Singh Offer");
				ReportStructure rs = new ReportStructure();
				
				if( mainOffer.containsKey(String.valueOf(offerID)))
				{
					ReportStructure.SourceMetrics sm = rs.new SourceMetrics();
					Long OfferCount = mainOffer.get(String.valueOf(offerID));					 
					sm.populateSourceMetrics(String.valueOf(offerID), OfferCount, 0L, 0L);
					rs.SrcMetric = sm;
				}				
				//populate from addonOffer
				else if( addonOffer.containsKey(String.valueOf(offerID)))
				{
					ReportStructure.SourceMetrics sm = rs.new SourceMetrics();
					Long OfferCount = addonOffer.get(String.valueOf(offerID));
					sm.populateSourceMetrics(String.valueOf(offerID), OfferCount, 0L, 0L);
					rs.SrcMetric = sm;
				}
				else
				{
					ReportStructure.SourceMetrics sm = rs.new SourceMetrics();
					sm.populateSourceMetrics(String.valueOf(offerID), 0L, 0L, 0L);
					rs.SrcMetric = sm;
				}
				
				
				//populateMainOffer/Addon Rejection
				if( targetMigrated.containsKey(String.valueOf(offerID)) || NotInCSRejection.containsKey(String.valueOf(offerID)) ||  
						targetOfferRejectionCount.containsKey(String.valueOf(offerID)) || targetExpiry.containsKey(String.valueOf(offerID))
						|| targetOutOfScope.containsKey(String.valueOf(offerID)))
				{
					Long OfferCountMigrated = targetMigrated.containsKey(String.valueOf(offerID))?targetMigrated.get(String.valueOf(offerID)):0;					
					
					Long OfferCountNotInCS = NotInCSRejection.containsKey(String.valueOf(offerID))?NotInCSRejection.get(String.valueOf(offerID)):0;
					Long OfferCountExpired = targetExpiry.containsKey(String.valueOf(offerID))?targetExpiry.get(String.valueOf(offerID)):0;
					Long OfferCountoutofScope = targetOutOfScope.containsKey(String.valueOf(offerID))?targetOutOfScope.get(String.valueOf(offerID)):0;
					
					//String TargetID,Long TargetCount,Long TargetOutofscope,Long TargetExpired,Long TargetOrphans) {
					ReportStructure.TargetMetrics st = rs.new TargetMetrics();
					st.populateTargetMetrics(String.valueOf(offerID), OfferCountMigrated, OfferCountoutofScope, OfferCountExpired,OfferCountNotInCS);
					rs.TrgtMetric = st;
				}				
				
				//populatepreSnapShot
				if( preSnapShotOffer.containsKey(String.valueOf(offerID)))
				{
					ReportStructure.SnapShotBeforeMetrics sbm = rs.new SnapShotBeforeMetrics();
					Long OfferCount = preSnapShotOffer.get(String.valueOf(offerID));
					sbm.populateSnapShotBeforeMetrics(String.valueOf(offerID), OfferCount);	
					rs.preSnapMetic = sbm;
				}
				
				//populatePostSnapShot
				if( postSnapShotOffer.containsKey(String.valueOf(offerID)))
				{
					ReportStructure.SnapShotAfterMetrics sam = rs.new SnapShotAfterMetrics();
					Long OfferCount = postSnapShotOffer.get(String.valueOf(offerID));
					sam.populateSnapShotAfterMetrics(String.valueOf(offerID), OfferCount);
					rs.postSnapMetic = sam;
				}
				
				OfferSourceReport.put(String.valueOf(offerID), rs);
			}
		}		
		//write the output file.
		
		//Write source Offer
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/Offer/OfferSource.csv"))){
			for(String offerID : CompleteMainOfferList)
			{
				if(OfferSourceReport.containsKey(String.valueOf(offerID))) {
					ReportStructure rs =  OfferSourceReport.get(String.valueOf(offerID));
					ReportStructure.SourceMetrics sm = rs.new SourceMetrics();
					sm = rs.SrcMetric;
					if(sm.SourceID.isEmpty())
						bw.append(offerID + ",0");
					else
						bw.append(sm.SourceID + "," + sm.SourceCount);
					bw.append(System.lineSeparator());
				}
				else
				{
					bw.append(offerID + ",0" );
					bw.append(System.lineSeparator());
				}
			}			
			
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		//Write discarded
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/Offer/OfferTarget.csv"))){
			for(String offerID : CompleteMainOfferList)
			{
				if(OfferSourceReport.containsKey(String.valueOf(offerID))) {
					ReportStructure rs =  OfferSourceReport.get(String.valueOf(offerID));
					ReportStructure.TargetMetrics sm = rs.new TargetMetrics();
					sm = rs.TrgtMetric;
					if(sm.TargetID.isEmpty())
						bw.append(offerID + ",0,0,0,0");
					else
						bw.append(sm.TargetID + "," + sm.TargetCount + "," + sm.TargetOutofscope + "," + sm.TargetExpired + "," + sm.TargetOrphans);
					bw.append(System.lineSeparator());
				}
				else
				{
					bw.append(offerID + ",0" );
					bw.append(System.lineSeparator());
				}
			}			
			
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		//presnapshot
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/Offer/OfferPreSnapshot.csv"))){
			for(String offerID : CompleteMainOfferList)
			{
				if(OfferSourceReport.containsKey(String.valueOf(offerID))) {
					ReportStructure rs =  OfferSourceReport.get(String.valueOf(offerID));
					ReportStructure.SnapShotBeforeMetrics sm = rs.new SnapShotBeforeMetrics();
					sm = rs.preSnapMetic;
					if(sm.beforeSnapshotID.isEmpty())
						bw.append(offerID + ",0");
					else
						bw.append(sm.beforeSnapshotID + "," + sm.beforeSnapshotCount);
					bw.append(System.lineSeparator());
				}
				else
				{
					bw.append(offerID + ",0" );
					bw.append(System.lineSeparator());
				}
			}			
			
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		//postsnapshot
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(reportDirectoryPath +  "/Offer/OfferPostSnapshot.csv"))){
			for(String offerID : CompleteMainOfferList)
			{
				if(OfferSourceReport.containsKey(String.valueOf(offerID))) {
					ReportStructure rs =  OfferSourceReport.get(String.valueOf(offerID));
					ReportStructure.SnapShotAfterMetrics sm = rs.new SnapShotAfterMetrics();
					sm = rs.postSnapMetic;
					if(sm.afterSnapshotID.isEmpty())
						bw.append(offerID + ",0");
					else
						bw.append(sm.afterSnapshotID + "," + sm.afterSnapshotCount);
					bw.append(System.lineSeparator());
				}
				else
				{
					bw.append(offerID + ",0" );
					bw.append(System.lineSeparator());
				}
			}			
			
			bw.flush();
			bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

	private HashMap<String, Long> readFileCommulativeSummary(String filename, String DefinationMapping) {
		// TODO Auto-generated method stub
		Long readInputFile = 0L;
		HashMap<String, Long> totalDABalance = new HashMap<String, Long>();
		HashMap<String, String> DAdefination = new HashMap<String, String>();
		DAdefination.putAll(readDefinationFile(this.SourceInputPath ,"DA_list.txt"));
		try 
		{	  
		  BufferedReader br = new BufferedReader(new FileReader(filename));
		  String line = "";
		  while ((line = br.readLine()) != null) {		
			  String daMapping = DAdefination.get(line.split(":")[0].trim());
			  if(totalDABalance.containsKey(daMapping))
			  {				  
				  Long temp = Long.parseLong(line.split(":")[1]);
				  readInputFile = (totalDABalance.get(daMapping)) + temp;
				  totalDABalance.put(DAdefination.get(line.split(":")[0].trim()), readInputFile);
			  }
			  else
			  {
				  readInputFile = Long.parseLong(line.split(":")[1]);
				  totalDABalance.put(DAdefination.get(line.split(":")[0]), readInputFile);
			  }			  		
		   }
		}
		catch(Exception ex)
		{
		   ex.printStackTrace();
		}
		return totalDABalance;
	}
	
	private Map<String,String> readDefinationFile(String sourceInputPath, String filename) {
		// TODO Auto-generated method stub
		HashMap<String, String> TargetResult = new HashMap<String, String>();
		try 
		{
			
			 BufferedReader br = new BufferedReader(new FileReader(sourceInputPath + filename));
			 String line = "";
			 while ((line = br.readLine()) != null) {
				 TargetResult.put(line.split(",")[0].trim(), line.split(",")[1].trim());
			 }
		}
		catch(Exception ex)
		{
		   ex.printStackTrace();
		}
		return TargetResult;
	}

	private HashMap<String, BigDecimal> readFileSharedSummary(String filename) {
		// TODO Auto-generated method stub\
		HashMap<String, BigDecimal> totalBalance = new HashMap<String, BigDecimal>();
		HashMap<String, String> sharedDefination = new HashMap<String, String>();
		sharedDefination.putAll(readDefinationFile(this.SourceInputPath ,"UT_list_source"));
		
		
		try 
		{
			BufferedReader br = new BufferedReader(new FileReader(filename));
		  	String line = "";
		  	while ((line = br.readLine()) != null) {
			  
			  if(SharedOffer.contains(line.split(":")[0]))
			  {
				  String daMapping = sharedDefination.get(line.split(":")[0].trim());
				  if(totalBalance.containsKey(daMapping))
				  {
					  BigDecimal readInputFile = new BigDecimal(Long.parseLong(line.split(":")[1]));
					  readInputFile = readInputFile.add(totalBalance.get(daMapping));
					  totalBalance.put(daMapping, readInputFile);
				  }
				  else
				  {
					  BigDecimal readInputFile = new BigDecimal(Long.parseLong(line.split(":")[1]));
					  totalBalance.put(daMapping, readInputFile);
				  }
			  }
		  }
		}
		catch(Exception ex)
	    {
		   ex.printStackTrace();
	    }
		return totalBalance;
	}
	
	private BigDecimal readFileSourceZero(String filename) {
		// TODO Auto-generated method stub
		BigDecimal readInputFile = new BigDecimal("0");
		try {
			  
		  BufferedReader br = new BufferedReader(new FileReader(filename));
		  String line = "";
		  while ((line = br.readLine()) != null) {
			  readInputFile = readInputFile.add(new BigDecimal(line.split(",")[2]));		
		  }
		}
		catch(Exception ex)
		{
			   ex.printStackTrace();
		}
		return readInputFile;
	}
	
	private BigDecimal readFileSource(String filename) {
		// TODO Auto-generated method stub
		BigDecimal readInputFile = new BigDecimal("0");
		try {
			  
		  BufferedReader br = new BufferedReader(new FileReader(filename));
		  String line = "";
		  while ((line = br.readLine()) != null) {
			  
			  readInputFile = readInputFile.add(new BigDecimal(line.split(",")[1]));		
		  }
		}
		catch(Exception ex)
		   {
				System.out.println("Exception came from file: " + filename);
			    ex.printStackTrace();
		   }
		return readInputFile;
	}
	
	private HashMap<String, BigDecimal> readFileSource(String filename, String sourceFileName) {
		// TODO Auto-generated method stub
		HashMap<String, BigDecimal> totalBalance = new HashMap<String, BigDecimal>();
		HashMap<String, String> definationMap = new HashMap<String, String>();
		definationMap.putAll(readDefinationFile(this.SourceInputPath ,sourceFileName));
		
		try {
			  
		  BufferedReader br = new BufferedReader(new FileReader(filename));
		  String line = "";
		  while ((line = br.readLine()) != null) {
			  String daMapping = definationMap.get(line.split(",")[0].trim());
			  if(totalBalance.containsKey(daMapping))
			  {				  
				  BigDecimal readInputFile = new BigDecimal(line.split(",")[1]);
				  readInputFile = readInputFile.add(totalBalance.get(daMapping));
				  totalBalance.put(definationMap.get(line.split(",")[0].trim()), readInputFile);
			  }
			  else
			  {
				  BigDecimal readInputFile = new BigDecimal(line.split(",")[1]);
				  totalBalance.put(definationMap.get(line.split(",")[0]), readInputFile);
			  }	
		  }
		}
		catch(Exception ex)
		   {
				System.out.println("Exception came from file: " + filename);
			    ex.printStackTrace();
		   }
		return totalBalance;
	}
	
	private HashMap<String, BigDecimal> readFileSourceWithUnit(String filename) {
		// TODO Auto-generated method stub
		//prepare a Set which will have offer based on unit.
		//HashMap<String,List<String>> CompleteSrcUTList = readUnitOfferFile(this.SourceInputPath ,"UT_list_source");
		
		HashMap<String, BigDecimal> TotalBalance = new HashMap<String, BigDecimal>();
		try {
		  BufferedReader br = new BufferedReader(new FileReader(filename));
		  String line = "";
		  while ((line = br.readLine()) != null) {
			  
			  if(TotalBalance.containsKey(line.split(",")[1].trim()))
			  {
				  BigDecimal temp = new BigDecimal(line.split(",")[2]);
				  temp = temp.add(TotalBalance.get(line.split(",")[1].trim()));
				  TotalBalance.put(line.split(",")[1].trim(), temp);
			  }
			  else
			  {
				  BigDecimal temp = new BigDecimal(line.split(",")[2]);
				  TotalBalance.put(line.split(",")[1].trim(), temp);
			  }
				 
			  //readInputFile = readInputFile.add(new BigDecimal(line.split(",")[0]));		
		  }
		}
		catch(Exception ex)
		{
			System.out.println("Exception came from file: " + filename);
		    ex.printStackTrace();
		}
		return TotalBalance;
	}
	
	private HashMap<String, BigDecimal> readFileSourceWithUnitZero(String filename) {
		// TODO Auto-generated method stub
		//prepare a Set which will have offer based on unit.
		//HashMap<String,List<String>> CompleteSrcUTList = readUnitOfferFile(this.SourceInputPath ,"UT_list_source");
		
		HashMap<String, BigDecimal> TotalBalance = new HashMap<String, BigDecimal>();
		
		BigDecimal readInputFile = new BigDecimal("0");
		try {
			  
		  BufferedReader br = new BufferedReader(new FileReader(filename));
		  String line = "";
		  while ((line = br.readLine()) != null) {
			  
			  if(TotalBalance.containsKey(line.split(",")[1].trim()))
			  {
				  BigDecimal temp = new BigDecimal(line.split(",")[3]);
				  temp = temp.add(TotalBalance.get(line.split(",")[1].trim()));
				  TotalBalance.put(line.split(",")[1].trim(), temp);
			  }
			  else
			  {
				  BigDecimal temp = new BigDecimal(line.split(",")[3]);
				  TotalBalance.put(line.split(",")[1].trim(), temp);
			  }
				 
			  //readInputFile = readInputFile.add(new BigDecimal(line.split(",")[0]));		
		  }
		}
		catch(Exception ex)
		{
			System.out.println("Exception came from file: " + filename);
		    ex.printStackTrace();
		}
		return TotalBalance;
	}
	
	private HashMap<String, BigDecimal> readFileSourceForDA(String filename) {
		// TODO Auto-generated method stub
		HashMap<String, BigDecimal> totalDABalance = new HashMap<String, BigDecimal>();
		HashMap<String, String> DAdefination = new HashMap<String, String>();
		DAdefination.putAll(readDefinationFile(this.SourceInputPath ,"DA_list.txt"));
		try {
			  
		  BufferedReader br = new BufferedReader(new FileReader(filename));
		  String line = "";
		  while ((line = br.readLine()) != null) {
			  String daMapping = DAdefination.get(line.split(":")[0].trim());
			  if(totalDABalance.containsKey(daMapping))
			  {				  
				  BigDecimal temp = new BigDecimal(line.split(":")[1]);
				  BigDecimal readInputFile = new BigDecimal("0");
				  readInputFile = totalDABalance.get(daMapping).add(temp);
				  totalDABalance.put(DAdefination.get(line.split(":")[0].trim()), readInputFile);
			  }
			  else
			  {
				  BigDecimal readInputFile = new BigDecimal(line.split(":")[1]);
				  totalDABalance.put(DAdefination.get(line.split(":")[0]), readInputFile);
			  }		
		  }
		}
		catch(Exception ex)
		   {
			   ex.printStackTrace();
		   }
		return totalDABalance;
	}
	
	private BigDecimal readFileDefaultTarget(String filename) {
		// TODO Auto-generated method stub
		BigDecimal readInputFile = new BigDecimal("0");
		try {
			  
		  BufferedReader br = new BufferedReader(new FileReader(filename));
		  String line = "";
		  while ((line = br.readLine()) != null) {
			  if(defaultOffer.contains(line.split(",")[0]))
			  {
				  readInputFile = readInputFile.add(new BigDecimal(line.split(",")[1]));
			  }
		  }
		}
		catch(Exception ex)
		   {
			   ex.printStackTrace();
		   }
		return readInputFile;
	}
	
	private HashMap<String,String> readFileTarget(String filename,String SourceFileName) {
		// TODO Auto-generated method stub
		String Result = "";
		HashMap<String,String> TargetResult= new HashMap<String, String>();
		try {
			  
		  BufferedReader br = new BufferedReader(new FileReader(filename));
		  String line = "";
		  while ((line = br.readLine()) != null) {
			  String OfferID = line.split(",")[0];
			  String unit = getUnitKey(OfferID,SourceFileName);
			  
			  if(!unit.isEmpty() && TargetResult.containsKey(unit))
			  {
				  String existing_value  = TargetResult.get(unit);
				  
				  BigDecimal readTargetValue = new BigDecimal(existing_value.split(":")[0]);
				  BigDecimal readOutOfScopeValue = new BigDecimal(existing_value.split(":")[1]);
				  BigDecimal readExpiryValue = new BigDecimal(existing_value.split(":")[2]);
				  BigDecimal readRejectedValue = new BigDecimal(existing_value.split(":")[3]);
				  
				  readTargetValue = readTargetValue.add(new BigDecimal(line.split(",")[1]));
				  readOutOfScopeValue = readOutOfScopeValue.add(new BigDecimal(line.split(",")[2]));
				  readExpiryValue = readExpiryValue.add(new BigDecimal(line.split(",")[3]));
				  readRejectedValue = readRejectedValue.add(new BigDecimal(line.split(",")[4])); 
				  TargetResult.put(unit, readTargetValue + ":" + readOutOfScopeValue + ":" + readExpiryValue + ":" + readRejectedValue);
			  }
			  else
			  {
				  BigDecimal readTargetValue = new BigDecimal(line.split(",")[1]);
				  BigDecimal readOutOfScopeValue = new BigDecimal(line.split(",")[2]);
				  BigDecimal readExpiryValue = new BigDecimal(line.split(",")[3]);
				  BigDecimal readRejectedValue = new BigDecimal(line.split(",")[3]);
				  
				  TargetResult.put(unit, readTargetValue + ":" + readOutOfScopeValue + ":" + readExpiryValue + ":" + readRejectedValue);
			  }
		  }
		}
		catch(Exception ex)
		   {
			   ex.printStackTrace();
		   }
		return TargetResult;
	}
	
	private HashMap<String, BigDecimal> readFileBigDecimal(String filename) {
		// TODO Auto-generated method stub
		HashMap<String, BigDecimal> readInputFile = new HashMap<String, BigDecimal>();
		try {
			  
		  BufferedReader br = new BufferedReader(new FileReader(filename));
		  String line = "";
		  while ((line = br.readLine()) != null) {
			String datas[] = line.split(":",-1);
			if(!datas[0].isEmpty())
			{
				if(readInputFile.containsKey(datas[0]))
				{
					BigDecimal temp = readInputFile.get(datas[0]).add(new BigDecimal(datas[1])); 
					readInputFile.put(datas[0], temp);
				}
				else
				{
					readInputFile.put(datas[0], new BigDecimal(datas[1]));
				}
			}
		  }
		}
		catch(Exception ex)
		   {
			   ex.printStackTrace();
		   }
		return readInputFile;
	}
	
	private HashMap<String, Long> readFile(String filename) {
		// TODO Auto-generated method stub
		HashMap<String, Long> readInputFile = new HashMap<String, Long>();
		try {
			  
		  BufferedReader br = new BufferedReader(new FileReader(filename));
		  String line = "";
		  while ((line = br.readLine()) != null) {
			String datas[] = line.split(":",-1);
			if(!datas[0].isEmpty())
			{
				if(readInputFile.containsKey(datas[0]))
				{
					Long temp = readInputFile.get(datas[0]) + Long.parseLong(datas[1]); 
					readInputFile.put(datas[0], temp);
				}
				else
				{
					readInputFile.put(datas[0], Long.parseLong(datas[1]));
				}
			}
		  }
		}
		catch(Exception ex)
		{
			System.out.println("Exception from file: " + filename);
			ex.printStackTrace();
		}
		return readInputFile;
	}
	
	private HashMap<String, Long> readCLFile(String filename) {
		// TODO Auto-generated method stub
		HashMap<String, Long> readInputFile = new HashMap<String, Long>();
		try {
			  
		  BufferedReader br = new BufferedReader(new FileReader(filename));
		  String line = "";
		  while ((line = br.readLine()) != null) {
			String datas[] = line.split(":",-1);
			if(!datas[0].isEmpty())
			{
				//Map to UT
				/*String MappedUT = "";
				if(datas[0].equals("REFERENCE_CREDIT_LIMIT"))
					MappedUT = "10000101";
				else if(datas[0].equals("SUBSCRIBER_BALANCE"))
					MappedUT = "10000102";
				else if(datas[0].equals("AVAILABLE_CREDIT_LIMIT"))
					MappedUT = "10000103";
				else if(datas[0].equals("MINPAY_FACTOR"))
					MappedUT = "10000104";
				else if(datas[0].equals("NOTIFICATION_70"))
					MappedUT = "10000105";
				else if(datas[0].equals("NOTIFICATION_80"))
					MappedUT = "10000106";
				else if(datas[0].equals("NOTIFICATION_90"))
					MappedUT = "10000107";
				else if(datas[0].equals("USAGE_VALUE"))
					MappedUT = "1000001";
				
				if(!MappedUT.isEmpty())
				{
					if(readInputFile.containsKey(datas[0]))
					{
						Long temp = readInputFile.get(datas[0]) + Long.parseLong(datas[1]); 
						readInputFile.put(MappedUT, temp);
					}
					else
					{
						readInputFile.put(MappedUT, Long.parseLong(datas[1]));
					}
				}*/
				
				if(readInputFile.containsKey(datas[0]))
				{
					Long temp = readInputFile.get(datas[0]) + Long.parseLong(datas[1]); 
					readInputFile.put(datas[0], temp);
				}
				else
				{
					readInputFile.put(datas[0], Long.parseLong(datas[1]));
				}
			}
		  }
		}
		catch(Exception ex)
		   {
			   ex.printStackTrace();
		   }
		return readInputFile;
	}
	
	
	
	private HashMap<String, Long> readCLFileNames(String filename) {
		// TODO Auto-generated method stub
		HashMap<String, Long> readInputFile = new HashMap<String, Long>();
		try {
			  
		  BufferedReader br = new BufferedReader(new FileReader(filename));
		  String line = "";
		  while ((line = br.readLine()) != null) {
			String datas[] = line.split(":",-1);
			if(!datas[0].isEmpty())
			{
				//Map to UT
				String MappedUT = "";
				if(datas[0].equals("REFERENCE_CREDIT_LIMIT_SRC"))
					MappedUT = "100000202";
				else if(datas[0].equals("SUBSCRIBER_BALANCE"))
					MappedUT = "10000102";
				else if(datas[0].equals("AVAILABLE_CREDIT_LIMIT_SRC"))
					MappedUT = "100000201";
				else if(datas[0].equals("MINPAY_FACTOR"))
					MappedUT = "10000104";
				else if(datas[0].equals("NOTIFICATION_70"))
					MappedUT = "10000105";
				else if(datas[0].equals("NOTIFICATION_80"))
					MappedUT = "10000106";
				else if(datas[0].equals("NOTIFICATION_90"))
					MappedUT = "10000107";
				else if(datas[0].equals("USAGE_VALUE_SRC"))
					MappedUT = "1000002";
				
				if(!MappedUT.isEmpty())
				{
					if(readInputFile.containsKey(datas[0]))
					{
						Long temp = readInputFile.get(datas[0]) + Long.parseLong(datas[1]); 
						readInputFile.put(MappedUT, temp);
					}
					else
					{
						readInputFile.put(MappedUT, Long.parseLong(datas[1]));
					}
				}
				
				
			}
		  }
		}
		catch(Exception ex)
		   {
			   ex.printStackTrace();
		   }
		return readInputFile;
	}
	
	private HashMap<String, Long> readCLZeroFile(String filename) {
		// TODO Auto-generated method stub
		HashMap<String, Long> readInputFile = new HashMap<String, Long>();
		try {
			  
		  BufferedReader br = new BufferedReader(new FileReader(filename));
		  String line = "";
		  while ((line = br.readLine()) != null) {
			String datas[] = line.split(":",-1);
			if(!datas[0].isEmpty())
			{
				//Map to UT
				String MappedUT = "";
				if(datas[0].equals("REFERENCE_CREDIT_LIMIT_ZERO"))
					MappedUT = "100000202";
				else if(datas[0].equals("SUBSCRIBER_BALANCE_ZERO"))
					MappedUT = "10000102";
				else if(datas[0].equals("AVAILABLE_CREDIT_LIMIT_ZERO"))
					MappedUT = "100000201";
				else if(datas[0].equals("MINPAY_FACTOR_ZERO_ZERO"))
					MappedUT = "10000104";
				/*else if(datas[0].equals("NOTIFICATION_70"))
					MappedUT = "10000105";
				else if(datas[0].equals("NOTIFICATION_80"))
					MappedUT = "10000106";
				else if(datas[0].equals("NOTIFICATION_90"))
					MappedUT = "10000107";*/
				else if(datas[0].equals("USAGE_VALUE_ZERO"))
					MappedUT = "1000002";
				
				if(!MappedUT.isEmpty())
				{
					if(readInputFile.containsKey(datas[0]))
					{
						Long temp = readInputFile.get(datas[0]) + Long.parseLong(datas[1]); 
						readInputFile.put(MappedUT, temp);
					}
					else
					{
						readInputFile.put(MappedUT, Long.parseLong(datas[1]));
					}
				}
			}
		  }
		}
		catch(Exception ex)
		   {
			   ex.printStackTrace();
		   }
		return readInputFile;
	}
	
	private HashMap<String, Long> readCLDummyFile(String filename) {
		// TODO Auto-generated method stub
		HashMap<String, Long> readInputFile = new HashMap<String, Long>();
		try {
			  
		  BufferedReader br = new BufferedReader(new FileReader(filename));
		  String line = "";
		  while ((line = br.readLine()) != null) {
			String datas[] = line.split(":",-1);
			if(!datas[0].isEmpty())
			{
				//Map to UT
				String MappedUT = "";
				if(datas[0].equals("REFERENCE_CREDIT_LIMIT_DUMMY"))
					MappedUT = "100000202";
				/*else if(datas[0].equals("SUBSCRIBER_BALANCE_DUMMY"))
					MappedUT = "10000102";*/
				else if(datas[0].equals("AVAILABLE_CREDIT_LIMIT_DUMMY"))
					MappedUT = "100000201";
				/*else if(datas[0].equals("MINPAY_FACTOR_ZERO_ZERO"))
					MappedUT = "10000104";
				else if(datas[0].equals("NOTIFICATION_70"))
					MappedUT = "10000105";
				else if(datas[0].equals("NOTIFICATION_80"))
					MappedUT = "10000106";
				else if(datas[0].equals("NOTIFICATION_90"))
					MappedUT = "10000107";*/
				else if(datas[0].equals("USAGE_VALUE_DUMMY"))
					MappedUT = "1000002";
				
				if(!MappedUT.isEmpty())
				{
					if(readInputFile.containsKey(datas[0]))
					{
						Long temp = readInputFile.get(datas[0]) + Long.parseLong(datas[1]); 
						readInputFile.put(MappedUT, temp);
					}
					else
					{
						readInputFile.put(MappedUT, Long.parseLong(datas[1]));
					}
				}
			}
		  }
		}
		catch(Exception ex)
		   {
			   ex.printStackTrace();
		   }
		return readInputFile;
	}
	
	
	private HashMap<String, Long> readCL99File(String filename) {
		// TODO Auto-generated method stub
		HashMap<String, Long> readInputFile = new HashMap<String, Long>();
		try {
			  
		  BufferedReader br = new BufferedReader(new FileReader(filename));
		  String line = "";
		  while ((line = br.readLine()) != null) {
			String datas[] = line.split(":",-1);
			if(!datas[0].isEmpty())
			{
				//Map to UT
				String MappedUT = "";
				if(datas[0].equals("REFERENCE_CREDIT_LIMIT99"))
					MappedUT = "100000202";
				else if(datas[0].equals("SUBSCRIBER_BALANCE99"))
					MappedUT = "10000102";
				else if(datas[0].equals("AVAILABLE_CREDIT_LIMIT99"))
					MappedUT = "100000201";
				else if(datas[0].equals("MINPAY_FACTOR99"))
					MappedUT = "10000104";
				else if(datas[0].equals("USAGE_VALUE99"))
					MappedUT = "1000002";
				
				if(!MappedUT.isEmpty())
				{
					if(readInputFile.containsKey(datas[0]))
					{
						Long temp = readInputFile.get(datas[0]) + Long.parseLong(datas[1]); 
						readInputFile.put(MappedUT, temp);
					}
					else
					{
						readInputFile.put(MappedUT, Long.parseLong(datas[1]));
					}
				}
			}
		  }
		}
		catch(Exception ex)
		   {
			   ex.printStackTrace();
		   }
		return readInputFile;
	}
	
	private HashMap<String, BigDecimal> readFileUT(String filename) {
		// TODO Auto-generated method stub
		HashMap<String, BigDecimal> readInputFile = new HashMap<String, BigDecimal>();
		try {
			  
		  BufferedReader br = new BufferedReader(new FileReader(filename));
		  String line = "";
		  while ((line = br.readLine()) != null) {
			String datas[] = line.split(":",-1);
			if(!datas[0].isEmpty())
			{
				readInputFile.put(datas[0], new BigDecimal(datas[1]));
			}
		  }
		}
		catch(Exception ex)
		   {
			   ex.printStackTrace();
		   }
		return readInputFile;
	}
	
	private HashMap<String, BigDecimal> readCLFileBalance(String filename) {
		// TODO Auto-generated method stub
		HashMap<String, BigDecimal> readInputFile = new HashMap<String, BigDecimal>();
		try {
			  
		  BufferedReader br = new BufferedReader(new FileReader(filename));
		  String line = "";
		  while ((line = br.readLine()) != null) {
			String datas[] = line.split(":",-1);
			if(!datas[0].isEmpty())
			{
				//Map to UT
				/*String MappedUT = "";
				if(datas[0].equals("REFERENCE_CREDIT_LIMIT"))
					MappedUT = "10000101";
				else if(datas[0].equals("SUBSCRIBER_BALANCE"))
					MappedUT = "10000102";
				else if(datas[0].equals("AVAILABLE_CREDIT_LIMIT"))
					MappedUT = "10000103";
				else if(datas[0].equals("MINPAY_FACTOR"))
					MappedUT = "10000104";
				else if(datas[0].equals("USAGE_VALUE"))
					MappedUT = "1000001";
				
				if(!MappedUT.isEmpty())
				{
					readInputFile.put(MappedUT, new BigDecimal(datas[1]));
				}*/
				readInputFile.put(datas[0], new BigDecimal(datas[1]));
			}
		  }
		}
		catch(Exception ex)
		   {
			   ex.printStackTrace();
		   }
		return readInputFile;
	}
	
	private HashMap<String, BigDecimal> readCLFileBalanceName(String filename) {
		// TODO Auto-generated method stub
		HashMap<String, BigDecimal> readInputFile = new HashMap<String, BigDecimal>();
		try {
			  
		  BufferedReader br = new BufferedReader(new FileReader(filename));
		  String line = "";
		  while ((line = br.readLine()) != null) {
			String datas[] = line.split(":",-1);
			if(!datas[0].isEmpty())
			{
				//Map to UT
				String MappedUT = "";
				if(datas[0].equals("REFERENCE_CREDIT_LIMIT"))
					MappedUT = "100000202";
				else if(datas[0].equals("SUBSCRIBER_BALANCE"))
					MappedUT = "10000102";
				else if(datas[0].equals("AVAILABLE_CREDIT_LIMIT"))
					MappedUT = "100000201";
				else if(datas[0].equals("MINPAY_FACTOR"))
					MappedUT = "10000104";
				else if(datas[0].equals("USAGE_VALUE"))
					MappedUT = "1000002";
				
				if(!MappedUT.isEmpty())
				{
					readInputFile.put(MappedUT, new BigDecimal(datas[1]));
				}
	
			}
		  }
		}
		catch(Exception ex)
		   {
			   ex.printStackTrace();
		   }
		return readInputFile;
	}
	
	private HashMap<String, BigDecimal> readCLBalance(String filename) {
		// TODO Auto-generated method stub
		HashMap<String, BigDecimal> readInputFile = new HashMap<String, BigDecimal>();
		try {
			  
		  BufferedReader br = new BufferedReader(new FileReader(filename));
		  String line = "";
		  while ((line = br.readLine()) != null) {
			String datas[] = line.split(":",-1);
			if(!datas[0].isEmpty())
			{
				//Map to UT
				readInputFile.put(datas[0], new BigDecimal(datas[1]));				
			}
		  }
		}
		catch(Exception ex)
		   {
			   ex.printStackTrace();
		   }
		return readInputFile;
	}
	
	private HashMap<String, BigDecimal> readCLFile99Balance(String filename) {
		// TODO Auto-generated method stub
		HashMap<String, BigDecimal> readInputFile = new HashMap<String, BigDecimal>();
		try {
			  
		  BufferedReader br = new BufferedReader(new FileReader(filename));
		  String line = "";
		  while ((line = br.readLine()) != null) {
			String datas[] = line.split(":",-1);
			if(!datas[0].isEmpty())
			{
				//Map to UT
				String MappedUT = "";
				if(datas[0].equals("REFERENCE_CREDIT_LIMIT99"))
					MappedUT = "100000202";
				else if(datas[0].equals("SUBSCRIBER_BALANCE99"))
					MappedUT = "10000102";
				else if(datas[0].equals("AVAILABLE_CREDIT_LIMIT99"))
					MappedUT = "100000201";
				else if(datas[0].equals("MINPAY_FACTOR99"))
					MappedUT = "10000104";
				else if(datas[0].equals("USAGE_VALUE99"))
					MappedUT = "1000002";
				
				if(!MappedUT.isEmpty())
				{
					readInputFile.put(MappedUT, new BigDecimal(datas[1]));
				}
			}
		  }
		}
		catch(Exception ex)
		   {
			   ex.printStackTrace();
		   }
		return readInputFile;
	}
	private List<String> readFile(String filePath,String fileName) {
		// TODO Auto-generated method stub
		List<String> readInputFile = new ArrayList();
		try {
			  
		  BufferedReader br = new BufferedReader(new FileReader(filePath + fileName));
		  String line = "";
		  while ((line = br.readLine()) != null) {
			if(!line.isEmpty())
			{
				readInputFile.add((line));
			}
		  }
		}
		catch(Exception ex)
	    {
		   ex.printStackTrace();
	    }
		return readInputFile;
	}
	
	private HashMap<String,List<String>> readUnitOfferFile(String filePath,String fileName) {
		// TODO Auto-generated method stub
		HashMap<String,List<String>> readInputFile = new HashMap<String, List<String>>();
		try {
			  
		  BufferedReader br = new BufferedReader(new FileReader(filePath + fileName));
		  String line = "";
		  while ((line = br.readLine()) != null) {
			if(!line.isEmpty())
			{
				if (readInputFile.containsKey(line.split(",")[1])) {
					List<String> temp = new ArrayList<String>();
					temp.addAll(readInputFile.get(line.split(",")[1]));
					temp.add(line.split(",")[0]);
					readInputFile.put(line.split(",")[1],temp);
				}
				else
				{
					List<String> temp = new ArrayList<String>();
					temp.add(line.split(",")[0]);
					readInputFile.put(line.split(",")[1],temp);
				}
			}
		  }
		}
		catch(Exception ex)
		   {
			   ex.printStackTrace();
		   }
		return readInputFile;
	}
	
	public String getUnitKey(String value, String fileName) {
		HashMap<String,List<String>> CompleteSrcList = readUnitOfferFile(this.SourceInputPath ,fileName);
		for(Entry<String,List<String>> entry : CompleteSrcList.entrySet())
		{
			List<String> temp = new ArrayList<String>();
			temp.addAll(entry.getValue());
			if(temp.contains(value))
				return entry.getKey();
		}
	   
		return "";
	}
	
	public static void main(String[] args) {
		String MigtoolPath = "C:\\Ericsson\\MyWorkingProject\\Charging_System\\dm_cs_2020_zain_bahrain\\dev\\src\\";
		ConsolidateReports cr = new ConsolidateReports(MigtoolPath);
		cr.execute();
	}	
}


