package com.ericsson.dm.cs.report;

import java.math.BigDecimal;

public class ReportStructure {
	SourceMetrics SrcMetric;
	TargetMetrics TrgtMetric;
	SourceBalanceMetrics SrcBalanceMetric;
	TargetBalanceMetrics TrgtBalanceMetric; 
	SnapShotBeforeMetrics preSnapMetic;
	SnapShotAfterMetrics postSnapMetic;
	
	public ReportStructure()
	{
		this.SrcMetric = new SourceMetrics();
		this.TrgtMetric = new TargetMetrics();
		this.SrcBalanceMetric = new SourceBalanceMetrics();
		this.TrgtBalanceMetric = new TargetBalanceMetrics();
		this.preSnapMetic = new SnapShotBeforeMetrics();
		this.postSnapMetic = new SnapShotAfterMetrics();
	}
	
	public class SourceMetrics{
		String SourceID;
		Long SourceCount;
		Long SourceInValidCount;
		Long SourceDummyCount;
		Long SourceValidCount;
		
		public SourceMetrics() {
			this.SourceID = "";
			this.SourceCount = 0L;
			this.SourceInValidCount = 0L;
			this.SourceValidCount = 0L;
		}
		
		public void populateSourceMetrics(String SourceID,Long SourceCount, Long SourceInValidCount, Long SourceValidCount) {
			// TODO Auto-generated constructor stub
			this.SourceID = SourceID;
			this.SourceCount = SourceCount;
			this.SourceInValidCount = SourceInValidCount;
			this.SourceValidCount = SourceValidCount;
		}
		
		public void populateSourceMetrics(String SourceID,Long SourceCount, Long SourceInValidCount, Long SourceValidCount, Long SourceDummyCount) {
			// TODO Auto-generated constructor stub
			this.SourceID = SourceID;
			this.SourceCount = SourceCount;
			this.SourceInValidCount = SourceInValidCount;
			this.SourceDummyCount = SourceDummyCount;
			this.SourceValidCount = SourceValidCount;
		}
	}
	
	public class SourceBalanceMetrics{
		String SourceID;
		BigDecimal SourceCount;
		BigDecimal SourceInValidCount;
		BigDecimal SourceValidCount;
		
		public SourceBalanceMetrics() {
			this.SourceID = "";
			this.SourceCount = new BigDecimal("0");
			this.SourceInValidCount = new BigDecimal("0");
			this.SourceValidCount = new BigDecimal("0");
		}
		
		public void populateSourceBalanceMetrics(String SourceID,BigDecimal SourceCount, BigDecimal SourceInValidCount, BigDecimal SourceValidCount) {
			// TODO Auto-generated constructor stub
			this.SourceID = SourceID;
			this.SourceCount = SourceCount;
			this.SourceInValidCount = SourceInValidCount;
			this.SourceValidCount = SourceValidCount;
		}
	}
	
	public class TargetMetrics{
		String TargetID;
		Long TargetCount;
		Long TargetOutofscope;
		Long TargetExpired;
		Long TargetOrphans;
		
		public TargetMetrics() {
			// TODO Auto-generated constructor stub
			this.TargetID = "";
			this.TargetCount = 0L;
			this.TargetOutofscope = 0L;
			this.TargetExpired = 0L;
			this.TargetOrphans = 0L;
		}
		
		public void populateTargetMetrics(String TargetID,Long TargetCount,Long TargetOutofscope,Long TargetExpired,Long TargetOrphans) {
			// TODO Auto-generated constructor stub
			this.TargetID = TargetID;
			this.TargetCount = TargetCount;
			this.TargetOutofscope = TargetOutofscope;
			this.TargetExpired = TargetExpired;
			this.TargetOrphans = TargetOrphans;
		}
		
		public void populateTargetMetrics(String TargetID,Long TargetCount,Long TargetOutofscope) {
			// TODO Auto-generated constructor stub
			this.TargetID = TargetID;
			this.TargetCount = TargetCount;
			this.TargetOutofscope = TargetOutofscope;
			this.TargetExpired = TargetExpired;
			this.TargetOrphans = TargetOrphans;
		}
	}
	
	public class TargetBalanceMetrics{
		String TargetID;
		BigDecimal TargetCount;
		BigDecimal TargetOutofscope;
		
		
		public TargetBalanceMetrics() {
			// TODO Auto-generated constructor stub
			this.TargetID = "";
			this.TargetCount = new BigDecimal("0");
			this.TargetOutofscope = new BigDecimal("0");
			
		}
		
		public void populateTargetBalanceMetrics(String TargetID,BigDecimal TargetCount,BigDecimal TargetOutofscope) {
			// TODO Auto-generated constructor stub
			this.TargetID = TargetID;
			this.TargetCount = TargetCount;
			this.TargetOutofscope = TargetOutofscope;
			
		}
	}
	
	public class SnapShotBeforeMetrics{
		String beforeSnapshotID;
		Long beforeSnapshotCount;		
		BigDecimal beforeSnapshotCountUsage;
		BigDecimal beforeSnapshotBalanceUsage;
		
		public SnapShotBeforeMetrics() {
			// TODO Auto-generated constructor stub
			this.beforeSnapshotCount = 0L;
			this.beforeSnapshotID = "";
		}
		
		public void populateSnapShotBeforeMetrics(String beforeSnapshotID, Long beforeSnapshotCount) {
			// TODO Auto-generated constructor stub
			this.beforeSnapshotCount = beforeSnapshotCount;
			this.beforeSnapshotID = beforeSnapshotID;
		}
		public void populateSnapShotBeforeMetricsBigDecimal(String beforeSnapshotID, BigDecimal beforeSnapshotCount) {
			// TODO Auto-generated constructor stub
			this.beforeSnapshotCountUsage = beforeSnapshotCount;
			this.beforeSnapshotID = beforeSnapshotID;
		}
		public void populateSnapShotBeforeMetricsBalance(String beforeSnapshotID, BigDecimal beforeSnapshotCount) {
			// TODO Auto-generated constructor stub
			this.beforeSnapshotBalanceUsage = beforeSnapshotCount;
			this.beforeSnapshotID = beforeSnapshotID;
		}
	}
	
	public class SnapShotAfterMetrics{
		String afterSnapshotID;
		Long afterSnapshotCount;
		BigDecimal afterSnapshotCountUsage;
		BigDecimal afterSnapshotBalanceUsage;
		
		public SnapShotAfterMetrics() {
			// TODO Auto-generated constructor stub
			this.afterSnapshotID = "";
			this.afterSnapshotCount = 0L;
		}
		
		public void populateSnapShotAfterMetrics(String afterSnapshotID, Long afterSnapshotCount) {
			// TODO Auto-generated constructor stub
			this.afterSnapshotCount = afterSnapshotCount;
			this.afterSnapshotID = afterSnapshotID;
		}
		public void populateSnapShotAfterMetricsBigDecimal(String afterSnapshotID, BigDecimal afterSnapshotCount) {
			// TODO Auto-generated constructor stub
			this.afterSnapshotCountUsage = afterSnapshotCount;
			this.afterSnapshotID = afterSnapshotID;
		}
		
		public void populateSnapShotAfterMetricsBalance(String afterSnapshotID, BigDecimal afterSnapshotCount) {
			// TODO Auto-generated constructor stub
			this.afterSnapshotBalanceUsage = afterSnapshotCount;
			this.afterSnapshotID = afterSnapshotID;
		}
	}
	
	
}
