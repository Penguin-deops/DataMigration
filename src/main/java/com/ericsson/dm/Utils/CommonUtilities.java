package com.ericsson.dm.Utils;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.xml.bind.DatatypeConverter;
import org.apache.log4j.Logger;

import com.ericsson.dm.inititialization.LoadSubscriberMapping;

public class CommonUtilities {
	final static Logger LOG = Logger.getLogger(CommonUtilities.class);
	private static final String DACOLUMNS = "ID_1,BALANCE_1,EXPIRY_DATE_1,ID_2,BALANCE_2,EXPIRY_DATE_2,ID_3,BALANCE_3,EXPIRY_DATE_3,ID_4,BALANCE_4,EXPIRY_DATE_4,ID_5,BALANCE_5,EXPIRY_DATE_5,ID_6,BALANCE_6,EXPIRY_DATE_6,ID_7,BALANCE_7,EXPIRY_DATE_7,ID_8,BALANCE_8,EXPIRY_DATE_8,ID_9,BALANCE_9,EXPIRY_DATE_9,ID_10,BALANCE_10,EXPIRY_DATE_10,START_DATE_1,START_DATE_2,START_DATE_3,START_DATE_4,START_DATE_5,START_DATE_6,START_DATE_7,START_DATE_8,PAM_SERVICE_ID_10,START_DATE_9,PAM_SERVICE_ID_1,PAM_SERVICE_ID_2,PAM_SERVICE_ID_3,PAM_SERVICE_ID_4,PAM_SERVICE_ID_5,PAM_SERVICE_ID_6,PAM_SERVICE_ID_7,PAM_SERVICE_ID_8,PAM_SERVICE_ID_9,START_DATE_10,PRODUCT_ID_10,PRODUCT_ID_1,PRODUCT_ID_2,PRODUCT_ID_3,PRODUCT_ID_4,PRODUCT_ID_5,PRODUCT_ID_6,PRODUCT_ID_7,PRODUCT_ID_8,PRODUCT_ID_9";
	//private static final String DACOLUMNS =  "ID_1,BALANCE_1,EXPIRY_DATE_1,ID_2,BALANCE_2,EXPIRY_DATE_2,ID_3,BALANCE_3,EXPIRY_DATE_3,ID_4,BALANCE_4,EXPIRY_DATE_4,ID_5,BALANCE_5,EXPIRY_DATE_5,ID_6,BALANCE_6,EXPIRY_DATE_6,ID_7,BALANCE_7,EXPIRY_DATE_7,ID_8,BALANCE_8,EXPIRY_DATE_8,ID_9,BALANCE_9,EXPIRY_DATE_9,ID_10,BALANCE_10,EXPIRY_DATE_10,START_DATE_1,START_DATE_2,START_DATE_3,START_DATE_4,START_DATE_5,START_DATE_6,START_DATE_7,START_DATE_8,PAM_SERVICE_ID_10,START_DATE_9,PRODUCT_ID_10,PRODUCT_ID_1,PRODUCT_ID_2,PRODUCT_ID_3,PRODUCT_ID_4,PRODUCT_ID_5,PRODUCT_ID_6,PRODUCT_ID_7,PRODUCT_ID_8,PRODUCT_ID_9,PAM_SERVICE_ID_1,PAM_SERVICE_ID_2,PAM_SERVICE_ID_3,PAM_SERVICE_ID_4,PAM_SERVICE_ID_5,PAM_SERVICE_ID_6,PAM_SERVICE_ID_7,PAM_SERVICE_ID_8,PAM_SERVICE_ID_9,START_DATE_10";
	private static final String DACOLUMNS2 = "ID_11,BALANCE_11,EXPIRY_DATE_11,ID_12,BALANCE_12,EXPIRY_DATE_12,ID_13,BALANCE_13,EXPIRY_DATE_13,ID_14,BALANCE_14,EXPIRY_DATE_14,ID_15,BALANCE_15,EXPIRY_DATE_15,ID_16,BALANCE_16,EXPIRY_DATE_16,ID_17,BALANCE_17,EXPIRY_DATE_17,ID_18,BALANCE_18,EXPIRY_DATE_18,ID_19,BALANCE_19,EXPIRY_DATE_19,ID_20,BALANCE_20,EXPIRY_DATE_20,START_DATE_20,PRODUCT_ID_11,PRODUCT_ID_12,PRODUCT_ID_13,PRODUCT_ID_14,START_DATE_11,PRODUCT_ID_20,PRODUCT_ID_15,START_DATE_12,PRODUCT_ID_16,START_DATE_13,PRODUCT_ID_17,START_DATE_14,PRODUCT_ID_18,START_DATE_15,PRODUCT_ID_19,START_DATE_16,START_DATE_17,START_DATE_18,START_DATE_19,PAM_SERVICE_ID_11,PAM_SERVICE_ID_12,PAM_SERVICE_ID_13,PAM_SERVICE_ID_14,PAM_SERVICE_ID_15,PAM_SERVICE_ID_16,PAM_SERVICE_ID_17,PAM_SERVICE_ID_18,PAM_SERVICE_ID_19,PAM_SERVICE_ID_20";
	private static final String DACOLUMNS3 = "ID_21,BALANCE_21,EXPIRY_DATE_21,ID_22,BALANCE_22,EXPIRY_DATE_22,ID_23,BALANCE_23,EXPIRY_DATE_23,ID_24,BALANCE_24,EXPIRY_DATE_24,ID_25,BALANCE_25,EXPIRY_DATE_25,ID_26,BALANCE_26,EXPIRY_DATE_26,ID_27,BALANCE_27,EXPIRY_DATE_27,ID_28,BALANCE_28,EXPIRY_DATE_28,ID_29,BALANCE_29,EXPIRY_DATE_29,ID_30,BALANCE_30,EXPIRY_DATE_30,START_DATE_30,PRODUCT_ID_21,PRODUCT_ID_22,PRODUCT_ID_23,PRODUCT_ID_24,START_DATE_21,PRODUCT_ID_30,PRODUCT_ID_25,START_DATE_22,PRODUCT_ID_26,START_DATE_23,PRODUCT_ID_27,START_DATE_24,PRODUCT_ID_28,START_DATE_25,PRODUCT_ID_29,START_DATE_26,START_DATE_27,START_DATE_28,START_DATE_29,PAM_SERVICE_ID_21,PAM_SERVICE_ID_22,PAM_SERVICE_ID_23,PAM_SERVICE_ID_24,PAM_SERVICE_ID_25,PAM_SERVICE_ID_26,PAM_SERVICE_ID_27,PAM_SERVICE_ID_28,PAM_SERVICE_ID_29,PAM_SERVICE_ID_30";
	private static final String DACOLUMNS4 = "ID_31,BALANCE_31,EXPIRY_DATE_31,ID_32,BALANCE_32,EXPIRY_DATE_32,ID_33,BALANCE_33,EXPIRY_DATE_33,ID_34,BALANCE_34,EXPIRY_DATE_34,ID_35,BALANCE_35,EXPIRY_DATE_35,ID_36,BALANCE_36,EXPIRY_DATE_36,ID_37,BALANCE_37,EXPIRY_DATE_37,ID_38,BALANCE_38,EXPIRY_DATE_38,ID_39,BALANCE_39,EXPIRY_DATE_39,ID_40,BALANCE_40,EXPIRY_DATE_40,START_DATE_40,PRODUCT_ID_31,PRODUCT_ID_32,PRODUCT_ID_33,PRODUCT_ID_34,START_DATE_31,PRODUCT_ID_40,PRODUCT_ID_35,START_DATE_32,PRODUCT_ID_36,START_DATE_33,PRODUCT_ID_37,START_DATE_34,PRODUCT_ID_38,START_DATE_35,PRODUCT_ID_39,START_DATE_36,START_DATE_37,START_DATE_38,START_DATE_39,PAM_SERVICE_ID_31,PAM_SERVICE_ID_32,PAM_SERVICE_ID_33,PAM_SERVICE_ID_34,PAM_SERVICE_ID_35,PAM_SERVICE_ID_36,PAM_SERVICE_ID_37,PAM_SERVICE_ID_38,PAM_SERVICE_ID_39,PAM_SERVICE_ID_40";
	private static final String DACOLUMNS5 = "ID_41,BALANCE_41,EXPIRY_DATE_41,ID_42,BALANCE_42,EXPIRY_DATE_42,ID_43,BALANCE_43,EXPIRY_DATE_43,ID_44,BALANCE_44,EXPIRY_DATE_44,ID_45,BALANCE_45,EXPIRY_DATE_45,ID_46,BALANCE_46,EXPIRY_DATE_46,ID_47,BALANCE_47,EXPIRY_DATE_47,ID_48,BALANCE_48,EXPIRY_DATE_48,ID_49,BALANCE_49,EXPIRY_DATE_49,ID_50,BALANCE_50,EXPIRY_DATE_50,START_DATE_50,PRODUCT_ID_41,PRODUCT_ID_42,PRODUCT_ID_43,PRODUCT_ID_44,START_DATE_41,PRODUCT_ID_50,PRODUCT_ID_45,START_DATE_42,PRODUCT_ID_46,START_DATE_43,PRODUCT_ID_47,START_DATE_44,PRODUCT_ID_48,START_DATE_45,PRODUCT_ID_49,START_DATE_46,START_DATE_47,START_DATE_48,START_DATE_49,PAM_SERVICE_ID_41,PAM_SERVICE_ID_42,PAM_SERVICE_ID_43,PAM_SERVICE_ID_44,PAM_SERVICE_ID_45,PAM_SERVICE_ID_46,PAM_SERVICE_ID_47,PAM_SERVICE_ID_48,PAM_SERVICE_ID_49,PAM_SERVICE_ID_50";
	private static final String DACOLUMNS6 = "ID_51,BALANCE_51,EXPIRY_DATE_51,ID_52,BALANCE_52,EXPIRY_DATE_52,ID_53,BALANCE_53,EXPIRY_DATE_53,ID_54,BALANCE_54,EXPIRY_DATE_54,ID_55,BALANCE_55,EXPIRY_DATE_55,ID_56,BALANCE_56,EXPIRY_DATE_56,ID_57,BALANCE_57,EXPIRY_DATE_57,ID_58,BALANCE_58,EXPIRY_DATE_58,ID_59,BALANCE_59,EXPIRY_DATE_59,ID_60,BALANCE_60,EXPIRY_DATE_60,START_DATE_60,PRODUCT_ID_51,PRODUCT_ID_52,PRODUCT_ID_53,PRODUCT_ID_54,START_DATE_51,PRODUCT_ID_60,PRODUCT_ID_55,START_DATE_52,PRODUCT_ID_56,START_DATE_53,PRODUCT_ID_57,START_DATE_54,PRODUCT_ID_58,START_DATE_55,PRODUCT_ID_59,START_DATE_56,START_DATE_57,START_DATE_58,START_DATE_59,PAM_SERVICE_ID_51,PAM_SERVICE_ID_52,PAM_SERVICE_ID_53,PAM_SERVICE_ID_54,PAM_SERVICE_ID_55,PAM_SERVICE_ID_56,PAM_SERVICE_ID_57,PAM_SERVICE_ID_58,PAM_SERVICE_ID_59,PAM_SERVICE_ID_60";
	private static final String DACOLUMNS7 = "ID_61,BALANCE_61,EXPIRY_DATE_61,ID_62,BALANCE_62,EXPIRY_DATE_62,ID_63,BALANCE_63,EXPIRY_DATE_63,ID_64,BALANCE_64,EXPIRY_DATE_64,ID_65,BALANCE_65,EXPIRY_DATE_65,ID_66,BALANCE_66,EXPIRY_DATE_66,ID_67,BALANCE_67,EXPIRY_DATE_67,ID_68,BALANCE_68,EXPIRY_DATE_68,ID_69,BALANCE_69,EXPIRY_DATE_69,ID_70,BALANCE_70,EXPIRY_DATE_70,START_DATE_70,PRODUCT_ID_61,PRODUCT_ID_62,PRODUCT_ID_63,PRODUCT_ID_64,START_DATE_61,PRODUCT_ID_70,PRODUCT_ID_65,START_DATE_62,PRODUCT_ID_66,START_DATE_63,PRODUCT_ID_67,START_DATE_64,PRODUCT_ID_68,START_DATE_65,PRODUCT_ID_69,START_DATE_66,START_DATE_67,START_DATE_68,START_DATE_69,PAM_SERVICE_ID_61,PAM_SERVICE_ID_62,PAM_SERVICE_ID_63,PAM_SERVICE_ID_64,PAM_SERVICE_ID_65,PAM_SERVICE_ID_66,PAM_SERVICE_ID_67,PAM_SERVICE_ID_68,PAM_SERVICE_ID_69,PAM_SERVICE_ID_70";
	private static final String DACOLUMNS8 = "ID_71,BALANCE_71,EXPIRY_DATE_71,ID_72,BALANCE_72,EXPIRY_DATE_72,ID_73,BALANCE_73,EXPIRY_DATE_73,ID_74,BALANCE_74,EXPIRY_DATE_74,ID_75,BALANCE_75,EXPIRY_DATE_75,ID_76,BALANCE_76,EXPIRY_DATE_76,ID_77,BALANCE_77,EXPIRY_DATE_77,ID_78,BALANCE_78,EXPIRY_DATE_78,ID_79,BALANCE_79,EXPIRY_DATE_79,ID_80,BALANCE_80,EXPIRY_DATE_80,START_DATE_80,PRODUCT_ID_71,PRODUCT_ID_72,PRODUCT_ID_73,PRODUCT_ID_74,START_DATE_71,PRODUCT_ID_80,PRODUCT_ID_75,START_DATE_72,PRODUCT_ID_76,START_DATE_73,PRODUCT_ID_77,START_DATE_74,PRODUCT_ID_78,START_DATE_75,PRODUCT_ID_79,START_DATE_76,START_DATE_77,START_DATE_78,START_DATE_79,PAM_SERVICE_ID_71,PAM_SERVICE_ID_72,PAM_SERVICE_ID_73,PAM_SERVICE_ID_74,PAM_SERVICE_ID_75,PAM_SERVICE_ID_76,PAM_SERVICE_ID_77,PAM_SERVICE_ID_78,PAM_SERVICE_ID_79,PAM_SERVICE_ID_80";
	private static final String DACOLUMNS9 = "ID_81,BALANCE_81,EXPIRY_DATE_81,ID_82,BALANCE_82,EXPIRY_DATE_82,ID_83,BALANCE_83,EXPIRY_DATE_83,ID_84,BALANCE_84,EXPIRY_DATE_84,ID_85,BALANCE_85,EXPIRY_DATE_85,ID_86,BALANCE_86,EXPIRY_DATE_86,ID_87,BALANCE_87,EXPIRY_DATE_87,ID_88,BALANCE_88,EXPIRY_DATE_88,ID_89,BALANCE_89,EXPIRY_DATE_89,ID_90,BALANCE_90,EXPIRY_DATE_90,START_DATE_90,PRODUCT_ID_81,PRODUCT_ID_82,PRODUCT_ID_83,PRODUCT_ID_84,START_DATE_81,PRODUCT_ID_90,PRODUCT_ID_85,START_DATE_82,PRODUCT_ID_86,START_DATE_83,PRODUCT_ID_87,START_DATE_84,PRODUCT_ID_88,START_DATE_85,PRODUCT_ID_89,START_DATE_86,START_DATE_87,START_DATE_88,START_DATE_89,PAM_SERVICE_ID_81,PAM_SERVICE_ID_82,PAM_SERVICE_ID_83,PAM_SERVICE_ID_84,PAM_SERVICE_ID_85,PAM_SERVICE_ID_86,PAM_SERVICE_ID_87,PAM_SERVICE_ID_88,PAM_SERVICE_ID_89,PAM_SERVICE_ID_90";
			
	private static final String ACMCOLUMNS = "ACCOUNT_ID,SEQUENCE_ID,ID_1,VALUE_1,CLEARING_DATE_1,ID_2,VALUE_2,CLEARING_DATE_2,ID_3,VALUE_3,CLEARING_DATE_3,ID_4,VALUE_4,CLEARING_DATE_4,ID_5,VALUE_5,CLEARING_DATE_5,ID_6,VALUE_6,CLEARING_DATE_6,ID_7,VALUE_7,CLEARING_DATE_7,ID_8,VALUE_8,CLEARING_DATE_8,ID_9,VALUE_9,CLEARING_DATE_9,ID_10,VALUE_10,CLEARING_DATE_10";
	private static final String ACMCOLUMNS2 = "ACCOUNT_ID,SEQUENCE_ID,ID_11,VALUE_11,CLEARING_DATE_11,ID_12,VALUE_12,CLEARING_DATE_12,ID_13,VALUE_13,CLEARING_DATE_13,ID_14,VALUE_14,CLEARING_DATE_14,ID_15,VALUE_15,CLEARING_DATE_15,ID_16,VALUE_16,CLEARING_DATE_16,ID_17,VALUE_17,CLEARING_DATE_17,ID_18,VALUE_18,CLEARING_DATE_18,ID_19,VALUE_19,CLEARING_DATE_19,ID_20,VALUE_20,CLEARING_DATE_20";
	private static final String ACMCOLUMNS3 = "ACCOUNT_ID,SEQUENCE_ID,ID_21,VALUE_21,CLEARING_DATE_21,ID_22,VALUE_22,CLEARING_DATE_22,ID_23,VALUE_23,CLEARING_DATE_23,ID_24,VALUE_24,CLEARING_DATE_24,ID_25,VALUE_25,CLEARING_DATE_25,ID_26,VALUE_26,CLEARING_DATE_26,ID_27,VALUE_27,CLEARING_DATE_27,ID_28,VALUE_28,CLEARING_DATE_28,ID_29,VALUE_29,CLEARING_DATE_29,ID_30,VALUE_30,CLEARING_DATE_30";
	
	
	
	public static long convertDateToEpoch(String date) {
		try {
			if (date != null && !date.equals("null") && date.length() > 0) {
				if (date.startsWith("0")) {
					String temp[] = date.split("-");
					if (temp.length > 1) {
						int tempInt = 2000 + Integer.parseInt(temp[0]);
						date = tempInt + "-" + temp[1] + "-" + temp[2];
					} else {
						date = "2000-01-01 00:00:00";
					}
				}
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				java.util.Date date1 = sdf.parse(date);
				java.util.Date date2 = sdf.parse("1970-01-01 00:00:00");

				String zone = Constants.DEFAULT_TIME_ZONE;
				zone = "".equals(zone) ? "UTC" : zone;

				// Here getDifferenceMillis will return negative or possitive
				// value depending on timezone.
				
				long days = (((date1.getTime() - getDifferenceMillis(date1, zone)) - date2.getTime())
						/ (1000 * 60 * 60 * 24)) ;
			
				return days;
			} else {
				return 0;
			}
		} catch (Exception e) {

			e.printStackTrace();
			LOG.error(e);
		}
		return 0;

	}
	
	public static long convertDateToEpochSeconds(String date) {
		try {
			if (date != null && !date.equals("null") && date.length() > 0) {
				if (date.startsWith("0")) {
					String temp[] = date.split("-");
					if (temp.length > 1) {
						int tempInt = 2000 + Integer.parseInt(temp[0]);
						date = tempInt + "-" + temp[1] + "-" + temp[2];
					} else {
						date = "2000-01-01 00:00:00";
					}
				}
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				java.util.Date date1 = sdf.parse(date);
				java.util.Date date2 = sdf.parse("1970-01-01 00:00:00");

				String zone = Constants.DEFAULT_TIME_ZONE;
				zone = "".equals(zone) ? "UTC" : zone;

				// Here getDifferenceMillis will return negative or possitive
				// value depending on timezone.
				
				long days = (((date1.getTime() - (getDifferenceMillis(date1, zone))) - date2.getTime())
						/ (1000)) -3600;
			
				return days;
			} else {
				return 0;
			}
		} catch (Exception e) {

			e.printStackTrace();
			LOG.error(e);
		}
		return 0;

	}

	public static String convertEpochDateToDate(int days) {
		String result = null;
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			java.util.Date date2 = sdf.parse("1970-01-01 00:00:00");

			Calendar c = Calendar.getInstance();
			c.setTime(date2);

			c.add(Calendar.DATE, days);
			result = sdf.format(c.getTime());

		} catch (Exception e) {
			LOG.error(e);
		}
		return result;
	}
	
	public static String getEndOfDay(String date) {
		String result = "";
		if(date.length() != 0)
		{
			LocalDate todaydate = LocalDate.parse(date.substring(0,10));		
			result = todaydate + " 00:00:00";
		}
	    return result;
	}

	public static Integer[] convertDateToTimerOfferDate(String date) {
		Integer[] data = new Integer[2];
		try {
			// String dateString = date;
			if (date != null && !date.equals("null") && date.length() > 0) {
				if (date.startsWith("0")) {
					String temp[] = date.split("-");
					if (temp.length > 1) {
						int tempInt = 2000 + Integer.parseInt(temp[0]);
						date = tempInt + "-" + temp[1] + "-" + temp[2];
					} else {
						date = "2000-01-01 00:00:00";
					}
				}
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				java.util.Date date1 = sdf.parse(date);
				java.util.Date date2 = sdf.parse("1970-01-01 00:00:00");
				//java.util.Date date2 = sdf.parse("1969-12-31 23:00:00");

				String zone = Constants.DEFAULT_TIME_ZONE;
				zone = "".equals(zone) ? "GMT" : zone;
				
				//System.out.println(getDifferenceMillis(date1, zone));

				// Here getDifferenceMillis will return negative or possitive
				// value
				// depending on timezone.
				long secs = (((date1.getTime() - getDifferenceMillis(date1, zone)) - date2.getTime()) / (1000)) - 3600;
				//long secs = ((date1.getTime() - 10800) - date2.getTime()) / (1000);
				data = convertDate(secs);
				sdf = null;
				date1 = null;
				date2 = null;
				zone = null;
				return data;
			}
		} catch (Exception e) {

			e.printStackTrace();
			LOG.error(e);
			Integer[] res = { 0, 0 };
			return res;
		} finally {

		}

		return data;
	}
	
	public static Integer[] convertDateToTimerOfferDate_New(String date) {
		Integer[] data = new Integer[2];
		try {
			// String dateString = date;
			if (date != null && !date.equals("null") && date.length() > 0) {
				if (date.startsWith("0")) {
					String temp[] = date.split("-");
					if (temp.length > 1) {
						int tempInt = 2000 + Integer.parseInt(temp[0]);
						date = tempInt + "-" + temp[1] + "-" + temp[2];
					} else {
						date = "2000-01-01 00:00:00";
					}
				}
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				java.util.Date date1 = sdf.parse(date);
				java.util.Date date2 = sdf.parse("1970-01-01 00:00:00");
				//java.util.Date date2 = sdf.parse("1969-12-31 23:00:00");
				System.out.println("TimeZone: " + Constants.DEFAULT_TIME_ZONE);
				String zone = Constants.DEFAULT_TIME_ZONE;
				zone = "".equals(zone) ? "GMT" : zone;
				
				System.out.println(getDifferenceMillis(date1, zone));
				
				// Here getDifferenceMillis will return negative or possitive
				// value
				// depending on timezone.
				long secs = (((date1.getTime() - getDifferenceMillis(date1, zone)) - date2.getTime()) / (1000))-3600;
				System.out.println("secs: " + secs);
				//long secs = ((date1.getTime() - 10800) - date2.getTime()) / (1000);
				data = convertDate(secs);
				sdf = null;
				date1 = null;
				date2 = null;
				zone = null;
				return data;
			}
		} catch (Exception e) {

			e.printStackTrace();
			LOG.error(e);
			Integer[] res = { 0, 0 };
			return res;
		} finally {

		}

		return data;
	}

	public static long getDifferenceMillis(Date date, String zone) {
		long diffmilis = 0;
		// get offset milis
		TimeZone timeZone = TimeZone.getTimeZone(zone);
		int rawOffsetMillis = timeZone.getRawOffset();// It might be -ve or +ve
														// value
		// get saving duration
		boolean isDT = TimeZone.getTimeZone(zone).inDaylightTime(date);
		//System.out.println("------------->>isDT :"+isDT );
		int dstSavingMillis = 0;
		if (isDT) {
			dstSavingMillis = timeZone.getDSTSavings(); // for no. of milli
														// seconds
		}
				// time zone diff - Daylight saving
		diffmilis = rawOffsetMillis + dstSavingMillis;
		return diffmilis;
	}

	private static Integer[] convertDate(long date) {
		int constt = 32767;
		String bin = Long.toBinaryString(date);
		String value1 = bin.substring(0, bin.length() - 16);
		String value2 = bin.substring(bin.length() - 16);
		Integer[] output = new Integer[2];
		output[0] = Integer.parseInt(value1, 2);
		output[1] = value2.startsWith("0") ? Integer.parseInt(value2.substring(1), 2)
				: -1 * (constt - Integer.parseInt(value2.substring(1), 2) + 1);
		return output;
	}
	
	private static String returnActualDateFromTimerDate(long date , long secs) throws ParseException {
		int constt = 32767;
		String bin1 = Long.toBinaryString(date);
		String bin2 = "";
		if(secs<0){
			secs=secs*-1;
			String str = Integer.toBinaryString((int)secs);//.substring(1);
			secs = Long.parseLong(str, 2);
			secs = constt+ secs;
			bin2 = Long.toBinaryString(secs);
			long originalNum =Long.parseLong(bin2.substring(1), 2);
			bin2 =Integer.toBinaryString(~(int)originalNum);
			bin2 =bin2.substring(bin2.length()-16);
		}
		else{
			bin2 = Long.toBinaryString(secs);
			long originalNum =Long.parseLong(bin2.substring(0), 2);
			bin2 =Integer.toBinaryString((int)originalNum);
			//bin2 =bin2.substring(bin2.length()-16);
			int indx =bin2.length();
			while(indx<16){
				bin2="0"+bin2;
				indx++;
			}
		}
		
		bin1 = bin1+bin2;
		long output=Long.parseLong(bin1, 2);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		java.util.Date date2 = sdf.parse("1970-01-01 00:00:00");
		//millisecs = date2.getTime();
		Calendar csl = Calendar.getInstance();
		csl.setTime(date2);
		csl.add(Calendar.SECOND, (int) output);
		//System.out.println(sdf.format(csl.getTime()));
		return sdf.format(csl.getTime());
		
	}

	public static String toHexadecimal(String text) throws UnsupportedEncodingException {
		byte[] myBytes = text.getBytes("UTF-8");

		return DatatypeConverter.printHexBinary(myBytes);
	}
	
	public static String toOfferAttrType3(String text) throws UnsupportedEncodingException
	{
		String hexValue = Long.toHexString(Long.parseLong(text));
		String PaddedValue = org.apache.commons.lang.StringUtils.leftPad(hexValue, 16, "0");
		
		String result = "";
		if(text.startsWith("-"))
			result = "01" + PaddedValue + "0000000000000001";
		else
			result = "01" + PaddedValue + "0000000000000001";
		
		return result;
	}

	public static String toHexadecimal(int text) throws UnsupportedEncodingException {

		return String.format("%x", new BigInteger(String.valueOf(text)));
	}

	public static String getCurrentPamPeriod(String paramString, int day) {

		SimpleDateFormat sdfDaily = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat sdfMonthly = new SimpleDateFormat("yyyy_MMM");
		SimpleDateFormat sdfday = new SimpleDateFormat("dd");

		// SimpleDateFormat sdfWeekly = new SimpleDateFormat("yyyy");
		Date currDate = new Date();

		Calendar cal = Calendar.getInstance();
		cal.setTime(currDate);
		String str = sdfday.format(currDate);
		if (Integer.parseInt(str) > day) {
			cal.add(Calendar.MONTH, +1);
			currDate = cal.getTime();
		}
		// String week = String.valueOf(cal.get(Calendar.WEEK_OF_YEAR));
		// week = week.length()==1?"0"+week:week;

		String returnValue = null;
		switch (paramString) {
		case "Daily":
			returnValue = "Daily" + sdfDaily.format(currDate);
			break;
		case "Monthly":
			returnValue = sdfMonthly.format(currDate);
			break;
		// case "Weekly" :
		// returnValue=sdfWeekly.format(currDate)+"_W"+week;break;

		}
		sdfDaily = null;
		sdfMonthly = null;
		// sdfWeekly=null;
		currDate = null;// week=null;paramString=null;
		return returnValue;
	}

	public static boolean isDateExpirted(String inputValue,String refValue)
	{
		boolean bExpired = false;
		String DateReference = refValue;
		
		SimpleDateFormat sdfDaily = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		Date refDate = new Date();
		Date inputDate = new Date();
		
		try {
			refDate = sdfDaily.parse(DateReference);
			inputDate = sdfDaily.parse(inputValue);
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return inputDate.after(refDate);
	}
	
	public static long getCurrentPamPeriodInDays(String paramString) {
		long returnValue = 0;
		SimpleDateFormat sdfDaily = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date currDate = new Date();
		Calendar cal = Calendar.getInstance();
		cal.setTime(currDate);
		switch (paramString) {
		case "Daily":
			
			
			cal.add(Calendar.DATE, -1);
			currDate = cal.getTime();
			returnValue = convertDateToEpoch(sdfDaily.format(currDate));
			break;
		case "Monthly":
			cal.set(Calendar.DAY_OF_MONTH, cal.getActualMinimum(Calendar.DAY_OF_MONTH));
			currDate = cal.getTime();
			
			returnValue = convertDateToEpoch(sdfDaily.format(currDate));
			break;
		}
		return returnValue;
	}
	
	public static Collection<? extends String> GenerateDABasedOnSDP(String MSISDN, Map<String, String> daMap) {
		//Fill the DA as per SDP server
		List<String> daList = new ArrayList<>();
		if(daMap.size() >= 1)
		{
			StringBuffer sb = new StringBuffer();
			StringBuffer sb2 = new StringBuffer();
			StringBuffer sb3 = new StringBuffer();
			StringBuffer sb4 = new StringBuffer();
			StringBuffer sb5 = new StringBuffer();
			StringBuffer sb6 = new StringBuffer();
			StringBuffer sb7 = new StringBuffer();
			StringBuffer sb8 = new StringBuffer();
			StringBuffer sb9 = new StringBuffer();
			String[] splittedColumns = DACOLUMNS.split(",");
			int columnCount = 1;						
			if (daMap.containsKey("ID_1")) {
				sb.append(MSISDN).append(",");
				sb.append(1).append(",");

				for (String column : splittedColumns) {
					if (columnCount <= splittedColumns.length - 1) {
						if (daMap.containsKey(column)) {
							sb.append(daMap.get(column)).append(",");
						} else {
							if (column.startsWith("ID") || column.startsWith("BALANCE")) {
								sb.append("0,");
							} else {
								if (column.startsWith("PAM_SERVICE_ID")) {
								sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_NULL")+","); 
								}
								else {
									if (column.startsWith("EXPIRY_DATE") || column.startsWith("START_DATE")) {
									sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_NULL")+",");
									}
									else
									{
									if (column.startsWith("PRODUCT_ID")) {
										//sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_0")+ ",");
										sb.append(LoadSubscriberMapping.CommonConfigMapping.get("Default_NULL")+ ",");
										}	
									}
								} 
							}
						}
					} else {
						if (daMap.containsKey(column)) {
							sb.append(daMap.get(column));
						} else {

							sb.append("NULL");
						}
					}
					if(columnCount == 60)
						break;
					columnCount++;								
				}														
			}
			if (daMap.containsKey("ID_1")) {
				daList.add(sb.toString());
			}
			sb = null;
			
			//it means we have more DA to be added so populate Sequence ID=2
			if(daMap.size() > 60)
			{
				String[] splittedColumns2 = DACOLUMNS2.split(",");
				int columnCount2 = 1;						
				if (daMap.containsKey("ID_11")) {
					sb2.append(MSISDN).append(",");
					sb2.append(2).append(",");

					for (String column : splittedColumns2) {
						if (columnCount2 <= splittedColumns2.length - 1) {
							if (daMap.containsKey(column)) {
								sb2.append(daMap.get(column)).append(",");
							} else {
								if (column.startsWith("ID") || column.startsWith("BALANCE")) {
									sb2.append("0,");
								} else {
									if (column.startsWith("PAM_SERVICE_ID")) {
										sb2.append(LoadSubscriberMapping.CommonConfigMapping.get("default_DA_PAM_SERVICE_ID")+","); //EPRIPRA
									}
									else {
										if (column.startsWith("EXPIRY_DATE") || column.startsWith("START_DATE")) {
											sb2.append("NULL,");
										}
										else
										{
										if (column.startsWith("PRODUCT_ID")) {
											sb2.append("0,");
											}	
										}
									} 
								}
							}
						} else {
							if (daMap.containsKey(column)) {
								sb2.append(daMap.get(column));
							} else {

								sb2.append("NULL");
							}
						}
						columnCount2++;										
					}			
				}
			}
			if (daMap.containsKey("ID_11")) {
				daList.add(sb2.toString());
			}
			
			////it means we have more DA to be added so populate Sequence ID=3
			if(daMap.size() > 90)
			{
				String[] splittedColumns3 = DACOLUMNS3.split(",");
				int columnCount3 = 1;						
				if (daMap.containsKey("ID_21")) {
					sb3.append(MSISDN).append(",");
					sb3.append(3).append(",");

					for (String column : splittedColumns3) {
						if (columnCount3 <= splittedColumns3.length - 1) {
							if (daMap.containsKey(column)) {
								sb3.append(daMap.get(column)).append(",");
							} else {
								if (column.startsWith("ID") || column.startsWith("BALANCE")) {
									sb3.append("0,");
								} else {
									if (column.startsWith("PAM_SERVICE_ID")) {
										sb3.append(LoadSubscriberMapping.CommonConfigMapping.get("default_DA_PAM_SERVICE_ID")+","); //EPRIPRA
									}
									else {
										if (column.startsWith("EXPIRY_DATE") || column.startsWith("START_DATE")) {
											sb3.append("NULL,");
										}
										else
										{
										if (column.startsWith("PRODUCT_ID")) {
											sb3.append("0,");
											}	
										}
									} 
								}
							}
						} else {
							if (daMap.containsKey(column)) {
								sb3.append(daMap.get(column));
							} else {

								sb3.append("NULL");
							}
						}
						columnCount3++;										
					}			
				}
			}
			if (daMap.containsKey("ID_21")) {
				daList.add(sb3.toString());
			}
			
			////it means we have more DA to be added so populate Sequence ID=4
			if(daMap.size() > 120)
			{
				String[] splittedColumns4 = DACOLUMNS4.split(",");
				int columnCount4 = 1;						
				if (daMap.containsKey("ID_31")) {
					sb4.append(MSISDN).append(",");
					sb4.append(4).append(",");

					for (String column : splittedColumns4) {
						if (columnCount4 <= splittedColumns4.length - 1) {
							if (daMap.containsKey(column)) {
								sb4.append(daMap.get(column)).append(",");
							} else {
								if (column.startsWith("ID") || column.startsWith("BALANCE")) {
									sb4.append("0,");
								} else {
									if (column.startsWith("PAM_SERVICE_ID")) {
										sb4.append(LoadSubscriberMapping.CommonConfigMapping.get("default_DA_PAM_SERVICE_ID")+","); //EPRIPRA
									}
									else {
										if (column.startsWith("EXPIRY_DATE") || column.startsWith("START_DATE")) {
											sb4.append("NULL,");
										}
										else
										{
										if (column.startsWith("PRODUCT_ID")) {
											sb4.append("0,");
											}	
										}
									} 
								}
							}
						} else {
							if (daMap.containsKey(column)) {
								sb4.append(daMap.get(column));
							} else {

								sb4.append("NULL");
							}
						}
						columnCount4++;										
					}			
				}
			}
			if (daMap.containsKey("ID_31")) {
				daList.add(sb4.toString());
			}
			////it means we have more DA to be added so populate Sequence ID=5
			if(daMap.size() > 150)
			{
				String[] splittedColumns5 = DACOLUMNS5.split(",");
				int columnCount5 = 1;						
				if (daMap.containsKey("ID_41")) {
					sb5.append(MSISDN).append(",");
					sb5.append(5).append(",");

					for (String column : splittedColumns5) {
						if (columnCount5 <= splittedColumns5.length - 1) {
							if (daMap.containsKey(column)) {
								sb5.append(daMap.get(column)).append(",");
							} else {
								if (column.startsWith("ID") || column.startsWith("BALANCE")) {
									sb5.append("0,");
								} else {
									if (column.startsWith("PAM_SERVICE_ID")) {
										sb5.append(LoadSubscriberMapping.CommonConfigMapping.get("default_DA_PAM_SERVICE_ID")+","); //EPRIPRA
									}
									else {
										if (column.startsWith("EXPIRY_DATE") || column.startsWith("START_DATE")) {
											sb5.append("NULL,");
										}
										else
										{
										if (column.startsWith("PRODUCT_ID")) {
											sb5.append("0,");
											}	
										}
									} 
								}
							}
						} else {
							if (daMap.containsKey(column)) {
								sb5.append(daMap.get(column));
							} else {

								sb5.append("NULL");
							}
						}
						columnCount5++;										
					}			
				}
			}
			if (daMap.containsKey("ID_41")) {
				daList.add(sb5.toString());
			}
			
			////it means we have more DA to be added so populate Sequence ID=6
			if(daMap.size() > 180)
			{
				String[] splittedColumns6 = DACOLUMNS6.split(",");
				int columnCount6 = 1;						
				if (daMap.containsKey("ID_51")) {
					sb6.append(MSISDN).append(",");
					sb6.append(6).append(",");

					for (String column : splittedColumns6) {
						if (columnCount6 <= splittedColumns6.length - 1) {
							if (daMap.containsKey(column)) {
								sb6.append(daMap.get(column)).append(",");
							} else {
								if (column.startsWith("ID") || column.startsWith("BALANCE")) {
									sb6.append("0,");
								} else {
									if (column.startsWith("PAM_SERVICE_ID")) {
										sb6.append(LoadSubscriberMapping.CommonConfigMapping.get("default_DA_PAM_SERVICE_ID")+","); //EPRIPRA
									}
									else {
										if (column.startsWith("EXPIRY_DATE") || column.startsWith("START_DATE")) {
											sb6.append("NULL,");
										}
										else
										{
										if (column.startsWith("PRODUCT_ID")) {
											sb6.append("0,");
											}	
										}
									} 
								}
							}
						} else {
							if (daMap.containsKey(column)) {
								sb6.append(daMap.get(column));
							} else {

								sb6.append("NULL");
							}
						}
						columnCount6++;										
					}			
				}
			}
			if (daMap.containsKey("ID_51")) {
				daList.add(sb6.toString());
			}
			
			////it means we have more DA to be added so populate Sequence ID=7
			if(daMap.size() > 210)
			{
				String[] splittedColumns7 = DACOLUMNS7.split(",");
				int columnCount7 = 1;						
				if (daMap.containsKey("ID_61")) {
					sb7.append(MSISDN).append(",");
					sb7.append(7).append(",");

					for (String column : splittedColumns7) {
						if (columnCount7 <= splittedColumns7.length - 1) {
							if (daMap.containsKey(column)) {
								sb7.append(daMap.get(column)).append(",");
							} else {
								if (column.startsWith("ID") || column.startsWith("BALANCE")) {
									sb7.append("0,");
								} else {
									if (column.startsWith("PAM_SERVICE_ID")) {
										sb7.append(LoadSubscriberMapping.CommonConfigMapping.get("default_DA_PAM_SERVICE_ID")+","); //EPRIPRA
									}
									else {
										if (column.startsWith("EXPIRY_DATE") || column.startsWith("START_DATE")) {
											sb7.append("NULL,");
										}
										else
										{
										if (column.startsWith("PRODUCT_ID")) {
											sb7.append("0,");
											}	
										}
									} 
								}
							}
						} else {
							if (daMap.containsKey(column)) {
								sb7.append(daMap.get(column));
							} else {

								sb7.append("NULL");
							}
						}
						columnCount7++;										
					}			
				}
			}
			if (daMap.containsKey("ID_61")) {
				daList.add(sb7.toString());
			}
			
			////it means we have more DA to be added so populate Sequence ID=8
			if(daMap.size() > 240)
			{
				String[] splittedColumns8 = DACOLUMNS8.split(",");
				int columnCount8 = 1;						
				if (daMap.containsKey("ID_71")) {
					sb8.append(MSISDN).append(",");
					sb8.append(8).append(",");

					for (String column : splittedColumns8) {
						if (columnCount8 <= splittedColumns8.length - 1) {
							if (daMap.containsKey(column)) {
								sb8.append(daMap.get(column)).append(",");
							} else {
								if (column.startsWith("ID") || column.startsWith("BALANCE")) {
									sb8.append("0,");
								} else {
									if (column.startsWith("PAM_SERVICE_ID")) {
										sb8.append(LoadSubscriberMapping.CommonConfigMapping.get("default_DA_PAM_SERVICE_ID")+","); //EPRIPRA
									}
									else {
										if (column.startsWith("EXPIRY_DATE") || column.startsWith("START_DATE")) {
											sb8.append("NULL,");
										}
										else
										{
										if (column.startsWith("PRODUCT_ID")) {
											sb8.append("0,");
											}	
										}
									} 
								}
							}
						} else {
							if (daMap.containsKey(column)) {
								sb8.append(daMap.get(column));
							} else {

								sb8.append("NULL");
							}
						}
						columnCount8++;										
					}			
				}
			}
			if (daMap.containsKey("ID_71")) {
				daList.add(sb8.toString());
			}
			
			////it means we have more DA to be added so populate Sequence ID=9
			if(daMap.size() > 290)
			{
				String[] splittedColumns9 = DACOLUMNS9.split(",");
				int columnCount9 = 1;						
				if (daMap.containsKey("ID_81")) {
					sb9.append(MSISDN).append(",");
					sb9.append(9).append(",");

					for (String column : splittedColumns9) {
						if (columnCount9 <= splittedColumns9.length - 1) {
							if (daMap.containsKey(column)) {
								sb9.append(daMap.get(column)).append(",");
							} else {
								if (column.startsWith("ID") || column.startsWith("BALANCE")) {
									sb9.append("0,");
								} else {
									if (column.startsWith("PAM_SERVICE_ID")) {
										sb9.append(LoadSubscriberMapping.CommonConfigMapping.get("default_DA_PAM_SERVICE_ID")+","); //EPRIPRA
									}
									else {
										if (column.startsWith("EXPIRY_DATE") || column.startsWith("START_DATE")) {
											sb9.append("NULL,");
										}
										else
										{
										if (column.startsWith("PRODUCT_ID")) {
											sb9.append("0,");
											}	
										}
									} 
								}
							}
						} else {
							if (daMap.containsKey(column)) {
								sb9.append(daMap.get(column));
							} else {

								sb9.append("NULL");
							}
						}
						columnCount9++;										
					}			
				}
			}
			if (daMap.containsKey("ID_81")) {
				daList.add(sb9.toString());
			}
			
		}
		return daList;
	}

	public static Collection<? extends String> GenerateAccumulatorBasedOnSDP(String MSISDN,
			Map<String, String> accMap) {
		// TODO Auto-generated method stub
		List<String> uaList = new ArrayList<>();
		
		if(accMap.size() >= 1)
		{
			StringBuffer sb = new StringBuffer();
			StringBuffer sb2 = new StringBuffer();
			StringBuffer sb3 = new StringBuffer();
			String[] splittedColumns = ACMCOLUMNS.split(",");
			int columnCount = 1;
			if (accMap.containsKey("ID_1")) {
				sb.append(MSISDN).append(",");
				sb.append(1).append(",");

				for (String column : splittedColumns) {
					if (columnCount <= splittedColumns.length-1) {
						if (accMap.containsKey(column)) {
							sb.append(accMap.get(column)).append(",");
						} else {
							if (column.startsWith("ID") || column.startsWith("VALUE")) {
								sb.append("0,");
							} else {
								if (column.startsWith("CLEARING_DATE")) {
								sb.append(LoadSubscriberMapping.CommonConfigMapping.get("default_last_reset_Date") +",");
								}										
							}
						}
					} else {
						if (accMap.containsKey(column)) {
							sb.append(accMap.get(column));
						} else {

							sb.append(LoadSubscriberMapping.CommonConfigMapping.get("default_last_reset_Date"));
						}
					}
					columnCount++;
				}
			}
			if (accMap.containsKey("ID_1")) {
				uaList.add(sb.toString());
			}
			sb = null;
			//it means we have more DA to be added so populate Sequence ID=2
			if(accMap.size() > 30)
			{
				String[] splittedColumns2 = ACMCOLUMNS2.split(",");
				int columnCount2 = 1;
				if (accMap.containsKey("ID_1")) {
					sb2.append(MSISDN).append(",");
					sb2.append(2).append(",");

					for (String column : splittedColumns2) {
						if (columnCount2 <= splittedColumns2.length-1) {
							if (accMap.containsKey(column)) {
								sb2.append(accMap.get(column)).append(",");
							} else {
								if (column.startsWith("ID") || column.startsWith("VALUE")) {
									sb2.append("0,");
								} else {
									if (column.startsWith("CLEARING_DATE")) {
										sb2.append(LoadSubscriberMapping.CommonConfigMapping.get("default_last_reset_Date") +",");
									}										
								}
							}
						} else {
							if (accMap.containsKey(column)) {
								sb2.append(accMap.get(column));
							} else {		
								sb2.append(LoadSubscriberMapping.CommonConfigMapping.get("default_last_reset_Date"));
							}
						}
						columnCount2++;
					}
				}
				if (accMap.containsKey("ID_11")) {
					uaList.add(sb2.toString());
				}
				sb2 = null;
			}
			
			if(accMap.size() > 60)
			{
				String[] splittedColumns3 = ACMCOLUMNS3.split(",");
				int columnCount3 = 1;
				if (accMap.containsKey("ID_21")) {
					sb3.append(MSISDN).append(",");
					sb3.append(3).append(",");

					for (String column : splittedColumns3) {
						if (columnCount3 <= splittedColumns3.length-1) {
							if (accMap.containsKey(column)) {
								sb3.append(accMap.get(column)).append(",");
							} else {
								if (column.startsWith("ID") || column.startsWith("VALUE")) {
									sb3.append("0,");
								} else {
									if (column.startsWith("CLEARING_DATE")) {
										sb3.append(LoadSubscriberMapping.CommonConfigMapping.get("default_last_reset_Date") +",");
									}										
								}
							}
						} else {
							if (accMap.containsKey(column)) {
								sb3.append(accMap.get(column));
							} else {		
								sb3.append(LoadSubscriberMapping.CommonConfigMapping.get("default_last_reset_Date"));
							}
						}
						columnCount3++;
					}
				}
				if (accMap.containsKey("ID_21")) {
					uaList.add(sb3.toString());
				}
				sb3 = null;
			}				
		}
		return uaList;
	}
	
	public static Map<String, String> SortDABasedOnValue(Map<String, String> unsortedDA) {
		Map<String,String> sortedDA = new ConcurrentHashMap<>();
		
		int index = 1;
		List<String> sDA_Value = unsortedDA.entrySet().stream().filter(e -> e.getKey().startsWith("ID_"))
				.map(Map.Entry::getValue).collect(Collectors.toList());
		
		List<Integer> DA_Value = sDA_Value.stream().map(Integer::parseInt).collect(Collectors.toList());
		
		Collections.sort(DA_Value);
		
		Set<String> CompletedDAID = new HashSet<>();
		for(int DAID : DA_Value)
		{
			for(Map.Entry<String, String> entry : unsortedDA.entrySet())
			{				
				if(entry.getKey().startsWith("ID_") && DAID == Integer.parseInt(entry.getValue()) && !CompletedDAID.contains(entry.getKey()))
				{
					String IDName = entry.getKey().split("_")[1];
					
					sortedDA.put("ID_" + index, String.valueOf(DAID));
					sortedDA.put("BALANCE_" + index, unsortedDA.get("BALANCE_" + IDName));
					sortedDA.put("START_DATE_" + index,unsortedDA.get("START_DATE_" + IDName));
					sortedDA.put("EXPIRY_DATE_" + index,unsortedDA.get("EXPIRY_DATE_" + IDName));
					sortedDA.put("PAM_SERVICE_ID_" + index,unsortedDA.get("PAM_SERVICE_ID_" + IDName));
					sortedDA.put("PRODUCT_ID_" + index, unsortedDA.get("PRODUCT_ID_" + IDName));
					index++;
					CompletedDAID.add(entry.getKey());
					break;
				}
			}
		}				
		
		return sortedDA;
	}
	
	public static Map<String, String> SortACCMBasedOnValue(Map<String, String> unsortedDA) {
		Map<String,String> sortedACCM = new ConcurrentHashMap<>();
		
		int index = 1;
		List<String> sACC_Value = unsortedDA.entrySet().stream().filter(e -> e.getKey().startsWith("ID_"))
				.map(Map.Entry::getValue).collect(Collectors.toList());
		
		List<Integer> ACCM_Value = sACC_Value.stream().map(Integer::parseInt).collect(Collectors.toList());
		
		Collections.sort(ACCM_Value);
		
		Set<String> CompletedACCMID = new HashSet<>();
		for(int ACCMID : ACCM_Value)
		{
			for(Map.Entry<String, String> entry : unsortedDA.entrySet())
			{				
				if(entry.getKey().startsWith("ID_") && ACCMID == Integer.parseInt(entry.getValue()) && !CompletedACCMID.contains(entry.getKey()))
				{
					String IDName = entry.getKey().split("_")[1];
					sortedACCM.put("ID_" + index, String.valueOf(ACCMID));
					sortedACCM.put("VALUE_" + index, unsortedDA.get("VALUE_" + IDName));
					sortedACCM.put("CLEARING_DATE_" + index, unsortedDA.get("CLEARING_DATE_" + IDName));
					
					index++;
					CompletedACCMID.add(entry.getKey());
					break;
				}
			}
		}
		return sortedACCM;
	}

	public static Date addDays(Date date, int days) {
		GregorianCalendar cal = new GregorianCalendar();
		cal.setTime(date);
		cal.add(Calendar.DATE, days);
				
		return cal.getTime();
	}
	
	public static Date subtractDays(Date date, int days) {
		GregorianCalendar cal = new GregorianCalendar();
		cal.setTime(date);
		cal.add(Calendar.DATE, -days);
				
		return cal.getTime();
	}
	/*
	 * private String mapZainEquipIdWithEoc(String equipId) {
	 * 
	 * Map<String, String> mapEquipIdWithItemCode =
	 * InitializationStep.catalogMainItem.get("ZAIN_MAP");
	 * 
	 * Map<String, Map<String, CatalogItem>> catalogItemData =
	 * InitializationStep.catalogItemData; Map<String, CatalogItem>
	 * catalogItemEoc = catalogItemData.get("EOC"); String result = null;
	 * Set<String> itemKeys = catalogItemEoc.keySet(); for (String itemKey :
	 * itemKeys) { if (itemKey != null && itemKey.startsWith(itemCode)) {
	 * CatalogItem catalogItem = catalogItemEoc.get(itemKey); if (catalogItem !=
	 * null && catalogItem.getItemcode().startsWith("CS_SC")) { result =
	 * catalogItem.getItemcode(); result = result.substring(6, result.length());
	 * break; } } } return result;
	 * 
	 * }
	 */

	public static void main(String args[]) throws ParseException, UnsupportedEncodingException {
	
		System.out.println("Number Of Days: "+convertDateToEpoch("2020-07-22 00:00:00"));;
		Integer [] res = convertDateToTimerOfferDate_New("2014-01-11 09:00:00");
		System.out.println("Date: "+res[0]+" Seconds: "+ res[1]);
		System.out.println("Actual Date from timer dates and secs: " + returnActualDateFromTimerDate(23345, -32256));
		
		System.out.println("Actual Date from number of days: "+convertEpochDateToDate(17807));
		System.out.println(toHexadecimal("30"));
		
		System.out.println(toOfferAttrType3("48"));
		
		System.out.println(getCurrentPamPeriodInDays("Daily"));
		System.out.println(getCurrentPamPeriodInDays("Monthly"));
		
		System.out.println(isDateExpirted("2019-05-28 00:00:00","2019-05-27 23:59:59"));

	}

}