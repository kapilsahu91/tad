package com.afour.tad.utils;	

import java.net.NetworkInterface;
import java.net.SocketException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import org.apache.storm.shade.org.joda.time.DateTime;
import org.apache.storm.shade.org.joda.time.format.DateTimeFormatter;
import org.apache.storm.shade.org.joda.time.format.ISODateTimeFormat;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class Utils {
	
	public static  Map<String,HashMap<String, String>> sensorChannel = new HashMap<>();
	public static DB mongoDb= MongoDBConnection.getMongoInstance().getMongoDb();
	static{
			DBCursor dbCollection = mongoDb.getCollection("IOT_SENSORS").find();
			HashMap<String, String> innerHashMap = null;
			for(DBObject dbObject:dbCollection) {
				if(sensorChannel.get(dbObject.get("CUST_ID").toString()) != null) {
					sensorChannel.get(dbObject.get("CUST_ID").toString()).put(dbObject.get("DESC").toString(), dbObject.get("_id").toString());
				} else {
					innerHashMap = new HashMap<>();
					innerHashMap.put(dbObject.get("DESC").toString(), dbObject.get("_id").toString());
					sensorChannel.put(dbObject.get("CUST_ID").toString(), innerHashMap);
				}
			}
			
			System.out.println(sensorChannel);
	}
	
	public static Date getDateStringInISO(Date dataDate) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US);
		DateTimeFormatter dateTimeFormat = ISODateTimeFormat.dateTime();
		DateTime result = dateTimeFormat.parseDateTime(format.format(dataDate));
		return result.toDate();
	}
    public static Date getDataDateFromFeed(Object object) {
		String created_at = (String)object;
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.US);
		Date date = null;
		try {
			date = format.parse(created_at);
		} catch (java.text.ParseException e) {
			e.printStackTrace();
		}
		return date;
	}
    private static final Random random = new Random();
    public static String getMacAddress() {
        String result = "";
        try {
            for (NetworkInterface ni : Collections.list(
                    NetworkInterface.getNetworkInterfaces())) {
                byte[] hardwareAddress = ni.getHardwareAddress();

                if (hardwareAddress != null) {
                    for (int i = 0; i < hardwareAddress.length; i++)
                        result += String.format((i == 0 ? "" : "-") + "%02X", hardwareAddress[i]);
                    return result;
                }
            }

        } catch (SocketException e) {
            System.out.println("Could not find out MAC Adress. Exiting Application ");
            System.exit(1);
        }
        return result;
    }
    public static int createRandomNumberBetween(int min, int max) {
        return random.nextInt(max - min + 1) + min;
    }
}

