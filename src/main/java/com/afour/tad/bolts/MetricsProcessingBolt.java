package com.afour.tad.bolts;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.afour.tad.utils.Constants;
import com.afour.tad.utils.MongoDBConnection;
import com.afour.tad.utils.Utils;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * This class provides implementation of bolt for processing metrics 
 * @author koustubh.m
 *
 */
public class MetricsProcessingBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4089341748331507758L;
	private final String mongoCollectionName;
	private OutputCollector collector;
	private static DB mongoDB = MongoDBConnection.getMongoInstance().getMongoDb();
	
	/**
	 * @param mongoCollectionName The Mongo collection name where data being
	 * written.
	 */
	public MetricsProcessingBolt(String mongoCollectionName){
		this.mongoCollectionName = mongoCollectionName;
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		if (shouldActOnInput(input)) {
			String collectionName = getMongoCollectionForInput();
			if((collectionName != null) && (mongoDB != null)) {
				List<DBObject> dbObjectList = getDBObjectListForInput(input);
				if ((dbObjectList != null) && (dbObjectList.size()>0)) {
					try {
						mongoDB.getCollection(collectionName).insert(dbObjectList);
						collector.ack(input);
					} catch (MongoException me) {
						collector.fail(input);
					}
				}
		   }
		} else {
			collector.ack(input);
		}
	}
	
	/**
	 * Returns the DBObject to store in Mongo for the specified input tuple.
	 * 
	 * @param input the input tuple under consideration
	 * @return the DBObject to be written to Mongo
	 */
	private List<DBObject> getDBObjectListForInput(Tuple tuple) {
		List<DBObject> dbObjects = null;
		DBObject percentageDBObject = null;
		DBObject volumeDBObject = null;
		String sentence = tuple.getString(0);
		
		if(sentence != null) {
			JSONParser jsonParser = new JSONParser();
			dbObjects = new ArrayList<>();
			try {
				JSONObject jsonObject = (JSONObject) jsonParser.parse(sentence);
				int appId = Constants.APP_ID_VALUE;
				String frequency = Constants.DAILY_FREQUENCY_VALUE;
				boolean isDelete = false;
				
				Object timeStamp = jsonObject.get(Constants.TIMESTAMP);
				Object sensorData = jsonObject.get(Constants.SENSOR_DATA);
				int sensorId = Integer.parseInt(jsonObject.get(Constants.SENSOR_ID).toString().trim());
				
				if(timeStamp == null) {
					return null;
				}
				
				if(sensorData == null) {
					return null;
				}
				
				long timeStampMillis = Long.parseLong(timeStamp.toString().trim()); 
				Date dataDate = new Date(timeStampMillis);
				
				percentageDBObject = getDBObjectForPercentage(appId, frequency, isDelete, timeStamp, sensorData, sensorId, dataDate);
				
				if(percentageDBObject != null) {
					dbObjects.add(percentageDBObject);	 
				}
				
				volumeDBObject = getDBObjectForVolume(appId, frequency, isDelete, timeStamp, sensorData, sensorId, dataDate);
				
				if(volumeDBObject != null) {
					dbObjects.add(volumeDBObject);	 
				}
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return dbObjects;
	}
	
	/**
	 * Method to get db object for storing volume information.
	 * @param appId
	 * @param frequency
	 * @param isDelete
	 * @param timeStamp
	 * @param sensorData
	 * @param sensorId
	 * @param dataDate
	 * @return
	 */
	private DBObject getDBObjectForVolume(int appId, String frequency, boolean isDelete, Object timeStamp,Object sensorData, int sensorId, Date dataDate) {
		
		DBObject volumeDBObject = new BasicDBObject();

		fillCommonFields(volumeDBObject, appId, frequency, isDelete, sensorId, dataDate);
		
		volumeDBObject.put(Constants.METRIC_ID, Constants.METRIC_ID_VOLUME);

		int volumeData = getVolumeFromData(sensorData);
		volumeDBObject.put(Constants.DATA, volumeData);
		
		return volumeDBObject;
	}
	
	/**
	 * Method to get db object for storing percentage information.
	 * @param appId
	 * @param frequency
	 * @param isDelete
	 * @param timeStamp
	 * @param sensorData
	 * @param sensorId
	 * @param dataDate
	 * @return
	 */
	private DBObject getDBObjectForPercentage(int appId, String frequency, boolean isDelete, Object timeStamp,Object sensorData, int sensorId, Date dataDate) {
		
		DBObject percentageDBObject = new BasicDBObject();

		fillCommonFields(percentageDBObject, appId, frequency, isDelete, sensorId, dataDate);
		
		percentageDBObject.put(Constants.METRIC_ID, Constants.METRIC_ID_PERCENTAGE);
		
		int percentageData = getPercentageFromData(sensorData);
		percentageDBObject.put(Constants.DATA, percentageData);
		
		return percentageDBObject;
	}

	/**
	 * Method to populate db object with common fields
	 * @param dbObject
	 * @param appId
	 * @param frequency
	 * @param isDelete
	 * @param sensorId
	 * @param dataDate
	 */
	public static void fillCommonFields(DBObject dbObject,int appId, String frequency, boolean isDelete, int sensorId, Date dataDate){
		
		dbObject.put(Constants.APP_ID, appId);
		dbObject.put(Constants.FREQUENCY, frequency);
		dbObject.put(Constants.DELETE, isDelete);
		dbObject.put(Constants.CREATED_DATE,Utils.getDateStringInISO(new Date()));
		dbObject.put(Constants.MODIFY_DATE, Utils.getDateStringInISO(new Date()));
		dbObject.put(Constants.SEN_ID, sensorId);
		dbObject.put(Constants.ASSET_ID, sensorId);
		dbObject.put(Constants.DATA_DATE, Utils.getDateStringInISO(dataDate));
		
		Calendar cal = Calendar.getInstance();
		cal.setTime(dataDate);  
		int hour = cal.get(Calendar.HOUR_OF_DAY);
		dbObject.put(Constants.XAXIS, hour);
	}
	
	/**
	 * Method to calculate percentage for given data
	 * @param data
	 * @return
	 */
	private int getPercentageFromData(Object sensorData) {
		int cmData = Integer.parseInt(sensorData.toString().replace(Constants.NEW_LINE, "").trim());
		return cmData;
	}
	
	/**
	 * Method to calculate volume for given data
	 * @param sensorData
	 * @return
	 */
	private int getVolumeFromData(Object sensorData) {
		int cmData = Integer.parseInt(sensorData.toString().replace(Constants.NEW_LINE, "").trim());
		return cmData;
	}
	
	/**
	 * Method to get Date String in ISO format.
	 * @param dataDate
	 * @return
	 */

	/**
	 * Decide whether or not this input tuple should trigger a Mongo write.
	 *
	 * @param input the input tuple under consideration.
	 * @return {@code true} iff this input tuple should trigger a Mongo write
	 */
	public boolean shouldActOnInput(Tuple input) {
		return true;
	}
	
	/**
	 * Returns the Mongo collection which the input tuple should be written to.
	 *
	 * @param input the input tuple under consideration
	 * @return the Mongo collection which the input tuple should be written to
	 */
	public String getMongoCollectionForInput() {
		return mongoCollectionName;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
