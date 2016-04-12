package com.afour.tad.utils;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;


/**
 * MongoDBConnection class for getting MongoDB instance. 
 * @author koustubh.m
 *
 */
public class MongoDBConnection {

	private static final Logger LOGGER = Logger.getLogger(MongoDBConnection.class);
	private static MongoDBConnection mongoDBConnection;
	private MongoClient mongoClient;
	private DB mongoDb;
	Properties prop = null;
	
	private MongoDBConnection(){
		try(InputStream inputStream = 
				MongoDBConnection.class.getResourceAsStream(Constants.MONGO_DATABASE_PROPERTIES)){
			prop = new Properties();
			if(inputStream==null){
				LOGGER.info("----------Error, unable to find------" + Constants.MONGO_DATABASE_PROPERTIES);
				throw new FileNotFoundException();
			}
			prop.load(inputStream);
			if(null == mongoClient){
				mongoClient = new MongoClient(prop.getProperty(Constants.MONGO_DATABASE_SERVER), 
							  Integer.parseInt(prop.getProperty(Constants.MONGO_DATABASE_PORT)));
				this.setMongoDb(mongoClient.getDB(prop.getProperty(Constants.MONGO_DATABASE_NAME)));
			}
	    LOGGER.info("------------MongoDB Initialized-------------");
		}catch (MongoException | IOException e){
		 	LOGGER.error("Exception while getting mongo connection",e);
		}catch(Exception e){
			LOGGER.error("Exception while getting mongo connection",e);
		}
	}
	
	public static MongoDBConnection getMongoInstance(){
		if(null == mongoDBConnection){
			mongoDBConnection = new MongoDBConnection();
		}
		return mongoDBConnection;
	}
	
	public Properties getProp() {
		return prop;
	}

	public void setProp(Properties prop) {
		this.prop = prop;
	}

	public DB getMongoDb() {
		return mongoDb;
	}

	public void setMongoDb(DB mongoDb) {
		this.mongoDb = mongoDb;
	}
		
}
