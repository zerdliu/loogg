package com.baidu.mongo.test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.utils.Utils;

import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public abstract class MongoSpout implements IRichSpout{
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		this.queue = new LinkedBlockingQueue<DBObject>(1000);
		try {
			this.mongoDB = new Mongo(this.mongoHost, this.mongoPort).getDB(mongoDbName);
		} catch (Exception e) {
			throw new RuntimeException(e); 
		}
		
		TailableCursorThread listener = new TailableCursorThread(this.queue, this.mongoDB, 
				this.mongoCollectionName, this.query);
		this.opened.set(true);
		listener.start();
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		this.opened.set(false);
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		DBObject dbo = this.queue.poll();
		if(dbo == null) {
			Utils.sleep(50);
		}else {
			this.collector.emit(dbObjectToStormTuple(dbo));
		}
	}

	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	private SpoutOutputCollector collector;
	
	private LinkedBlockingQueue<DBObject> queue;
	private final AtomicBoolean opened = new AtomicBoolean(false);
	
	
	private DBObject query;
	private DB mongoDB;
	private final String mongoHost;
	private final int mongoPort;
	private final String mongoDbName;
	private final String mongoCollectionName;
	
	public MongoSpout(String mongoHost, int mongoPort, String mongoDbName, 
			String mongoCollectionName, DBObject query) {
		this.mongoHost = mongoHost;
		this.mongoPort = mongoPort;
		this.mongoDbName = mongoDbName;
		this.mongoCollectionName = mongoCollectionName;
		this.query = query;
	}
	
	class TailableCursorThread extends Thread {
		
		LinkedBlockingQueue<DBObject> queue;
		String mongoCollectionName;
		DB mongDB;
		DBObject query;
		
		public TailableCursorThread(LinkedBlockingQueue<DBObject> queue,
				DB mongDB, String mongoCollectionName, DBObject query) {
			this.query = query;
			this.mongDB = mongoDB;
			this.mongoCollectionName = mongoCollectionName;
			this.queue = queue;
		}
		//实现线程的run()方法
		public void run() {
			
			while(opened.get()) {
				try {
					mongDB.requestStart();
					
					final DBCursor cursor = mongoDB.getCollection(mongoCollectionName)
							.find(query)
							.sort(new BasicDBObject("$natural", 1))
							//use "$natural" to return documents in the order they exist on disk
							.addOption(Bytes.QUERYOPTION_TAILABLE)
							.addOption(Bytes.QUERYOPTION_AWAITDATA);
					
					try{
						while(opened.get() && cursor.hasNext()) {
							final DBObject doc = cursor.next();
							
							if (doc == null) break;
							
							queue.put(doc);
						}
					}finally {
						try {
							if(cursor != null)
								cursor.close();
						}catch (final Throwable t){
							
						}
					}
					Utils.sleep(500);
				}catch (final MongoException.CursorNotFound cnf) {
					if(opened.get()) {
						throw cnf;
					}
				} catch (InterruptedException e) {
					break;
				}
			}
		};
	}
	
	public abstract List<Object> dbObjectToStormTuple(DBObject message);

}
