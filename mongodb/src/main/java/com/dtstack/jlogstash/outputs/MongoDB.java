package com.dtstack.jlogstash.outputs;

import com.dtstack.jlogstash.annotation.Required;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @author zxb
 * @version 1.0.0
 *          2017年03月24日 10:34
 * @since Jdk1.6
 */
public class MongoDB extends BaseOutput {

    private static final Logger logger = LoggerFactory.getLogger(MongoDB.class);

    @Required(required = true)
    private static String uri;

    @Required(required = true)
    private static String db_name;

    @Required(required = true)
    private static String collection;

    /**
     * 15秒提交一次
     */
    private static Integer flush_interval = 15;

    private static Integer size = 3000;

    /**
     * 操作类型，可选值insert,update,upsert
     */
    private String action = "insert";

    private MongoClient client;

    private MongoDatabase database;

    private MongoCollection<Document> coll;

    private volatile BlockingQueue<WriteModel<Document>> writeModelQueue;

    private Action actionType;

    private ExecutorService executor = Executors.newSingleThreadExecutor();

    private BulkThread bulkThread;

    private Future<?> future;

    public MongoDB(Map config) {
        super(config);
    }

    public void prepare() {
        client = new MongoClient(new MongoClientURI(uri));
        database = client.getDatabase(db_name);
        coll = database.getCollection(collection);

        writeModelQueue = new LinkedBlockingQueue<WriteModel<Document>>();

        initAction();

        bulkThread = new BulkThread();
        future = executor.submit(bulkThread);
    }

    private void initAction() {
        actionType = Action.getByName(action);
        if (actionType == null) {
            actionType = Action.INSERT;
        }
    }

    protected void emit(Map event) {
        Document document = new Document(event);
        if (actionType == Action.INSERT) {
            writeModelQueue.offer(new InsertOneModel<Document>(document));
        } else if (actionType == Action.UPDATE) {
            Object id = document.get("_id");
            BsonDocument bsonDocument = document.toBsonDocument(null, MongoClient.getDefaultCodecRegistry());
            BsonDocument update = new BsonDocument("$set", bsonDocument);
            UpdateOneModel<Document> updateOneModel = new UpdateOneModel<Document>(new Document("_id", id), update);
            writeModelQueue.offer(updateOneModel);
        } else if (actionType == Action.UPSERT) {
            Object id = document.get("_id");
            BsonDocument bsonDocument = document.toBsonDocument(null, MongoClient.getDefaultCodecRegistry());
            BsonDocument update = new BsonDocument("$set", bsonDocument);
            UpdateOneModel<Document> updateOneModel = new UpdateOneModel<Document>(new Document("_id", id), update, new UpdateOptions().upsert(true));
            writeModelQueue.offer(updateOneModel);
        }
    }

    @Override
    public void release() {
        bulkThread.stop();
        try {
            future.get();
        } catch (InterruptedException e) {
            logger.error("interrupted", e);
        } catch (ExecutionException e) {
            logger.error("execute error", e);
        }
        executor.shutdown();

        if (client != null) {
            client.close();
        }
    }

    private void submit(List<WriteModel<Document>> list) {
        if (list.size() > 0) {
            BulkWriteResult writeResult = null;
            try {
                writeResult = coll.bulkWrite(list, new BulkWriteOptions().ordered(false));
            } catch (Exception e) {
                logger.error("bulk write data error!", e);
            }
        }
    }

    private class BulkThread implements Runnable {

        private Long submitTimestamp = System.currentTimeMillis();

        private volatile boolean stop;

        private List<WriteModel<Document>> cachedModelList = new ArrayList<WriteModel<Document>>(size);

        private long position;

        public void run() {
            while (!stop) {
                WriteModel<Document> writeModel = writeModelQueue.poll();
                if (writeModel != null) {
                    cachedModelList.add(writeModel);
                    position++;

                    if (position % size == 0 || System.currentTimeMillis() - submitTimestamp >= flush_interval) {
                        flush();
                    }
                } else {
                    try {
                        flush();
                        Thread.sleep(flush_interval);
                    } catch (InterruptedException e) {
                        logger.error("interrupted", e);
                    }
                }
            }

            flush();
        }

        public void flush() {
            if (cachedModelList.size() > 0) {
                submit(cachedModelList);
                cachedModelList.clear();
                submitTimestamp = System.currentTimeMillis();
            }
        }

        public void stop() {
            stop = true;
        }
    }
}
