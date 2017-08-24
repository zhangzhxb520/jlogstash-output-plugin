package com.dtstack.jlogstash.outputs;

import com.dtstack.jlogstash.annotation.Required;
import com.dtstack.jlogstash.render.Formatter;
import com.dtstack.jlogstash.utils.ThreadPoolUtil;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.GridFSUploadStream;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.client.model.*;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author zxb
 * @version 1.0.0
 * 2017年03月24日 10:34
 * @since Jdk1.6
 */
public class MongoDB extends BaseOutput {

    public static final String RECEIVER_THREAD_NAME = "MongoDB-ReceiverThread";
    public static final String BULK_THREAD_NAME = "MongoDB-BulkThread";
    public static final int QUEUE_CAPACITY = 5000;
    public static final int BULK_POOL_SIZE = 5;
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDB.class);

    @Required(required = true)
    private static String uri;

    @Required(required = true)
    private static String db_name;

    @Required(required = true)
    private static String collection;
    /**
     * 需要上传到GridFS的列名
     */
    private static Map<String, String> gridfsColumns;

    /** GridFS文件块大小，单位：字节（byte） */
    private static Integer chunkSize = 358400;

    /**
     * 是否需要做GridFS处理
     */
    private static boolean needHandleGridFS;
    private static Integer size = 3000;

    private GridFSUploadOptions options;
    /**
     * 操作类型，可选值insert,update,upsert
     */
    private String action = "insert";
    private MongoClient client;
    private MongoDatabase database;
    private GridFSBucket gridFSBucket;
    private MongoCollection<Document> coll;
    private volatile BlockingQueue<WriteModel<Document>> writeModelQueue;
    private Action actionType;
    private ExecutorService receiverThreadExecutor = ThreadPoolUtil.newSingleThreadExecutor(QUEUE_CAPACITY, RECEIVER_THREAD_NAME);

    public MongoDB(Map config) {
        super(config);
    }

    public void prepare() {
        if (!needHandleGridFS && gridfsColumns != null && gridfsColumns.size() > 0) {
            needHandleGridFS = true;
        }

        client = new MongoClient(new MongoClientURI(uri));
        database = client.getDatabase(db_name);
        coll = database.getCollection(collection);

        if (needHandleGridFS) {
            gridFSBucket = GridFSBuckets.create(database);
            options = new GridFSUploadOptions().chunkSizeBytes(chunkSize);
        }

        writeModelQueue = new LinkedBlockingQueue<WriteModel<Document>>();

        initAction();

        receiverThreadExecutor.submit(new ReceiverThread());
    }

    private void initAction() {
        actionType = Action.getByName(action);
        if (actionType == null) {
            actionType = Action.INSERT;
        }
    }

    protected void emit(Map event) {
        if (needHandleGridFS) {
            handleGridFS(event);
        }
        Document document = new Document(event);
        try {
            if (actionType == Action.INSERT) {
                writeModelQueue.put(new InsertOneModel<Document>(document));
            } else if (actionType == Action.UPDATE) {
                Object id = document.get("_id");
                BsonDocument bsonDocument = document.toBsonDocument(null, MongoClient.getDefaultCodecRegistry());
                BsonDocument update = new BsonDocument("$set", bsonDocument);
                UpdateOneModel<Document> updateOneModel = new UpdateOneModel<Document>(new Document("_id", id), update);
                writeModelQueue.put(updateOneModel);
            } else if (actionType == Action.UPSERT) {
                Object id = document.get("_id");
                BsonDocument bsonDocument = document.toBsonDocument(null, MongoClient.getDefaultCodecRegistry());
                BsonDocument update = new BsonDocument("$set", bsonDocument);
                UpdateOneModel<Document> updateOneModel = new UpdateOneModel<Document>(new Document("_id", id), update, new UpdateOptions().upsert(true));
                writeModelQueue.put(updateOneModel);
            }
        } catch (InterruptedException e) {
            LOGGER.error("写入数据被中断，" + event, e);
        }
    }

    /**
     * 处理GridFS
     *
     * @param event 事件
     */
    private void handleGridFS(Map event) {
        String columnName;
        String fileName;
        BsonObjectId bsonObjectId = null;
        GridFSUploadStream uploadStream = null;

        for (Map.Entry<String, String> columnEntry : gridfsColumns.entrySet()) {
            columnName = columnEntry.getKey();
            fileName = Formatter.format(event, columnEntry.getValue());

            byte[] data = (byte[]) event.get(columnName);
            if (data == null || data.length <= 0) {
                continue;
            }

            try {
                bsonObjectId = new BsonObjectId();
                uploadStream = gridFSBucket.openUploadStream(bsonObjectId, fileName, options);
                uploadStream.write(data);
            } catch (Exception e) {
                LOGGER.error("GridFS上传失败，文件名：" + fileName, e);
            } finally {
                if (uploadStream != null) {
                    uploadStream.close();
                }
                if (bsonObjectId != null) {
                    event.put(columnName, bsonObjectId.getValue().toHexString());
                }
            }
        }
    }

    @Override
    public void release() {
        receiverThreadExecutor.shutdownNow();
    }

    private class ReceiverThread implements Runnable {

        private List<WriteModel<Document>> cachedModelList = new ArrayList<WriteModel<Document>>(size + 1);

        public void run() {
            WriteModel<Document> writeModel;
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    writeModel = writeModelQueue.take();
                    cachedModelList.add(writeModel);

                    if (cachedModelList.size() >= size) {
                        coll.bulkWrite(cachedModelList, new BulkWriteOptions().ordered(false));
                        cachedModelList = new ArrayList<WriteModel<Document>>(size + 1);
                    }
                }
            } catch (InterruptedException e) {
                LOGGER.error("MongoDB的ReceiverThread被中断", e);
            } finally {
                if (cachedModelList.size() > 0) {
                    coll.bulkWrite(cachedModelList, new BulkWriteOptions().ordered(false));
                }
            }
        }
    }
}
