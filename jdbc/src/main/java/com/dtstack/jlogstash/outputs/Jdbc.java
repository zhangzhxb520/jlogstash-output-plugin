package com.dtstack.jlogstash.outputs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

/**
 * @author zxb
 * @version 1.0.0
 *          2017年03月27日 14:19
 * @since Jdk1.6
 */
public class Jdbc extends BaseOutput {

    /**
     * logger
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(Jdbc.class);

    private Integer jdbc_batch_size = 3000;

    private long count;

    private Connection connection;

    private PreparedStatement ppst;

    public Jdbc(Map config) {
        super(config);
    }

    public void prepare() {

    }

    protected void emit(Map event) {
        Map<String, Object> row = event;
        try {
            for (Map.Entry<String, Object> keyValue : row.entrySet()) {
                String key = keyValue.getKey();
                Object value = keyValue.getValue();

                ppst.setObject(1, value); // FIXME 参数位置要固定，也不能按遍历顺序来
            }
            ppst.addBatch();

            if (++count % jdbc_batch_size == 0) {
                ppst.executeBatch();
                ppst.clearBatch();
            }
        } catch (SQLException e) {
            LOGGER.error("execute Batch error, event:" + event, e);
        }
    }

    @Override
    public void release() {

    }
}
