package com.dtstack.jlogstash.outputs;

import org.apache.commons.lang3.StringUtils;

/**
 * 索引写入动作
 *
 * @author zxb
 * @version 1.0.0
 *          2017年03月26日 22:09
 * @since Jdk1.6
 */
public enum Action {

    /**
     * 插入操作
     */
    INDEX("index"),

    /**
     * 更新操作
     */
    UPDATE("update"),

    /**
     * 插入更新操作
     */
    UPSERT("upsert"),

    /**
     * 删除操作
     */
    DELETE("delete");

    private String name;

    private Action(String name) {
        this.name = name;
    }

    public static Action getByName(String name) {
        if (StringUtils.isEmpty(name)) {
            return null;
        }

        for (Action action : Action.values()) {
            if (action.getName().equals(name)) {
                return action;
            }
        }
        return null;
    }

    public String getName() {
        return name;
    }
}
