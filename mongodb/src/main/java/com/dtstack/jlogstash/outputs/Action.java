package com.dtstack.jlogstash.outputs;

import org.apache.commons.lang3.StringUtils;

/**
 * @author zxb
 * @version 1.0.0
 *          2017年03月24日 11:01
 * @since Jdk1.6
 */
public enum Action {

    INSERT("insert"),

    UPDATE("update"),

    UPSERT("upsert");

    private String name;

    private Action(String name) {
        this.name = name;
    }

    public static Action getByName(String name) {
        if (StringUtils.isNotEmpty(name)) {
            for (Action action : Action.values()) {
                if (name.equals(action.getName())) {
                    return action;
                }
            }
        }
        return null;
    }

    public String getName() {
        return name;
    }
}
