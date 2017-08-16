/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.jlogstash.outputs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 打印数据传输速度
 *
 * @author zxb
 */
@SuppressWarnings("serial")
public class Speed extends BaseOutput {

    private static Logger LOGGER = LoggerFactory.getLogger(Speed.class);

    private static AtomicLong eventNumber = new AtomicLong(0);

    private static Long startTime = System.currentTimeMillis();

    private static ExecutorService executor = Executors.newSingleThreadExecutor();

    private static ScheduledExecutorService scheduleExecutor = Executors.newScheduledThreadPool(1);

    public Speed(Map<String, Object> config) {
        super(config);
    }

    @Override
    public void prepare() {
        scheduleExecutor.scheduleAtFixedRate(new SpeedTask(), 5, 5, TimeUnit.SECONDS);
    }

    @Override
    protected void emit(Map event) {
        eventNumber.getAndIncrement();
    }

    class SpeedTask implements Runnable {
        @Override
        public void run() {
            Long endTime = System.currentTimeMillis();
            Long count = eventNumber.get();
            LOGGER.info("当前共抽取数据量：{}，耗时：{}秒", count, (endTime - startTime) / 1000.0);
        }
    }
}
