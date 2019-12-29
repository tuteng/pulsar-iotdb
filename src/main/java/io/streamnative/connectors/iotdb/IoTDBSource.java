/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.streamnative.connectors.iotdb;

import com.google.gson.Gson;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.iotdb.tsfile.read.ReadOnlyTsFile;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;


/**
 * IoTDBSource implement class.
 */
@Slf4j
public class IoTDBSource extends PushSource<String> {

    protected Thread thread = null;

    private TsFileSequenceReader reader;

    private ReadOnlyTsFile readTsFile;

    private IoTDBSourceConfig ioTDBSourceConfig;

    private String[] paths;

    private ArrayList<Path> queryPaths;

    protected volatile boolean running = false;

    private Gson gson;

    protected final Thread.UncaughtExceptionHandler handler = (t, e) -> log.error(
            "[{}] parse events has an error", t.getName(), e);

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        ioTDBSourceConfig = IoTDBSourceConfig.load(config);
        ioTDBSourceConfig.validate();
        reader = new TsFileSequenceReader(ioTDBSourceConfig.getTsfile());
        readTsFile = new ReadOnlyTsFile(reader);
        paths = ioTDBSourceConfig.getPaths().split(",");
        queryPaths = new ArrayList<>();
        for (String path : paths) {
            queryPaths.add(new Path(path));
        }
        gson = new Gson();
        log.info("IoTDBSource init use configuration {}", ioTDBSourceConfig.toString());
        this.start();
    }

    protected void start() {
        Objects.requireNonNull(reader, "reader is null");
        thread = new Thread(this::process);
        thread.setName("IoTDB source thread");
        thread.setUncaughtExceptionHandler(handler);
        running = true;
        thread.start();
    }

    @Override
    public void close() throws IOException, InterruptedException {
        log.info("close IoTDBSource");
        if (!running) {
            return;
        }
        if (readTsFile != null) {
            readTsFile.close();
        }
        if (reader != null) {
            reader.close();
        }
        running = false;
        if (thread != null) {
            thread.interrupt();
            thread.join();
        }
    }

    protected void process() {
        while (running) {
            try {
                log.info("start IoTDB source process");
                while (running) {
                    QueryExpression queryExpression = QueryExpression.create(queryPaths, null);
                    // To do -> Construct conditions and query incremental data
                    QueryDataSet queryDataSet = readTsFile.query(queryExpression);
                    // To do -> After adding the query expression, you can delete this delay,
                    // which is currently only for testing purposes.
                    Thread.sleep(3000);
                    while (queryDataSet.hasNext()) {
                        RowRecord rowRecord = queryDataSet.next();
                        IOTDBRecord<String> iotdbRecord = new IOTDBRecord<>();
                        iotdbRecord.setRecord(extractValue(rowRecord));
                        consume(iotdbRecord);
                    }
                }
            } catch (Exception e) {
                log.error("process error!", e);
            }
        }
    }

    public String extractValue(RowRecord rowRecord) {
        return gson.toJson(rowRecord);
    }

    @Getter
    @Setter
    private static class IOTDBRecord<V> implements Record<V> {

        private V record;

        @Override
        public V getValue() {
            return record;
        }

    }
}
