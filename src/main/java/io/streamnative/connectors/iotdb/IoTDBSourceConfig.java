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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Paths;
import java.util.Map;
import lombok.Data;
import org.apache.pulsar.io.core.annotations.FieldDoc;
import org.apache.pulsar.shade.org.apache.commons.lang.StringUtils;

/**
 * Configuration class for the IoTDB Source Connector.
 */
@Data
public class IoTDBSourceConfig {

    /**
     * The input file name.
     */
    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "The path of tsfile"
    )
    private String tsfile;

    /**
     * The device path.
     */
    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "A comma-separated list of path "
    )
    private String paths;

    public static IoTDBSourceConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), IoTDBSourceConfig.class);
    }

    public static IoTDBSourceConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), IoTDBSourceConfig.class);
    }

    public void validate() {
        if (StringUtils.isBlank(tsfile)) {
            throw new IllegalArgumentException("Required property tsfile not set.");
        } else if (Files.notExists(Paths.get(tsfile), LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalArgumentException("Specified input file name path does not exist");
        } else if (!Files.isReadable(Paths.get(tsfile))) {
            throw new IllegalArgumentException("Specified input directory is not readable");
        }
        if (StringUtils.isBlank(paths)) {
            throw new IllegalArgumentException("Required property path not set.");
        }
    }
}
