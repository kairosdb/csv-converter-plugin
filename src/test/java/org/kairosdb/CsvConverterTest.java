/*
 * Copyright 2023 KairosDB Authors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.kairosdb;

import org.junit.Test;
import org.kairosdb.core.PluginException;
import org.kairosdb.csvconverter.CsvConverter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class CsvConverterTest {

    @Test
    public void singleTagGroupBy() throws IOException, PluginException {
        String metricName = "ScanCloud.Plugin.JobCompletedTime.Count";
        CsvConverter converter = new CsvConverter();

        File result = converter.processQueryResults(new File("./src/test/resources/MultipleGroupByTagsQueryResponse.json"));
        result.deleteOnExit();

        List<String> actual = Files.readAllLines(result.toPath());
        assertRow(actual.get(0), "Metric Name", "Timestamp", "Value", "environment", "service");
        assertRow(actual.get(1), metricName, "02/15/2023 10:07:12:012", 2469, "awslab", "clear-api");
        assertRow(actual.get(2), metricName, "02/16/2023 00:20:12:012", 2883, "awslab", "clear-api");
        assertRow(actual.get(3), metricName, "02/17/2023 00:19:12:012", 314, "awslab", "clear-api");
        assertRow(actual.get(4), metricName, "02/15/2023 13:10:25:025", 13, "awslab", "dlp-coordinator");
        assertRow(actual.get(5), metricName, "02/15/2023 18:51:11:011", 4, "awslab", "dr-connector-lab");
        assertRow(actual.get(6), metricName, "02/16/2023 01:11:12:012", 120, "awslab", "dr-connector-lab");
        assertRow(actual.get(7), metricName, "02/15/2023 14:32:11:011", 33, "awslab", "mde-client");
        assertRow(actual.get(8), metricName, "02/15/2023 10:17:11:011", 318, "awslab", "mde-proxy-service");
        assertRow(actual.get(9), metricName, "02/16/2023 09:24:12:012", 30, "awslab", "mde-proxy-service");
        assertRow(actual.get(10), metricName, "02/15/2023 10:31:11:011", 141, "awslab", "oit-service-global-uim-access-ci");
        assertRow(actual.get(11), metricName, "02/16/2023 11:04:39:039", 35, "awslab", "oit-service-global-uim-access-ci");
        assertRow(actual.get(12), metricName, "02/17/2023 09:49:12:012", 21, "awslab", "oit-service-global-uim-access-ci");
    }

    private void assertRow(String actual, String metricName, String timestamp, String value, String service1Value, String service2Value) {
        String[] columns = actual.split(",");
        assertThat(columns[0], equalTo(metricName));
        assertThat(columns[1], equalTo(timestamp));
        assertThat(columns[2], equalTo(value));
        assertThat(columns[3], equalTo(service1Value));
        assertThat(columns[4], equalTo(service2Value));
    }

    private void assertRow(String actual, String metricName, String timestamp, int value, String service1Value, String service2Value) {
        assertRow(actual, metricName, timestamp, Long.toString(value), service1Value, service2Value);
    }
}
