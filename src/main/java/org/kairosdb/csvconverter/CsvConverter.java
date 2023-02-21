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

package org.kairosdb.csvconverter;

import com.google.common.collect.ListMultimap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import org.kairosdb.client.DataPointTypeRegistry;
import org.kairosdb.client.builder.DataFormatException;
import org.kairosdb.client.builder.DataPoint;
import org.kairosdb.client.deserializer.GroupByDeserializer;
import org.kairosdb.client.deserializer.ListMultiMapDeserializer;
import org.kairosdb.client.deserializer.ResultsDeserializer;
import org.kairosdb.client.deserializer.TimeZoneDeserializer;
import org.kairosdb.client.response.GroupResult;
import org.kairosdb.client.response.Result;
import org.kairosdb.client.response.grouping.TagGroupResult;
import org.kairosdb.core.PluginException;
import org.kairosdb.core.annotation.PluginName;
import org.kairosdb.core.datastore.QueryPostProcessingPlugin;

import javax.inject.Inject;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.*;

@PluginName(name = "CsvConverter", description = "Converts response JSON to CSV format")
public class CsvConverter implements QueryPostProcessingPlugin {
    private static final String DELIMITER = ",";
    private static final SimpleDateFormat dateFormatter = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss:sss");
    private final Gson gson;
    private boolean showMetricName = true;

    @Inject
    public CsvConverter() {
        GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(GroupResult.class, new GroupByDeserializer());
        builder.registerTypeAdapter(Result.class, new ResultsDeserializer(new DataPointTypeRegistry()));
        builder.registerTypeAdapter(ListMultimap.class, new ListMultiMapDeserializer());
        builder.registerTypeAdapter(TimeZone.class, new TimeZoneDeserializer());
        gson = builder.create();
    }

    @Override
    public String getName() {
        return "CsvConverter";
    }

    @SuppressWarnings("unused")
    public boolean getShowMetricName() {
        return this.showMetricName;
    }

    @SuppressWarnings("unused")
    public void setShowMetricName(boolean showMetricName) {
        this.showMetricName = showMetricName;
    }

    @Override
    public File processQueryResults(File file) throws IOException, PluginException {
        File outputFile = File.createTempFile("kairosdb-csv-plugin", ".csv", file.getParentFile());
        try (PrintWriter writer = new PrintWriter(outputFile)) {
            try (FileReader fileReader = new FileReader(file)) {
                try (JsonReader reader = new JsonReader(fileReader)) {
                    convertToCsv(reader, writer);
                }
            }
        }
        return outputFile;
    }

    private void convertToCsv(JsonReader reader, PrintWriter writer) throws IOException, PluginException {
        while (reader.hasNext()) {
            JsonToken nextToken = reader.peek();
            if (JsonToken.BEGIN_OBJECT.equals(nextToken)) {
                reader.beginObject();
            } else if (JsonToken.BEGIN_ARRAY.equals(nextToken)) {
                reader.beginArray();
            } else if (JsonToken.NAME.equals(nextToken)) {
                String name = reader.nextName();
                if ("queries".equals(name)) {
                    reader.beginArray();
                } else if ("results".equals(name)) {
                    processQueryResults(reader, writer);
                } else {
                    reader.skipValue();
                }
            } else if (JsonToken.END_OBJECT.equals(nextToken)) {
                reader.endObject();
            } else if (JsonToken.END_ARRAY.equals(nextToken)) {
                reader.endArray();
            }
        }
        reader.endObject();
        reader.endArray();
    }

    private void processQueryResults(JsonReader reader, PrintWriter writer) throws PluginException {
        try {
            reader.beginArray();
            boolean firstTime = true;
            boolean done = false;
            while (!done) {
                var nextToken = reader.peek();
                if (JsonToken.BEGIN_OBJECT.equals(nextToken)) {
                    Result queryResult = gson.fromJson(reader, Result.class);
                    var groups = getGroups(queryResult);
                    if (firstTime) {
                        writer.println(createHeader(groups));
                        firstTime = false;
                    }
                    queryResult.getDataPoints().forEach(dataPoint -> writeData(writer, queryResult.getName(), dataPoint, groups.values()));
                    writer.flush();
                } else if (JsonToken.END_OBJECT.equals(nextToken)) {
                    reader.endObject();
                } else if (JsonToken.END_ARRAY.equals(nextToken)) {
                    reader.endArray();
                    done = true;
                }
            }
        } catch (IOException e) {
            throw new PluginException(getName(), "Query results are invalid");
        }
    }

    private TreeMap<String, String> getGroups(Result queryResult) {
        // todo support other groupBy types
        TreeMap<String, String> groupMap = new TreeMap<>();
        queryResult.getGroupResults().forEach(groupBy -> {
            if (groupBy.getName().equals("tag")) {
                TagGroupResult groupResult = (TagGroupResult) groupBy;
                groupMap.putAll(groupResult.getGroup());
            }
        });
        return groupMap;
    }

    private void writeData(PrintWriter writer, String metricName, DataPoint dataPoint, Collection<String> groupValues) {
        if (showMetricName) {
            writer.print(metricName + DELIMITER);
        }
        writer.print(convertDate(dataPoint.getTimestamp()));
        writer.print(DELIMITER + getDataPointValue(dataPoint));
        groupValues.forEach(groupValue -> writer.print(DELIMITER + groupValue));
        writer.println();
    }

    private String createHeader(TreeMap<String, String> groups) {
        StringBuilder builder = new StringBuilder();
        if (showMetricName){
            builder.append("Metric Name" + DELIMITER);
        }
        builder.append("Timestamp" + DELIMITER + "Value");
        groups.forEach((key, value) -> builder.append(DELIMITER).append(key));
        return builder.toString();
    }

    private String getDataPointValue(DataPoint dataPoint) {
        try {
            if (dataPoint.isIntegerValue()) return Long.toString(dataPoint.longValue());
            else return Double.toString(dataPoint.doubleValue());
        } catch (DataFormatException e) {
            return Long.toString(0L);
        }
    }

    private String convertDate(long epoch) {
        return dateFormatter.format(new Date(epoch));
    }
}

