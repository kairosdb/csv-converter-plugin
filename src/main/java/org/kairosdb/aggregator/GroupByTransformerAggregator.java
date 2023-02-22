package org.kairosdb.aggregator;


import com.google.inject.Inject;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.annotation.FeatureComponent;
import org.kairosdb.core.annotation.FeatureProperty;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.groupby.GroupByResult;
import org.kairosdb.core.groupby.TagGroupByResult;
import org.kairosdb.plugin.Aggregator;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;


@FeatureComponent(
        name = "group_by_transformer",
        description = "Uppercase or lowercase groupBy values."
)
public class GroupByTransformerAggregator implements Aggregator {
    @Inject
    public GroupByTransformerAggregator() {
    }

    @FeatureProperty(
            name = "action",
            label = "Action",
            description = "Action to perform. Options are lowercase or uppercase.",
            default_value = "lowercase"
    )
    private String action = "";

    @FeatureProperty(
            name = "tag_names",
            label = "Tag Names",
            description = "List of tag names that the action will be performed on."
    )    private List<String> tagNames = Collections.emptyList();

    private final BiFunction<String, String, String> toLowercase = (key, value) -> tagNames.contains(key) ? value.toLowerCase() : value;
    private final BiFunction<String, String, String> toUppercase = (key, value) -> tagNames.contains(key) ? value.toUpperCase() : value;
    private BiFunction<String, String, String> actionFunction = toLowercase;

    @SuppressWarnings("unused")
    public void setAction(String action) {
        this.action = action;
        actionFunction = "lowercase".equalsIgnoreCase(action) ? toLowercase : toUppercase;
    }

    @SuppressWarnings("unused")
    public void setTagNames(List<String> tagNames) {
        this.tagNames = tagNames;
    }

    @Override
    public DataPointGroup aggregate(DataPointGroup dataPointGroup) {
        if (!action.isEmpty() && tagNames.isEmpty()) {
            throw new IllegalArgumentException("TagNames cannot be empty");
        }
        var groupBys = dataPointGroup.getGroupByResult();
        var modifiedGroupBys = groupBys.stream().map(this::convert).collect(Collectors.toList());
        return new GroupByDataPointGroup(dataPointGroup, modifiedGroupBys);
    }

    private GroupByResult convert(GroupByResult groupBy) {
        if (groupBy instanceof TagGroupByResult) {
            var tagGroupBy = (TagGroupByResult) groupBy;
            tagGroupBy.getTagResults().replaceAll(actionFunction);
            return tagGroupBy;
        } else {
            return groupBy;
        }
    }

    @Override
    public boolean canAggregate(String groupType) {
        return true;
    }

    @Override
    public String getAggregatedGroupType(String s) {
        return null;
    }

    @Override
    public void init() {
    }

    private static class GroupByDataPointGroup implements DataPointGroup {
        private final List<GroupByResult> groupByResults;
        private final DataPointGroup dataPointGroup;

        public GroupByDataPointGroup(DataPointGroup dataPointGroup, List<GroupByResult> groupByResults) {
            this.groupByResults = groupByResults;
            this.dataPointGroup = dataPointGroup;
        }

        @Override
        public String getName() {
            return dataPointGroup.getName();
        }

        @Override
        public List<GroupByResult> getGroupByResult() {
            return groupByResults;
        }

        @Override
        public void close() {
            dataPointGroup.close();
        }

        @Override
        public boolean hasNext() {
            return dataPointGroup.hasNext();
        }

        @Override
        public DataPoint next() {
            return dataPointGroup.next();
        }

        @Override
        public Set<String> getTagNames() {
            return dataPointGroup.getTagNames();
        }

        @Override
        public Set<String> getTagValues(String s) {
            return dataPointGroup.getTagValues(s);
        }
    }
}
