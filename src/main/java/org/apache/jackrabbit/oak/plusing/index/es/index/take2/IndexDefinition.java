package org.apache.jackrabbit.oak.plusing.index.es.index.take2;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.PathFilter;

/**
 * Created by ieb on 25/05/2016.
 */
public class IndexDefinition {
    private int maxExtractLength;
    private String indexName;
    private PathFilter pathFilter;

    public PathFilter getPathFilter() {
        return pathFilter;
    }

    public int getMaxExtractLength() {
        return maxExtractLength;
    }

    public String getIndexName() {
        return indexName;
    }

    public IndexingRule getApplicableIndexingRule(Tree current) {
        return null;
    }

    public class IndexingRule {

        private Aggregate aggregate;
        private boolean nodeNameIndexed;

        public Aggregate getAggregate() {
            return aggregate;
        }

        public PropertyDefinition getConfig(String pname) {
            return null;
        }

        public boolean isNodeNameIndexed() {
            return nodeNameIndexed;
        }

        public boolean indexesAllNodesOfMatchingType() {
            return false;
        }

        public boolean isIndexed(String name) {
            return false;
        }
    }
}