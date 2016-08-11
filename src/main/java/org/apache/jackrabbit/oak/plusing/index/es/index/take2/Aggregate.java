package org.apache.jackrabbit.oak.plusing.index.es.index.take2;

import java.util.List;

/**
 * Created by ieb on 25/05/2016.
 */
class Aggregate {
    public List<Matcher> createMatchers(ESIndexEditor2 esIndexEditor2) {
        return null;
    }

    public interface AggregateRoot {
        void markDirty();
    }
}