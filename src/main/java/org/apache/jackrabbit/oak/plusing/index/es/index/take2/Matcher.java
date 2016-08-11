package org.apache.jackrabbit.oak.plusing.index.es.index.take2;

import org.apache.jackrabbit.oak.spi.state.NodeState;

import java.util.Collection;

/**
 * Created by ieb on 25/05/2016.
 */
public class Matcher {
    public Collection<? extends Matcher> nextSet() {
        return null;
    }

    public static enum Status {CONTINUE, MATCH_FOUND, FAIL}
    private Status status;

    public void markRootDirty() {

    }

    public boolean aggregatesProperty(String name) {
        return false;
    }

    public Matcher match(String name, NodeState after) {
        return null;
    }

    public Status getStatus() {
        return status;
    }

}
