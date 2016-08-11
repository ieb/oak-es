/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plusing.index.es.index.take2;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.IndexEditor;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.PathFilter;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.tree.TreeFactory;
import org.apache.jackrabbit.oak.plusing.index.es.index.DrainingFutureQueue;
import org.apache.jackrabbit.oak.plusing.index.es.index.ESServer;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.commons.PathUtils.concat;

/**
 * {@link IndexEditor} implementation that is responsible for keeping the Elastic Search index update.
 *
 */
public class ESIndexEditor2 implements IndexEditor, Aggregate.AggregateRoot {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(ESIndexEditor2.class);
    static final String TEXT_EXTRACTION_ERROR = "TextExtractionError";
    private static final String INDEX_TYPE = "ElasticSearch";

    /** Name of this node, or {@code null} for the root node. */
    private final String name;

    /** Parent editor or {@code null} if this is the root editor. */
    private final ESIndexEditor2 parent;
    private final ESIndexEditorContext context;

    /** Path of this editor, built lazily in {@link #getPath()}. */
    private String path;

    private boolean propertiesChanged = false;

    private List<PropertyState> propertiesModified = Lists.newArrayList();

    private final NodeState root;

    /**
     * Flag indicating if the current tree being traversed has a deleted parent.
     */
    private final boolean isDeleted;

    private Tree afterTree;

    private Tree beforeTree;

    private IndexDefinition.IndexingRule indexingRule;

    private List<Matcher> currentMatchers = Collections.emptyList();

    private final MatcherState matcherState;

    private final PathFilter.Result pathFilterResult;

    ESIndexEditor2(NodeState root, NodeBuilder definition,
                      IndexUpdateCallback updateCallback,
                      ESServer esServer,
                      ExtractedTextCache extractedTextCache,
                      IndexAugmentorFactory augmentorFactory,
                      DrainingFutureQueue<ActionFuture<? extends ActionResponse>> responseQueue) throws CommitFailedException {
        this.parent = null;
        this.name = null;
        this.path = "/";
        this.context = new ESIndexEditorContext(esServer, root, definition,
                updateCallback, extractedTextCache, augmentorFactory, responseQueue);
        this.root = root;
        this.isDeleted = false;
        this.matcherState = MatcherState.NONE;
        this.pathFilterResult = context.getDefinition().getPathFilter().filter(getPath());
    }

    private ESIndexEditor2(ESIndexEditor2 parent, String name,
                              MatcherState matcherState,
                              PathFilter.Result pathFilterResult,
                              boolean isDeleted) {
        this.parent = parent;
        this.name = name;
        this.path = null;
        this.context = parent.context;
        this.root = parent.root;
        this.isDeleted = isDeleted;
        this.matcherState = matcherState;
        this.pathFilterResult = pathFilterResult;
    }

    public String getPath() {
        //TODO Use the tree instance to determine path
        if (path == null) { // => parent != null
            path = concat(parent.getPath(), name);
        }
        return path;
    }

    @Override
    public void enter(NodeState before, NodeState after)
            throws CommitFailedException {
        if (EmptyNodeState.MISSING_NODE == before && parent == null){
            context.enableReindexMode();
        }

        if (parent == null){
            afterTree = TreeFactory.createReadOnlyTree(after);
            beforeTree = TreeFactory.createReadOnlyTree(before);
        } else {
            afterTree = parent.afterTree.getChild(name);
            beforeTree = parent.beforeTree.getChild(name);
        }

        //Only check for indexing if the result is include.
        //In case like TRAVERSE nothing needs to be indexed for those
        //path
        if (pathFilterResult == PathFilter.Result.INCLUDE) {
            //For traversal in deleted sub tree before state has to be used
            Tree current = afterTree.exists() ? afterTree : beforeTree;
            indexingRule = getDefinition().getApplicableIndexingRule(current);

            if (indexingRule != null) {
                currentMatchers = indexingRule.getAggregate().createMatchers(this);
            }
        }
    }

    @Override
    public void leave(NodeState before, NodeState after)
            throws CommitFailedException {
        if (propertiesChanged || !before.exists()) {
            String path = getPath();
            if (isIndexable()) {
                if ( context.addOrUpdate(path, after, before.exists(), indexingRule)) {
                    long indexed = context.incIndexedNodes();
                    if (indexed % 1000 == 0) {
                        LOGGER.debug("[{}] => Indexed {} nodes...", getIndexName(), indexed);
                    }
                }
            }
        }

        for (Matcher m : matcherState.affectedMatchers){
            m.markRootDirty();
        }


        if (parent == null) {
            // submit the collected index document as it is.
            context.queueIndex();
            if (context.getIndexedNodes() > 0) {
                LOGGER.debug("[{}] => Indexed {} nodes, done.", getIndexName(), context.getIndexedNodes());
            }
        }
    }


    @Override
    public void propertyAdded(PropertyState after) {
        markPropertyChanged(after.getName());
        checkAggregates(after.getName());
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) {
        markPropertyChanged(before.getName());
        propertiesModified.add(before);
        checkAggregates(before.getName());
    }

    @Override
    public void propertyDeleted(PropertyState before) {
        markPropertyChanged(before.getName());
        propertiesModified.add(before);
        checkAggregates(before.getName());
    }

    @Override
    public Editor childNodeAdded(String name, NodeState after) {
        PathFilter.Result filterResult = getPathFilterResult(name);
        if (filterResult != PathFilter.Result.EXCLUDE) {
            return new ESIndexEditor2(this, name, getMatcherState(name, after), filterResult, false);
        }
        return null;
    }

    @Override
    public Editor childNodeChanged(
            String name, NodeState before, NodeState after) {
        PathFilter.Result filterResult = getPathFilterResult(name);
        if (filterResult != PathFilter.Result.EXCLUDE) {
            return new ESIndexEditor2(this, name, getMatcherState(name, after), filterResult, false);
        }
        return null;
    }

    @Override
    public Editor childNodeDeleted(String name, NodeState before)
            throws CommitFailedException {
        PathFilter.Result filterResult = getPathFilterResult(name);
        if (filterResult == PathFilter.Result.EXCLUDE) {
            return null;
        }

        if (!isDeleted) {
            // tree deletion is handled on the parent node
            String path = concat(getPath(), name);
            try {
                Client client = context.getClient();
                // Remove all index entries in the removed subtree
                this.context.queueDeleteFromPath(path);
                this.context.indexUpdate();
            } catch (Exception e) {
                throw new CommitFailedException(INDEX_TYPE, 5,
                        "Failed to remove the index entries of"
                                + " the removed subtree " + path, e);
            }
        }

        MatcherState ms = getMatcherState(name, before);
        if (!ms.isEmpty()){
            return new ESIndexEditor2(this, name, ms, filterResult, true);
        }
        return null; // no need to recurse down the removed subtree
    }


    /**
     * Determines which all matchers are affected by this property change
     *
     * @param name modified property name
     */
    private void checkAggregates(String name) {
        for (Matcher m : matcherState.matched) {
            if (!matcherState.affectedMatchers.contains(m)
                    && m.aggregatesProperty(name)) {
                matcherState.affectedMatchers.add(m);
            }
        }
    }


    //~-------------------------------------------------------< Aggregate >

    @Override
    public void markDirty() {
        propertiesChanged = true;
    }

    private MatcherState getMatcherState(String name, NodeState after) {
        List<Matcher> matched = Lists.newArrayList();
        List<Matcher> inherited = Lists.newArrayList();
        for (Matcher m : Iterables.concat(matcherState.inherited, currentMatchers)) {
            Matcher result = m.match(name, after);
            if (result.getStatus() == Matcher.Status.MATCH_FOUND){
                matched.add(result);
            }

            if (result.getStatus() != Matcher.Status.FAIL){
                inherited.addAll(result.nextSet());
            }
        }

        if (!matched.isEmpty() || !inherited.isEmpty()) {
            return new MatcherState(matched, inherited);
        }
        return MatcherState.NONE;
    }


    private static class MatcherState {
        final static MatcherState NONE = new MatcherState(Collections.<Matcher>emptyList(),
                Collections.<Matcher>emptyList());

        final List<Matcher> matched;
        final List<Matcher> inherited;
        final Set<Matcher> affectedMatchers;

        public MatcherState(List<Matcher> matched,
                            List<Matcher> inherited){
            this.matched = matched;
            this.inherited = inherited;

            //Affected matches would only be used when there are
            //some matched matchers
            if (matched.isEmpty()){
                affectedMatchers = Collections.emptySet();
            } else {
                affectedMatchers = Sets.newIdentityHashSet();
            }
        }

        public boolean isEmpty() {
            return matched.isEmpty() && inherited.isEmpty();
        }
    }

    private void markPropertyChanged(String name) {
        if (isIndexable()
                && !propertiesChanged
                && indexingRule.isIndexed(name)) {
            propertiesChanged = true;
        }
    }

    private IndexDefinition getDefinition() {
        return context.getDefinition();
    }

    private boolean isIndexable(){
        return indexingRule != null;
    }

    private PathFilter.Result getPathFilterResult(String childNodeName) {
        return context.getDefinition().getPathFilter().filter(concat(getPath(), childNodeName));
    }

    private String getIndexName() {
        return context.getDefinition().getIndexName();
    }

}
