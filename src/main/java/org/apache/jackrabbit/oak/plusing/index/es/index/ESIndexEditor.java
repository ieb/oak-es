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
package org.apache.jackrabbit.oak.plusing.index.es.index;

import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexEditor;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.WriteOutContentHandler;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.JcrConstants.JCR_DATA;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;

/**
 * Index editor for keeping a ElasticSearch index up to date.
 *
 * Observations.
 * This is based on the Solr index which appears to be quite inefficient. It appears to store one document per property
 * which will bloat each index. It would be better if it stored 1 document per node, but without further investigation that
 * may not be possible depending on the way in which Oak works.
 *
 */
class ESIndexEditor implements IndexEditor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ESIndexEditor.class);
    private static final String INDEX_TYPE_NAME = ESQueryIndex.TYPE;

    /**
     * Parent editor, or {@code null} if this is the root editor.
     */
    private final ESIndexEditor parent;

    /**
     * Name of this node, or {@code null} for the root node.
     */
    private final String name;

    /**
     * Path of this editor, built lazily in {@link #getPath()}.
     */
    private String path;

    private final ESServer esServer;

    private boolean propertiesChanged = false;

    private final IndexUpdateCallback updateCallback;

    private static final Parser parser = new AutoDetectParser();
    /**
     * This queue holds futures from messages sent to the index. It is a runnable and will drain the queue as the futures are done.
     * The drain process is insanely fast, but there is still a risk that the processing of the futures may cause the queue to grow, so
     * it will block if rather than overflow. It should be sized large enough to match the burst rate of the underlying server.
     */
    private DrainingFutureQueue<ActionFuture<? extends ActionResponse>> responseQueue;
    private ESConfiguration configuration;

    ESIndexEditor(
            ESServer esServer,
            ESConfiguration configuration,
            IndexUpdateCallback callback, DrainingFutureQueue<ActionFuture<? extends ActionResponse>> responseQueue) {
        this.parent = null;
        this.name = null;
        this.path = "/";
        this.esServer = esServer;
        this.updateCallback = callback;
        this.responseQueue = responseQueue;
        this.configuration = configuration;
    }


    private ESIndexEditor(ESIndexEditor parent, String name) {
        this.parent = parent;
        this.name = name;
        this.path = null;
        this.configuration = parent.configuration;
        this.esServer = parent.esServer;
        this.responseQueue = parent.responseQueue;
        this.updateCallback = parent.updateCallback;
    }

    String getPath() {
        if (path == null) { // => parent != null
            path = concat(parent.getPath(), name);
        }
        return path;
    }

    // FIXME: This is a performance issue, if this is true, then every property will be indexed in its own ES document, which
    // will bloat the ES index by several orders of magnitude.
    String getId() {
        return getPath();
    }

    @Override
    public void enter(NodeState before, NodeState after) {


    }

    @Override
    public void leave(NodeState before, NodeState after)
            throws CommitFailedException {
        if (propertiesChanged || !before.exists()) {
            updateCallback.indexUpdate();
            try {
                responseQueue.add(esServer.getClient().index(indexRequestFromState(after)));
            } catch (Exception e) {
                throw new CommitFailedException(
                        INDEX_TYPE_NAME, 6, "Failed to submit data for indexing", e);
            }
        }

        if (parent == null) {
            // FIXME: can all properties be indexed here in 1 document.
            // could build a batch of requests here and commit all at once, not certain if that would be better. Leave for the moment.
        }
    }


    @Override
    public void propertyAdded(PropertyState after) {
        propertiesChanged = true;
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) {
        propertiesChanged = true;
    }

    @Override
    public void propertyDeleted(PropertyState before) {
        propertiesChanged = true;
    }

    @Override
    public Editor childNodeAdded(String name, NodeState after) {
        return new ESIndexEditor(this, name);
    }

    @Override
    public Editor childNodeChanged(
            String name, NodeState before, NodeState after) {
        return new ESIndexEditor(this, name);
    }

    @Override
    public Editor childNodeDeleted(String name, NodeState before)
            throws CommitFailedException {
        try {
            responseQueue.add(esServer.getClient().delete(deleteRequestFromPath(path)));
            updateCallback.indexUpdate();
        } catch (Exception e) {
            throw new CommitFailedException(
                    INDEX_TYPE_NAME, 6, "Failed to submit delete for indexing", e);
        }

        return null; // no need to recurse down the removed subtree
    }

    private DeleteRequest deleteRequestFromPath(String path) {
        DeleteRequestBuilder deleteRequestBuilder = new DeleteRequestBuilder(esServer.getClient(), DeleteAction.INSTANCE, configuration.getIndexName());
        deleteRequestBuilder.setId(getId());
        return deleteRequestBuilder.request();
    }

    private IndexRequest indexRequestFromState(NodeState state) {
        Map<String, Object> indexDoc = new HashMap<String, Object>();
        String path = getPath();
        indexDoc.put(configuration.getPathField(), path);
        indexDoc.put(configuration.getPathDepthField(), PathUtils.getDepth(path));

        if (configuration.collapseJcrContentNodes()) {
            int jcrContentIndex = path.lastIndexOf(JcrConstants.JCR_CONTENT);
            if (jcrContentIndex >= 0) {
                int index = jcrContentIndex + JcrConstants.JCR_CONTENT.length();
                String collapsedPath = path.substring(0, index);
                indexDoc.put(configuration.getCollapsedPathField(), collapsedPath);
            }
        }

        for (PropertyState property : state.getProperties()) {
            if ((configuration.getUsedProperties().size() > 0 && configuration.getUsedProperties().contains(property.getName()))
                    || !configuration.getIgnoredProperties().contains(property.getName())) {
                // try to get the field to use for this property from configuration
                String fieldName = configuration.getFieldNameFor(property.getType());
                Object fieldValue;
                if (fieldName != null) {
                    fieldValue = property.getValue(property.getType());
                } else {
                    fieldName = property.getName();
                    if (Type.BINARY.tag() == property.getType().tag()) {
                        fieldValue = extractTextValues(property, state);
                    } else if (property.isArray()) {
                        fieldValue = property.getValue(Type.STRINGS);
                    } else {
                        fieldValue = property.getValue(Type.STRING);
                    }
                }
                // add property field
                indexDoc.put(fieldName, fieldValue);

                Object sortValue;
                if (fieldValue instanceof Iterable) {
                    Iterable values = (Iterable) fieldValue;
                    StringBuilder builder = new StringBuilder();
                    String stringValue = null;
                    for (Object value : values) {
                        builder.append(value);
                        if (builder.length() > 1024) {
                            stringValue = builder.substring(0, 1024);
                            break;
                        }
                    }
                    if (stringValue == null) {
                        stringValue = builder.toString();
                    }
                    sortValue = stringValue;
                } else {
                    if (fieldValue.toString().length() > 1024) {
                        sortValue = fieldValue.toString().substring(0, 1024);
                    } else {
                        sortValue = fieldValue;
                    }
                }

                // add sort field
                indexDoc.put(ESUtils.getSortingField(property.getType().tag(), property.getName()), sortValue);
            }
        }
        IndexRequestBuilder indexRequestBuilder = new IndexRequestBuilder(esServer.getClient(), IndexAction.INSTANCE, configuration.getIndexName());
        indexRequestBuilder.setId(getId());
        indexRequestBuilder.setSource(indexDoc);
        indexRequestBuilder.setOpType(IndexRequest.OpType.INDEX);
        return indexRequestBuilder.request();
    }

    // This will be very slow, blocking the indexing process.
    // It should be offloaded
    private List<String> extractTextValues(
            PropertyState property, NodeState state) {
        List<String> values = new LinkedList<String>();
        Metadata metadata = new Metadata();
        if (JCR_DATA.equals(property.getName())) {
            String type = state.getString(JcrConstants.JCR_MIMETYPE);
            if (type != null) { // not mandatory
                metadata.set(Metadata.CONTENT_TYPE, type);
            }
            String encoding = state.getString(JcrConstants.JCR_ENCODING);
            if (encoding != null) { // not mandatory
                metadata.set(Metadata.CONTENT_ENCODING, encoding);
            }
        }

        for (Blob v : property.getValue(Type.BINARIES)) {
            values.add(parseStringValue(v, metadata));
        }
        return values;
    }

    private String parseStringValue(Blob v, Metadata metadata) {
        LOGGER.warn("Extracting Data from Blob. This is a performance issue");
        WriteOutContentHandler handler = new WriteOutContentHandler();
        try {
            InputStream stream = v.getNewStream();
            try {
                parser.parse(stream, handler, metadata, new ParseContext());
            } finally {
                stream.close();
            }
        } catch (LinkageError e) {
            // Capture and ignore errors caused by extraction libraries
            // not being present. This is equivalent to disabling
            // selected media types in configuration, so we can simply
            // ignore these errors.
        } catch (Throwable t) {
            // Capture and report any other full text extraction problems.
            // The special STOP exception is used for normal termination.
            if (!handler.isWriteLimitReached(t)) {
                LOGGER.debug("Failed to extract text from a binary property: "
                        + " This is a fairly common case, and nothing to"
                        + " worry about. The stack trace is included to"
                        + " help improve the text extraction feature.", t);
                return "TextExtractionError";
            }
        }
        return handler.toString();
    }

}
