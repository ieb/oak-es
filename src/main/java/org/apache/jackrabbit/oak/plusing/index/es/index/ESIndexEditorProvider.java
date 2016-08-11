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

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ES based {@link IndexEditorProvider}
 *
 * @see ESIndexEditor
 */
public class ESIndexEditorProvider implements IndexEditorProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(ESIndexEditorProvider.class);


    /**
     * Pre-initialised ESServer
     */
    private final ESServer esServer;

    /**
     * default configuration.
     */
    private final ESConfigurationProvider oakESConfigurationProvider;

    /**
     * Holds futures of operations in flight into the index.
     */
    private final DrainingFutureQueue<ActionFuture<? extends ActionResponse>> responseQueue;

    /**
     * Thread to drain the future queue.
     */
    private final Thread thread;


    public ESIndexEditorProvider(
            @Nonnull ESServer esServer,
            @Nonnull ESConfigurationProvider oakESConfigurationProvider,
            int backlog) {
        this.esServer = esServer;
        this.oakESConfigurationProvider = oakESConfigurationProvider;

        this.responseQueue = new DrainingFutureQueue<ActionFuture<? extends ActionResponse>>(backlog, new DrainingFutureQueue.Callback<ActionFuture<? extends ActionResponse>>() {
            @Override
            public void done(ActionFuture<? extends ActionResponse> indexResponseActionFuture) {
                LOGGER.info("Indexed {} ", indexResponseActionFuture.actionGet());
            }
        });
        this.thread = new Thread(this.responseQueue);
        this.thread.start();

    }

    public void dispose() {
        this.responseQueue.stop();
    }

    @Override
    public Editor getIndexEditor(
            @Nonnull String type, @Nonnull NodeBuilder definition, @Nonnull NodeState root, @Nonnull IndexUpdateCallback callback)
            throws CommitFailedException {
        ESIndexEditor editor = null;
        if (ESQueryIndex.TYPE.equals(type)) {
            try {
                // if index definition contains a persisted configuration, use that
                if (isPersistedConfiguration(definition)) {
                    NodeState nodeState = definition.getNodeState();
                    ESConfiguration configuration = new NodeStateESConfiguration(nodeState);
                    editor = getEditor(configuration, callback);
                } else { // otherwise use the default configuration providers (e.g. defined via code or OSGi)
                    ESConfiguration configuration = oakESConfigurationProvider.getConfiguration();
                    editor = getEditor(configuration, callback);
                }
            } catch (Exception e) {
                LOGGER.warn("could not get Solr index editor from {}", definition.getNodeState(), e);
            }
        }
        return editor;
    }

    private boolean isPersistedConfiguration(NodeBuilder definition) {
        return definition.hasChildNode("server");
    }

    private ESIndexEditor getEditor(ESConfiguration configuration,
                                      IndexUpdateCallback callback) {
        try {
            return new ESIndexEditor(esServer, configuration, callback, responseQueue);
        } catch (Exception e) {
            LOGGER.error("unable to create SolrIndexEditor", e);
        }
        return null;
    }

}
