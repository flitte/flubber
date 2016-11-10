/*
 * (C) Copyright 2016 flitte
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flitte.flubber;

import org.apache.commons.io.FileUtils;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static org.elasticsearch.common.settings.Settings.*;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * A simple class which provides a "mock" instance of Elasticsearch.
 *
 * @author flitte
 * @since 18/06/16.
 */
public class Flubber {

    private final Node node;
    private final List<String> indexes;
    private final String dataDirectory;

    /**
     * Default constructor.
     *
     * @param directory the data directory
     * @param indexes   a {@link List} of indexes to create
     */
    public Flubber(final String directory, final List<String> indexes) {
        this.dataDirectory = UUID.randomUUID()
                                 .toString() + "/" + directory;
        this.indexes = indexes;
        final Builder elasticsearchSettings = settingsBuilder()
                .put("http.enabled", "false")
                .put("path.data", "target/" + dataDirectory)
                .put("path.home", "./elastic/");

        node = nodeBuilder()
                .local(true)
                .settings(elasticsearchSettings.build())
                .node();
    }

    /**
     * Perform a refresh request on the specified index.
     *
     * @param index the index to refresh
     */
    public void refresh(final String index) {
        node.client()
            .admin()
            .indices()
            .refresh(new RefreshRequest().indices(index))
            .actionGet();
    }

    /**
     * Get the {@link Client} object for this node.
     *
     * @return the client
     */
    public Client getClient() {
        return node.client();
    }

    /**
     * Shutdown the mock instance and clean away any persistent data created.
     *
     * @throws IOException if it was not possible to delete the data directory
     */
    public void shutdown() throws IOException {
        if (indexes != null && indexes.size() > 0) {
            indexes.stream()
                   .forEach(index -> node.client()
                                         .admin()
                                         .indices()
                                         .delete(new DeleteIndexRequest(index)));
        }

        node.close();
        deleteDataDirectory();
    }

    /**
     * Delete the specified index.
     *
     * @param index the index to delete
     */
    public void deleteIndex(String index) {
        node.client()
            .admin()
            .indices()
            .delete(new DeleteIndexRequest(index));
    }

    /**
     * Delete the data directory for this mock Elasticsearch instance.
     *
     * @throws IOException if it was not possible to delete the data directory
     */
    private void deleteDataDirectory() throws IOException {
        FileUtils.deleteDirectory(new File(dataDirectory));
    }

}