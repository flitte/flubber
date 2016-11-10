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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.IndexNotFoundException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Simple unit tests for the {@link Flubber} class.
 *
 * @author flitte
 * @since 18/06/16.
 */
public class FlubberTest {

    private Flubber server;
    private final String TEST_INDEX = "test_index";
    private final String TEST_TYPE = "document";
    private final ObjectMapper mapper = new ObjectMapper();

    @Before
    public void setup() {
        server = new Flubber("data", singletonList(TEST_INDEX));
    }

    @After
    public void tearDown() throws IOException {
        server.shutdown();
    }

    @AfterClass
    public static void afterClass() throws IOException {
        FileUtils.deleteDirectory(new File("./elastic"));
    }

    @Test
    public void testShouldSaveDocumentForQuery() throws Exception {
        final Map<String, String> data = new HashMap<>();
        data.put("key1", "value1");

        saveDocument(data);
        server.refresh(TEST_INDEX);

        final SearchResponse searchResponse = server.getClient()
                                                    .prepareSearch(TEST_INDEX)
                                                    .setTypes(TEST_TYPE)
                                                    .setQuery(constantScoreQuery(
                                                            boolQuery().must(termQuery("key1", "value1"))))
                                                    .setNoFields()
                                                    .setSize(1)
                                                    .execute()
                                                    .actionGet();

        assertEquals(1L, searchResponse.getHits()
                                       .totalHits());
    }

    @Test
    public void testAdminApi() throws ExecutionException, InterruptedException {
        final ClusterHealthResponse clusterIndexHealths = server.getClient()
                                                                .admin()
                                                                .cluster()
                                                                .prepareHealth()
                                                                .execute()
                                                                .get();

        assertEquals("elasticsearch", clusterIndexHealths.getClusterName());
    }

    @Test(expected = IndexNotFoundException.class)
    public void testDeleteIndex() throws Exception {
        final Map<String, String> data = new HashMap<>();
        data.put("key1", "value1");

        saveDocument(data);
        server.refresh(TEST_INDEX);
        server.deleteIndex(TEST_INDEX);
        server.refresh(TEST_INDEX);

        server.getClient()
              .prepareSearch(TEST_INDEX)
              .setTypes(TEST_TYPE)
              .setQuery(constantScoreQuery(boolQuery().must(termQuery("key1", "value1"))))
              .setNoFields()
              .setSize(1)
              .execute()
              .actionGet();

        fail("Index not removed correctly.");
    }

    private void saveDocument(final Map<String, String> data) throws Exception {
        server.getClient()
              .prepareIndex()
              .setIndex(TEST_INDEX)
              .setType(TEST_TYPE)
              .setId("1")
              .setSource(mapper.writeValueAsString(data))
              .execute()
              .get(2, SECONDS);
    }

}
