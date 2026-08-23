/*
 * Copyright 2012-2025 CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.codelibs.fess.ds.db;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;

import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Behaviour that only a real PostgreSQL server and its driver expose. Disabled
 * automatically when Docker is not available.
 */
@Testcontainers(disabledWithoutDocker = true)
public class PostgreSqlDataStoreTest extends AbstractDatabaseDataStoreTestCase {

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine");

    @Override
    protected String newJdbcUrl() {
        return POSTGRES.getJdbcUrl();
    }

    @Override
    protected String driverClassName() {
        return POSTGRES.getDriverClassName();
    }

    @Override
    protected String dbUsername() {
        return POSTGRES.getUsername();
    }

    @Override
    protected String dbPassword() {
        return POSTGRES.getPassword();
    }

    @Override
    public void setUp(final TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        // The server outlives each test, so the fixture has to be reset.
        execute("DROP TABLE IF EXISTS doc");
    }

    @Test
    public void test_textColumnIsReadAsString() throws Exception {
        execute("CREATE TABLE doc (id INT PRIMARY KEY, title VARCHAR(100))");
        execute("INSERT INTO doc VALUES (1, 'ようこそ')");

        final List<Map<String, Object>> docs = runStoreData(params("SELECT id, title FROM doc"), scripts("title", "title"));

        assertNoRowFailure();
        assertEquals("ようこそ", docs.get(0).get("title"));
    }

    @Test
    public void test_columnLabelsAreFoldedToLowerCase() throws Exception {
        execute("CREATE TABLE doc (id INT PRIMARY KEY, \"MixedCase\" VARCHAR(10), plain VARCHAR(10))");
        execute("INSERT INTO doc VALUES (1, 'q', 'p')");

        // PostgreSQL folds unquoted identifiers to lower case and keeps quoted ones,
        // the opposite of H2. A script written against one database does not
        // necessarily resolve against the other.
        final List<Map<String, Object>> docs =
                runStoreData(params("SELECT \"MixedCase\", plain FROM doc"), scripts("quoted", "MixedCase", "unquoted", "plain"));

        assertNoRowFailure();
        assertEquals("q", docs.get(0).get("quoted"));
        assertEquals("p", docs.get(0).get("unquoted"));
    }

    /**
     * As on MySQL, a binary column arrives as a byte array, and it is extracted
     * rather than decoded as UTF-8.
     */
    @Test
    public void test_byteaIsExtractedEvenThoughTheDriverReturnsAByteArray() throws Exception {
        execute("CREATE TABLE doc (id INT PRIMARY KEY, payload BYTEA)");
        final byte[] payload = { '%', 'P', 'D', 'F', '-', '1', '.', '4', (byte) 0x0a, (byte) 0x80 };
        try (Connection con = connect(); PreparedStatement ps = con.prepareStatement("INSERT INTO doc VALUES (1, ?)")) {
            ps.setBytes(1, payload);
            ps.execute();
        }

        final DataStoreParams paramMap = params("SELECT payload FROM doc");
        paramMap.put("default_mimetype", "application/pdf");

        final List<Map<String, Object>> docs = runStoreData(paramMap, scripts("content", "payload"));

        assertNoRowFailure();
        assertEquals("pdf:" + new String(payload, StandardCharsets.UTF_8), docs.get(0).get("content"));
    }

    /**
     * PostgreSQL is where the {@code ARRAY} branch actually runs: the driver
     * returns a {@link java.sql.Array}. The branch used to read the element
     * result set without advancing it, so the column was dropped.
     */
    @Test
    public void test_arrayColumnIsJoined() throws Exception {
        execute("CREATE TABLE doc (id INT PRIMARY KEY, tags TEXT[])");
        execute("INSERT INTO doc VALUES (1, ARRAY['alpha', 'beta'])");

        final List<Map<String, Object>> docs = runStoreData(params("SELECT id, tags FROM doc"), scripts("url", "id", "tags", "tags"));

        assertNoRowFailure();
        assertEquals(1, docs.size());
        assertEquals("1", docs.get(0).get("url"));
        assertEquals("alpha beta", docs.get(0).get("tags"));
    }

    /**
     * {@code fetch_size=MIN_VALUE} is a MySQL idiom, and PostgreSQL rejects the
     * negative value. It used to end the crawl having indexed nothing; the
     * rejection is now reported and the crawl runs with the driver default. A
     * configuration copied from a MySQL data store therefore still works here.
     */
    @Test
    public void test_fetchSizeMinValueFallsBackToTheDriverDefault() throws Exception {
        execute("CREATE TABLE doc (id INT PRIMARY KEY)");
        execute("INSERT INTO doc VALUES (1)");

        final DataStoreParams paramMap = params("SELECT id FROM doc");
        paramMap.put("fetch_size", "MIN_VALUE");

        final List<Map<String, Object>> docs = runStoreData(paramMap, scripts("url", "id"));

        assertNoRowFailure();
        assertEquals(1, docs.size());
    }
}
