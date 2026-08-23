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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Behaviour that only a real MySQL server and its driver expose. Disabled
 * automatically when Docker is not available.
 */
@Testcontainers(disabledWithoutDocker = true)
public class MySqlDataStoreTest extends AbstractDatabaseDataStoreTestCase {

    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4");

    @Override
    protected String newJdbcUrl() {
        return MYSQL.getJdbcUrl();
    }

    @Override
    protected String driverClassName() {
        return MYSQL.getDriverClassName();
    }

    @Override
    protected String dbUsername() {
        return MYSQL.getUsername();
    }

    @Override
    protected String dbPassword() {
        return MYSQL.getPassword();
    }

    @Override
    public void setUp(final TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        // The server outlives each test, so the fixture has to be reset.
        execute("DROP TABLE IF EXISTS doc");
    }

    @Test
    public void test_textColumnIsReadAsString() throws Exception {
        execute("CREATE TABLE doc (id INT PRIMARY KEY, title VARCHAR(100)) DEFAULT CHARSET=utf8mb4");
        execute("INSERT INTO doc VALUES (1, 'ようこそ')");

        final List<Map<String, Object>> docs = runStoreData(params("SELECT id, title FROM doc"), scripts("title", "title"));

        assertNoRowFailure();
        assertEquals("ようこそ", docs.get(0).get("title"));
    }

    @Test
    public void test_columnLabelsKeepTheDeclaredCase() throws Exception {
        execute("CREATE TABLE doc (id INT PRIMARY KEY, MixedCase VARCHAR(10))");
        execute("INSERT INTO doc VALUES (1, 'x')");

        // Unlike H2, MySQL reports the label exactly as declared rather than folding it.
        final List<Map<String, Object>> docs = runStoreData(params("SELECT MixedCase FROM doc"), scripts("v", "MixedCase"));

        assertNoRowFailure();
        assertEquals("x", docs.get(0).get("v"));
    }

    /**
     * MySQL returns a BLOB column as a byte array rather than as
     * {@link java.sql.Blob}, so this is the driver that proves the byte array
     * branch extracts. It used to decode as UTF-8 instead, which indexed a PDF
     * stored in a MySQL BLOB as mojibake however the type hints were set.
     */
    @Test
    public void test_blobIsExtractedEvenThoughTheDriverReturnsAByteArray() throws Exception {
        execute("CREATE TABLE doc (id INT PRIMARY KEY, payload BLOB)");
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
     * {@code fetch_size=MIN_VALUE} is a MySQL idiom: the driver reads the result
     * set row by row instead of buffering it. This is the only driver where the
     * value is meaningful.
     */
    @Test
    public void test_fetchSizeMinValueStreamsTheResultSet() throws Exception {
        execute("CREATE TABLE doc (id INT PRIMARY KEY)");
        try (Connection con = connect(); PreparedStatement ps = con.prepareStatement("INSERT INTO doc VALUES (?)")) {
            for (int i = 1; i <= 50; i++) {
                ps.setInt(1, i);
                ps.addBatch();
            }
            ps.executeBatch();
        }

        final DataStoreParams paramMap = params("SELECT id FROM doc ORDER BY id");
        paramMap.put("fetch_size", "MIN_VALUE");

        final List<Map<String, Object>> docs = runStoreData(paramMap, scripts("url", "id"));

        assertNoRowFailure();
        assertEquals(50, docs.size());
    }
}
