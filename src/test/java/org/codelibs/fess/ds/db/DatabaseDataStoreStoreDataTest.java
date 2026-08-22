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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.junit.jupiter.api.Test;

/**
 * Covers the row loop, the parameter visibility and the failure handling of
 * {@link DatabaseDataStore#storeData}. LOB extraction is covered separately by
 * {@link DatabaseDataStoreLobTest}.
 *
 * <p>
 * Several tests below pin behaviour that is wrong rather than desirable - the
 * dropped {@code ARRAY} column, the binary column that never reaches an
 * extractor, the negative {@code fetch_size} that kills the whole crawl. They
 * are named so that this is obvious, and exist so the fixes later in this stack
 * have something to flip.
 * </p>
 */
public class DatabaseDataStoreStoreDataTest extends AbstractDatabaseDataStoreTestCase {

    private static final String DB_USER = "crawler";

    private static final String DB_PASSWORD = "s3cr3t";

    // ------------------------------------------------------------------
    // row loop
    // ------------------------------------------------------------------

    @Test
    public void test_storeData_mapsEachRowToOneDocument() throws Exception {
        execute("CREATE TABLE doc (id INT PRIMARY KEY, title VARCHAR(100))");
        execute("INSERT INTO doc VALUES (1, 'first'), (2, 'second')");

        final List<Map<String, Object>> docs =
                runStoreData(params("SELECT id, title FROM doc ORDER BY id"), scripts("url", "ID", "title", "TITLE"));

        assertNoRowFailure();
        assertEquals(2, docs.size());
        assertEquals("1", docs.get(0).get("url"));
        assertEquals("first", docs.get(0).get("title"));
        assertEquals("2", docs.get(1).get("url"));
        assertEquals("second", docs.get(1).get("title"));
    }

    @Test
    public void test_storeData_emptyResultSetStoresNothing() throws Exception {
        execute("CREATE TABLE doc (id INT PRIMARY KEY)");

        final List<Map<String, Object>> docs = runStoreData(params("SELECT id FROM doc"), scripts("url", "ID"));

        assertNoRowFailure();
        assertTrue(docs.isEmpty());
    }

    @Test
    public void test_storeData_defaultDataMapIsCopiedPerRow() throws Exception {
        execute("CREATE TABLE doc (id INT PRIMARY KEY)");
        execute("INSERT INTO doc VALUES (1), (2)");

        final Map<String, Object> defaultDataMap = new HashMap<>();
        defaultDataMap.put("label", "shared");
        final List<Map<String, Object>> docs = runStoreData(params("SELECT id FROM doc ORDER BY id"), scripts("url", "ID"), defaultDataMap);

        assertNoRowFailure();
        assertEquals(2, docs.size());
        assertEquals("shared", docs.get(0).get("label"));
        assertEquals("shared", docs.get(1).get("label"));
        // Each row must get its own copy, not a shared reference.
        assertTrue(docs.get(0) != docs.get(1));
    }

    // ------------------------------------------------------------------
    // column labels and values
    // ------------------------------------------------------------------

    @Test
    public void test_storeData_columnLabelCasingFollowsTheDriver() throws Exception {
        execute("CREATE TABLE doc (id INT PRIMARY KEY, \"lower\" VARCHAR(10))");
        execute("INSERT INTO doc VALUES (1, 'x')");

        // H2 folds unquoted identifiers to upper case and keeps quoted ones verbatim.
        // The label the data store publishes is whatever the driver reports, so a
        // script written as "id" does not resolve while "ID" does.
        final List<Map<String, Object>> docs =
                runStoreData(params("SELECT id, \"lower\" FROM doc"), scripts("upper", "ID", "quoted", "lower"));

        assertNoRowFailure();
        assertEquals(1, docs.size());
        assertEquals("1", docs.get(0).get("upper"));
        assertEquals("x", docs.get(0).get("quoted"));
    }

    @Test
    public void test_storeData_columnAliasIsUsedAsTheLabel() throws Exception {
        execute("CREATE TABLE doc (id INT PRIMARY KEY)");
        execute("INSERT INTO doc VALUES (7)");

        final List<Map<String, Object>> docs = runStoreData(params("SELECT id AS doc_id FROM doc"), scripts("url", "DOC_ID"));

        assertNoRowFailure();
        assertEquals("7", docs.get(0).get("url"));
    }

    @Test
    public void test_storeData_nullColumnBecomesEmptyString() throws Exception {
        execute("CREATE TABLE doc (id INT PRIMARY KEY, title VARCHAR(100))");
        execute("INSERT INTO doc VALUES (1, NULL)");

        final List<Map<String, Object>> docs = runStoreData(params("SELECT id, title FROM doc"), scripts("title", "TITLE"));

        assertNoRowFailure();
        assertEquals("", docs.get(0).get("title"));
    }

    @Test
    public void test_storeData_nonTextColumnsAreStringified() throws Exception {
        execute("CREATE TABLE doc (id INT PRIMARY KEY, n BIGINT, d DECIMAL(5,2), b BOOLEAN, ts TIMESTAMP)");
        execute("INSERT INTO doc VALUES (1, 42, 3.50, TRUE, TIMESTAMP '2026-01-02 03:04:05')");

        final List<Map<String, Object>> docs =
                runStoreData(params("SELECT n, d, b, ts FROM doc"), scripts("num", "N", "dec", "D", "bool", "B", "stamp", "TS"));

        assertNoRowFailure();
        assertEquals("42", docs.get(0).get("num"));
        assertEquals("3.50", docs.get(0).get("dec"));
        assertEquals("true", docs.get(0).get("bool"));
        assertEquals("2026-01-02 03:04:05.0", docs.get(0).get("stamp"));
    }

    @Test
    public void test_storeData_clobIsReadInFull() throws Exception {
        execute("CREATE TABLE doc (id INT PRIMARY KEY, body CLOB)");
        final String body = "x".repeat(200000);
        try (Connection con = connect(); Statement stmt = con.createStatement()) {
            stmt.execute("INSERT INTO doc VALUES (1, '" + body + "')");
        }

        final List<Map<String, Object>> docs = runStoreData(params("SELECT body FROM doc"), scripts("content", "BODY"));

        assertNoRowFailure();
        assertEquals(body, docs.get(0).get("content"));
    }

    /**
     * A binary column reaches an extractor whether the driver returns it as a
     * {@code Blob} or as a {@code byte[]}. It used to be decoded as UTF-8 in the
     * byte array case, so the same table produced different content depending on
     * the driver.
     */
    @Test
    public void test_storeData_binaryColumnIsExtracted() throws Exception {
        execute("CREATE TABLE doc (id INT PRIMARY KEY, payload VARBINARY(100))");
        final byte[] payload = { '%', 'P', 'D', 'F', '-', '1', '.', '4', (byte) 0x00, (byte) 0xff };
        try (Connection con = connect(); var ps = con.prepareStatement("INSERT INTO doc VALUES (1, ?)")) {
            ps.setBytes(1, payload);
            ps.execute();
        }

        final List<Map<String, Object>> docs = runStoreData(params("SELECT payload FROM doc"), scripts("content", "PAYLOAD"));

        assertNoRowFailure();
        // The %PDF header makes the builder sniff application/pdf, so the pdf extractor runs.
        assertEquals("pdf:" + new String(payload, StandardCharsets.UTF_8), docs.get(0).get("content"));
    }

    /**
     * The elements of an ARRAY column are joined with spaces. The branch used to
     * read the element ResultSet without advancing it, so the conversion threw
     * and the column was dropped.
     */
    @Test
    public void test_storeData_arrayColumnIsJoined() throws Exception {
        execute("CREATE TABLE doc (id INT PRIMARY KEY, tags VARCHAR ARRAY)");
        execute("INSERT INTO doc VALUES (1, ARRAY['a', 'b', 'c'])");

        final List<Map<String, Object>> docs = runStoreData(params("SELECT id, tags FROM doc"), scripts("url", "ID", "tags", "TAGS"));

        assertNoRowFailure();
        assertEquals(1, docs.size());
        assertEquals("1", docs.get(0).get("url"));
        assertEquals("a b c", docs.get(0).get("tags"));
    }

    // ------------------------------------------------------------------
    // parameter visibility
    // ------------------------------------------------------------------

    @Test
    public void test_storeData_connectionParametersAreVisibleAsScriptVariables() throws Exception {
        // H2 fixes the credentials on the connection that creates the in-memory
        // database, so the fixture has to be built with them.
        executeAs(DB_USER, DB_PASSWORD, "CREATE TABLE doc (id INT PRIMARY KEY)");
        executeAs(DB_USER, DB_PASSWORD, "INSERT INTO doc VALUES (1)");

        final DataStoreParams paramMap = params("SELECT id FROM doc");
        paramMap.put("username", DB_USER);
        paramMap.put("password", DB_PASSWORD);

        final List<Map<String, Object>> docs = runStoreData(paramMap, scripts("leaked", "password", "sqlText", "sql"));

        assertNoRowFailure();
        // The whole data store parameter map is published to scripts, credentials included.
        assertEquals(DB_PASSWORD, docs.get(0).get("leaked"));
        assertEquals("SELECT id FROM doc", docs.get(0).get("sqlText"));
    }

    /**
     * Pins the trap behind the documented {@code url=url} recipe: with no column
     * labelled {@code url}, the script resolves to the JDBC connection URL.
     */
    @Test
    public void test_storeData_urlScriptResolvesToJdbcUrlWhenThereIsNoUrlColumn() throws Exception {
        execute("CREATE TABLE doc (id INT PRIMARY KEY)");
        execute("INSERT INTO doc VALUES (1)");

        final List<Map<String, Object>> docs = runStoreData(params("SELECT id FROM doc"), scripts("url", "url"));

        assertNoRowFailure();
        assertEquals(jdbcUrl, docs.get(0).get("url"));
    }

    @Test
    public void test_storeData_columnValueShadowsParameterOfTheSameName() throws Exception {
        execute("CREATE TABLE doc (id INT PRIMARY KEY, url VARCHAR(100))");
        execute("INSERT INTO doc VALUES (1, 'https://example.com/1')");

        final List<Map<String, Object>> docs = runStoreData(params("SELECT id, url AS url FROM doc"), scripts("url", "URL"));

        assertNoRowFailure();
        assertEquals("https://example.com/1", docs.get(0).get("url"));
    }

    // ------------------------------------------------------------------
    // failures
    // ------------------------------------------------------------------

    @Test
    public void test_storeData_rowFailureIsRecordedAndCrawlingContinues() throws Exception {
        execute("CREATE TABLE doc (id INT PRIMARY KEY)");
        execute("INSERT INTO doc VALUES (1), (2), (3)");

        final List<Map<String, Object>> stored = new ArrayList<>();
        final DataConfig config = newConfig();
        dataStore.storeData(config, new CapturingCallback(stored, dataMap -> "2".equals(dataMap.get("url"))), //
                params("SELECT id FROM doc ORDER BY id"), scripts("url", "ID"), new HashMap<>());

        assertEquals(2, stored.size());
        assertEquals("1", stored.get(0).get("url"));
        assertEquals("3", stored.get(1).get("url"));
        assertEquals(1, failureUrlService.urls.size());
    }

    /**
     * Pins current behaviour: the recorded failure URL is the entire SQL
     * statement with the row number appended.
     */
    @Test
    public void test_storeData_failureUrlIsTheWholeSqlPlusRowNumber() throws Exception {
        execute("CREATE TABLE doc (id INT PRIMARY KEY)");
        execute("INSERT INTO doc VALUES (1)");

        final String sql = "SELECT id FROM doc ORDER BY id";
        dataStore.storeData(newConfig(), new CapturingCallback(new ArrayList<>(), dataMap -> true), //
                params(sql), scripts("url", "ID"), new HashMap<>());

        assertEquals(1, failureUrlService.urls.size());
        assertEquals(sql + ":1", failureUrlService.urls.get(0));
    }

    /**
     * Pins current behaviour: a negative fetch size reaches
     * {@code Statement#setFetchSize}, which rejects it, and the whole crawl ends
     * without indexing a single row.
     */
    @Test
    public void test_storeData_negativeFetchSizeAbortsTheWholeCrawl() throws Exception {
        execute("CREATE TABLE doc (id INT PRIMARY KEY)");
        execute("INSERT INTO doc VALUES (1), (2)");

        final DataStoreParams paramMap = params("SELECT id FROM doc");
        paramMap.put("fetch_size", "-100");

        final List<Map<String, Object>> stored = new ArrayList<>();
        try {
            dataStore.storeData(newConfig(), new CapturingCallback(stored, dataMap -> false), paramMap, scripts("url", "ID"),
                    new HashMap<>());
            fail("Should throw DataStoreException");
        } catch (final DataStoreException e) {
            assertEquals("Failed to crawl data in DB.", e.getMessage());
        }
        assertTrue(stored.isEmpty());
    }

    @Test
    public void test_storeData_positiveFetchSizeIsApplied() throws Exception {
        execute("CREATE TABLE doc (id INT PRIMARY KEY)");
        execute("INSERT INTO doc VALUES (1), (2)");

        final DataStoreParams paramMap = params("SELECT id FROM doc ORDER BY id");
        paramMap.put("fetch_size", "1");

        final List<Map<String, Object>> docs = runStoreData(paramMap, scripts("url", "ID"));

        assertNoRowFailure();
        assertEquals(2, docs.size());
    }

    /**
     * Pins current behaviour: every setup failure, whatever its cause, surfaces
     * as the same message. A missing driver and a malformed query are
     * indistinguishable to an administrator reading the log.
     */
    @Test
    public void test_storeData_missingDriverIsReportedWithTheGenericMessage() {
        final DataStoreParams paramMap = params("SELECT 1");
        paramMap.put("driver", "no.such.Driver");

        try {
            dataStore.storeData(newConfig(), new CapturingCallback(new ArrayList<>(), dataMap -> false), paramMap, scripts("url", "ID"),
                    new HashMap<>());
            fail("Should throw DataStoreException");
        } catch (final DataStoreException e) {
            assertEquals("Failed to crawl data in DB.", e.getMessage());
            assertTrue(e.getCause().getClass().getName(), e.getCause() instanceof ClassNotFoundException);
        }
    }

    @Test
    public void test_storeData_invalidSqlIsReportedWithTheGenericMessage() throws Exception {
        execute("CREATE TABLE doc (id INT PRIMARY KEY)");

        try {
            dataStore.storeData(newConfig(), new CapturingCallback(new ArrayList<>(), dataMap -> false), params("SELECT nope FROM nowhere"),
                    scripts("url", "ID"), new HashMap<>());
            fail("Should throw DataStoreException");
        } catch (final DataStoreException e) {
            assertEquals("Failed to crawl data in DB.", e.getMessage());
        }
    }

}
