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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codelibs.fess.Constants;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.helper.CrawlingInfoHelper;
import org.codelibs.fess.opensearch.config.exbhv.DataConfigBhv;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Covers the incremental crawl: expanding the last crawl time into the query,
 * writing the new one back, and keeping the stale-document sweep from emptying
 * the index.
 */
public class DatabaseDataStoreIncrementalTest extends AbstractDatabaseDataStoreTestCase {

    private CapturingDataConfigBhv dataConfigBhv;

    @Override
    public void setUp(final TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        // The real behavior writes to OpenSearch. Capture the update instead.
        dataConfigBhv = new CapturingDataConfigBhv();
        ComponentUtil.register(dataConfigBhv, DataConfigBhv.class.getCanonicalName());
        // AbstractDataStore#store asks it for the document expiry.
        ComponentUtil.register(new CrawlingInfoHelper(), "crawlingInfoHelper");
    }

    // ------------------------------------------------------------------
    // expanding the placeholder
    // ------------------------------------------------------------------

    @Test
    public void test_firstRunSelectsEverything() throws Exception {
        final DataStoreParams paramMap = new DataStoreParams();
        // Nothing stored yet, so the epoch is substituted and every row qualifies.
        assertEquals("SELECT id FROM doc WHERE updated_at > '1970-01-01 00:00:00'",
                dataStore.resolveLastCrawlTime(paramMap, "SELECT id FROM doc WHERE updated_at > '${last_crawl_time}'"));
    }

    @Test
    public void test_storedTimeIsSubstituted() throws Exception {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("last_crawl_time", "2026-08-22 09:00:00");

        assertEquals("SELECT id FROM doc WHERE updated_at > '2026-08-22 09:00:00'",
                dataStore.resolveLastCrawlTime(paramMap, "SELECT id FROM doc WHERE updated_at > '${last_crawl_time}'"));
    }

    @Test
    public void test_queryWithoutThePlaceholderIsUntouched() throws Exception {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("last_crawl_time", "2026-08-22 09:00:00");

        assertEquals("SELECT id FROM doc", dataStore.resolveLastCrawlTime(paramMap, "SELECT id FROM doc"));
    }

    @Test
    public void test_aStoredValueThatIsNotATimestampIsRefused() throws Exception {
        final DataStoreParams paramMap = new DataStoreParams();
        // The value is spliced into the statement, so anything that could change its
        // shape rather than a value in it has to be refused rather than substituted.
        paramMap.put("last_crawl_time", "1970-01-01' OR '1'='1");

        try {
            dataStore.resolveLastCrawlTime(paramMap, "SELECT id FROM doc WHERE updated_at > '${last_crawl_time}'");
            fail("Should throw DataStoreException");
        } catch (final DataStoreException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("last_crawl_time"));
        }
    }

    // ------------------------------------------------------------------
    // the stale-document sweep
    // ------------------------------------------------------------------

    @Test
    public void test_incrementalCrawlTurnsOffTheStaleDocumentSweep() throws Exception {
        execute("CREATE TABLE doc (id INT PRIMARY KEY, updated_at TIMESTAMP)");
        execute("INSERT INTO doc VALUES (1, CURRENT_TIMESTAMP)");

        final DataConfig config = newConfig();
        config.setHandlerParameter(handlerParameter("SELECT id FROM doc WHERE updated_at > '${last_crawl_time}'"));
        assertTrue(dataStore.isIncremental(config));

        // store() has to decide this before the crawl, because DataIndexHelper reads
        // the same map after the crawl to decide whether to sweep.
        final DataStoreParams initParamMap = new DataStoreParams();
        dataStore.store(config, new CapturingCallback(new ArrayList<>(), dataMap -> false), initParamMap);

        assertEquals(Constants.FALSE, initParamMap.getAsString("delete_old_docs"));
    }

    @Test
    public void test_anExplicitSettingWins() throws Exception {
        execute("CREATE TABLE doc (id INT PRIMARY KEY, updated_at TIMESTAMP)");

        final DataConfig config = newConfig();
        config.setHandlerParameter(
                handlerParameter("SELECT id FROM doc WHERE updated_at > '${last_crawl_time}'") + "\ndelete_old_docs=true");

        final DataStoreParams initParamMap = new DataStoreParams();
        dataStore.store(config, new CapturingCallback(new ArrayList<>(), dataMap -> false), initParamMap);

        // The configured parameters are merged over the one store() set.
        assertEquals(Constants.TRUE, initParamMap.getAsString("delete_old_docs"));
    }

    @Test
    public void test_aFullCrawlLeavesTheStaleDocumentSweepAlone() throws Exception {
        execute("CREATE TABLE doc (id INT PRIMARY KEY)");

        final DataConfig config = newConfig();
        config.setHandlerParameter(handlerParameter("SELECT id FROM doc"));
        assertFalse(dataStore.isIncremental(config));

        final DataStoreParams initParamMap = new DataStoreParams();
        dataStore.store(config, new CapturingCallback(new ArrayList<>(), dataMap -> false), initParamMap);

        assertNull(initParamMap.getAsString("delete_old_docs"));
    }

    // ------------------------------------------------------------------
    // writing the watermark back
    // ------------------------------------------------------------------

    @Test
    public void test_theCrawlTimeIsWrittenBackAfterAnIncrementalCrawl() throws Exception {
        execute("CREATE TABLE doc (id INT PRIMARY KEY, updated_at TIMESTAMP)");
        execute("INSERT INTO doc VALUES (1, CURRENT_TIMESTAMP)");

        final DataConfig config = newConfig();
        final String sql = "SELECT id FROM doc WHERE updated_at > '${last_crawl_time}'";
        config.setHandlerParameter("sql=" + sql);

        final DataStoreParams paramMap = params(sql);
        final List<Map<String, Object>> stored = new ArrayList<>();
        dataStore.storeData(config, new CapturingCallback(stored, dataMap -> false), paramMap, scripts("url", "ID"), new HashMap<>());

        assertNoRowFailure();
        assertEquals(1, stored.size());
        assertEquals(1, dataConfigBhv.updated.size());
        final String written = dataConfigBhv.updated.get(0).getHandlerParameter();
        assertTrue(written, written.contains("last_crawl_time="));
        // The query line has to survive, or the next crawl has nothing to run.
        assertTrue(written, written.contains("sql=" + sql));
    }

    @Test
    public void test_aFullCrawlWritesNothingBack() throws Exception {
        execute("CREATE TABLE doc (id INT PRIMARY KEY)");
        execute("INSERT INTO doc VALUES (1)");

        final DataConfig config = newConfig();
        config.setHandlerParameter("sql=SELECT id FROM doc");

        dataStore.storeData(config, new CapturingCallback(new ArrayList<>(), dataMap -> false), params("SELECT id FROM doc"),
                scripts("url", "ID"), new HashMap<>());

        assertNoRowFailure();
        assertTrue(dataConfigBhv.updated.isEmpty());
    }

    @Test
    public void test_anEncryptedValueKeepsItsStoredForm() throws Exception {
        final DataConfig config = newConfig();
        config.setHandlerParameter("driver=org.h2.Driver\npassword={cipher}AAAA\nlast_crawl_time=1970-01-01 00:00:00");

        dataStore.storeLastCrawlTime(config, "2026-08-22 10:00:00");

        final String written = config.getHandlerParameter();
        // Rebuilding from getHandlerParameterMap() would write the decrypted password back.
        assertTrue(written, written.contains("password={cipher}AAAA"));
        assertTrue(written, written.contains("last_crawl_time=2026-08-22 10:00:00"));
        // Updated in place rather than appended a second time.
        assertEquals(1, written.split("last_crawl_time=", -1).length - 1);
    }

    @Test
    public void test_theCrawlTimeIsAppendedWhenItIsNotThereYet() throws Exception {
        final DataConfig config = newConfig();
        config.setHandlerParameter("driver=org.h2.Driver");

        dataStore.storeLastCrawlTime(config, "2026-08-22 10:00:00");

        assertEquals("driver=org.h2.Driver\nlast_crawl_time=2026-08-22 10:00:00", config.getHandlerParameter());
    }

    @Test
    public void test_theCrawlTimeFormatIsConfigurable() throws Exception {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("last_crawl_time_format", "yyyy/MM/dd");

        // 2026-08-22T00:00:00 in the JVM default zone.
        final long startedAt = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2026-08-22 00:00:00").getTime();
        assertEquals("2026/08/22", dataStore.formatCrawlTime(paramMap, startedAt));
    }

    /**
     * The whole point, end to end: the first run selects everything, the second
     * selects only what changed since the first one started.
     */
    @Test
    public void test_theSecondRunOnlySelectsWhatChanged() throws Exception {
        execute("CREATE TABLE doc (id INT PRIMARY KEY, updated_at TIMESTAMP)");
        // Fixed timestamps, one comfortably before this crawl and one after, so the
        // assertion does not depend on how the watermark rounds.
        execute("INSERT INTO doc VALUES (1, TIMESTAMP '2020-01-01 00:00:00')");
        execute("INSERT INTO doc VALUES (2, TIMESTAMP '2099-01-01 00:00:00')");

        final String sql = "SELECT id FROM doc WHERE updated_at > '${last_crawl_time}' ORDER BY id";
        final DataConfig config = newConfig();
        config.setHandlerParameter(handlerParameter(sql));

        final List<Map<String, Object>> firstRun = new ArrayList<>();
        dataStore.storeData(config, new CapturingCallback(firstRun, dataMap -> false), params(sql), scripts("url", "ID"), new HashMap<>());

        assertNoRowFailure();
        assertEquals(2, firstRun.size());
        assertEquals(1, dataConfigBhv.updated.size());

        // Feed the stored watermark back in, as AbstractDataStore#store would.
        final String storedTime = handlerParameterValue(config.getHandlerParameter(), "last_crawl_time");
        final DataStoreParams secondParams = params(sql);
        secondParams.put("last_crawl_time", storedTime);

        final List<Map<String, Object>> secondRun = new ArrayList<>();
        dataStore.storeData(config, new CapturingCallback(secondRun, dataMap -> false), secondParams, scripts("url", "ID"),
                new HashMap<>());

        assertNoRowFailure();
        assertEquals(1, secondRun.size());
        assertEquals("2", secondRun.get(0).get("url"));
    }

    private String handlerParameterValue(final String handlerParameter, final String key) {
        for (final String line : handlerParameter.split("\n")) {
            if (line.startsWith(key + "=")) {
                return line.substring(key.length() + 1);
            }
        }
        throw new AssertionError(key + " is not in " + handlerParameter);
    }

    private String handlerParameter(final String sql) {
        return "driver=org.h2.Driver\nurl=" + jdbcUrl + "\nsql=" + sql;
    }

    /** Captures the data configuration update instead of writing it to OpenSearch. */
    private static class CapturingDataConfigBhv extends DataConfigBhv {
        private final List<DataConfig> updated = new ArrayList<>();

        @Override
        public void update(final DataConfig entity) {
            updated.add(entity);
        }
    }
}
