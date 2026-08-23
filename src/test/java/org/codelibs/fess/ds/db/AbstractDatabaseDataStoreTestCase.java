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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.codelibs.fess.app.service.FailureUrlService;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.SystemHelper;
import org.codelibs.fess.opensearch.config.exentity.CrawlingConfig;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.opensearch.config.exentity.FailureUrl;
import org.codelibs.fess.script.ScriptEngineFactory;
import org.codelibs.fess.script.groovy.GroovyEngine;
import org.codelibs.fess.util.ComponentUtil;
import org.junit.jupiter.api.TestInfo;

/**
 * Wires {@link DatabaseDataStore#storeData} up against a real H2 in-memory
 * database, so subclasses can assert over an actual JDBC
 * {@link java.sql.ResultSet} rather than a mock.
 *
 * <p>
 * Script templates in these tests are written as bare column labels.
 * {@code convertValue} returns the parameter map entry directly when the
 * template is an exact key, so the assertions describe what the data store puts
 * into the map without dragging a script engine into them. The real Groovy
 * engine is registered all the same, so a template that does <em>not</em>
 * resolve behaves the way it would on a live server.
 * </p>
 */
public abstract class AbstractDatabaseDataStoreTestCase extends UnitDsTestCase {

    private static final AtomicInteger DB_SEQ = new AtomicInteger();

    protected DatabaseDataStore dataStore;

    protected String jdbcUrl;

    protected CapturingFailureUrlService failureUrlService;

    @Override
    public void setUp(final TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        dataStore = new DatabaseDataStore();
        jdbcUrl = "jdbc:h2:mem:dsdb" + DB_SEQ.incrementAndGet() + ";DB_CLOSE_DELAY=-1";

        // storeData() records per-row statistics, which needs these two helpers.
        ComponentUtil.register(new SystemHelper(), "systemHelper");
        final CrawlerStatsHelper crawlerStatsHelper = new CrawlerStatsHelper();
        crawlerStatsHelper.init();
        ComponentUtil.register(crawlerStatsHelper, "crawlerStatsHelper");

        // The real FailureUrlService needs a behavior bound to OpenSearch. Register a stub
        // under the canonical name, which ComponentUtil.getComponent(Class) falls back to
        // when container auto-binding fails.
        failureUrlService = new CapturingFailureUrlService();
        ComponentUtil.register(failureUrlService, FailureUrlService.class.getCanonicalName());

        final ScriptEngineFactory scriptEngineFactory = new ScriptEngineFactory();
        ComponentUtil.register(scriptEngineFactory, "scriptEngineFactory");
        final GroovyEngine groovyEngine = new GroovyEngine();
        groovyEngine.init();
        groovyEngine.register();
    }

    // ------------------------------------------------------------------
    // fixture helpers
    // ------------------------------------------------------------------

    protected DataStoreParams params(final String sql) {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("driver", "org.h2.Driver");
        paramMap.put("url", jdbcUrl);
        paramMap.put("sql", sql);
        return paramMap;
    }

    protected Map<String, String> scripts(final String... keyValues) {
        final Map<String, String> scriptMap = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            scriptMap.put(keyValues[i], keyValues[i + 1]);
        }
        return scriptMap;
    }

    protected DataConfig newConfig() {
        final DataConfig config = new DataConfig();
        config.setId("test-config");
        return config;
    }

    protected List<Map<String, Object>> runStoreData(final DataStoreParams paramMap, final Map<String, String> scriptMap) {
        return runStoreData(paramMap, scriptMap, new HashMap<>());
    }

    protected List<Map<String, Object>> runStoreData(final DataStoreParams paramMap, final Map<String, String> scriptMap,
            final Map<String, Object> defaultDataMap) {
        final List<Map<String, Object>> stored = new ArrayList<>();
        dataStore.storeData(newConfig(), new CapturingCallback(stored, dataMap -> false), paramMap, scriptMap, defaultDataMap);
        return stored;
    }

    /** Fails with the recorded cause rather than with a bare count mismatch. */
    protected void assertNoRowFailure() {
        if (!failureUrlService.throwables.isEmpty()) {
            final Throwable first = failureUrlService.throwables.get(0);
            throw new AssertionError("A row failed unexpectedly: " + failureUrlService.urls.get(0), first);
        }
    }

    protected Connection connect() throws Exception {
        Class.forName("org.h2.Driver");
        return DriverManager.getConnection(jdbcUrl);
    }

    protected void execute(final String sql) throws Exception {
        try (Connection con = connect(); Statement stmt = con.createStatement()) {
            stmt.execute(sql);
        }
    }

    protected void executeAs(final String user, final String password, final String sql) throws Exception {
        Class.forName("org.h2.Driver");
        try (Connection con = DriverManager.getConnection(jdbcUrl, user, password); Statement stmt = con.createStatement()) {
            stmt.execute(sql);
        }
    }

    /** Records what storeData() reported as a failed row instead of writing to the index. */
    protected static class CapturingFailureUrlService extends FailureUrlService {
        protected final List<Throwable> throwables = new ArrayList<>();

        protected final List<String> urls = new ArrayList<>();

        @Override
        public FailureUrl store(final CrawlingConfig crawlingConfig, final String errorName, final String url, final Throwable e) {
            throwables.add(e);
            urls.add(url);
            return new FailureUrl();
        }
    }

    /** Collects stored documents, optionally rejecting some rows to exercise the failure path. */
    protected static class CapturingCallback implements IndexUpdateCallback {
        private final List<Map<String, Object>> stored;

        private final Predicate<Map<String, Object>> reject;

        protected CapturingCallback(final List<Map<String, Object>> stored, final Predicate<Map<String, Object>> reject) {
            this.stored = stored;
            this.reject = reject;
        }

        @Override
        public void store(final DataStoreParams paramMap, final Map<String, Object> dataMap) {
            if (reject.test(dataMap)) {
                throw new CrawlingAccessException("rejected by test");
            }
            stored.add(new LinkedHashMap<>(dataMap));
        }

        @Override
        public long getDocumentSize() {
            return stored.size();
        }

        @Override
        public long getExecuteTime() {
            return 0L;
        }

        @Override
        public void commit() {
            // nothing
        }
    }
}
