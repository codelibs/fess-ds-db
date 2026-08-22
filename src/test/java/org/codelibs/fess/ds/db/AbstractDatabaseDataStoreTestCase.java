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

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
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

import org.codelibs.core.io.InputStreamUtil;
import org.codelibs.fess.app.service.FailureUrlService;
import org.codelibs.fess.crawler.container.CrawlerContainer;
import org.codelibs.fess.crawler.entity.ExtractData;
import org.codelibs.fess.crawler.extractor.Extractor;
import org.codelibs.fess.crawler.extractor.ExtractorFactory;
import org.codelibs.fess.crawler.helper.ContentLengthHelper;
import org.codelibs.fess.crawler.helper.impl.MimeTypeHelperImpl;
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

    protected TestExtractorFactory extractorFactory;

    @Override
    public void setUp(final TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        dataStore = new DatabaseDataStore();
        jdbcUrl = newJdbcUrl();

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

        // Binary columns are handed to an extractor, so one has to exist. ExtractorBuilder
        // resolves its collaborators through a CrawlerContainer, which the unit-test
        // container does not provide; supply a map-backed one. The extractors tag their
        // output so an assertion can tell which of them ran.
        final Map<String, Object> components = new HashMap<>();
        extractorFactory = new TestExtractorFactory(new MapCrawlerContainer(components));
        extractorFactory.addExtractor("application/pdf", new TaggingExtractor("pdf"));
        extractorFactory.addExtractor("text/plain", new TaggingExtractor("text"));
        components.put("extractorFactory", extractorFactory);
        components.put("contentLengthHelper", new ContentLengthHelper());
        components.put("mimeTypeHelper", new MimeTypeHelperImpl());
        // The name ExtractorBuilder falls back to when no extractor matches the MIME type.
        components.put("tikaExtractor", new TaggingExtractor("fallback"));
        ComponentUtil.register(extractorFactory, "extractorFactory");

        final ScriptEngineFactory scriptEngineFactory = new ScriptEngineFactory();
        ComponentUtil.register(scriptEngineFactory, "scriptEngineFactory");
        final GroovyEngine groovyEngine = new GroovyEngine();
        groovyEngine.init();
        groovyEngine.register();
    }

    // ------------------------------------------------------------------
    // the database under test - overridden by the container based subclasses
    // ------------------------------------------------------------------

    /** A fresh in-memory database per test, so credentials cannot leak between them. */
    protected String newJdbcUrl() {
        return "jdbc:h2:mem:dsdb" + DB_SEQ.incrementAndGet() + ";DB_CLOSE_DELAY=-1";
    }

    protected String driverClassName() {
        return "org.h2.Driver";
    }

    protected String dbUsername() {
        return null;
    }

    protected String dbPassword() {
        return null;
    }

    // ------------------------------------------------------------------
    // fixture helpers
    // ------------------------------------------------------------------

    protected DataStoreParams params(final String sql) {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("driver", driverClassName());
        paramMap.put("url", jdbcUrl);
        if (dbUsername() != null) {
            paramMap.put("username", dbUsername());
        }
        if (dbPassword() != null) {
            paramMap.put("password", dbPassword());
        }
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
        Class.forName(driverClassName());
        if (dbUsername() != null) {
            return DriverManager.getConnection(jdbcUrl, dbUsername(), dbPassword());
        }
        return DriverManager.getConnection(jdbcUrl);
    }

    protected void execute(final String sql) throws Exception {
        try (Connection con = connect(); Statement stmt = con.createStatement()) {
            stmt.execute(sql);
        }
    }

    protected void executeAs(final String user, final String password, final String sql) throws Exception {
        Class.forName(driverClassName());
        try (Connection con = DriverManager.getConnection(jdbcUrl, user, password); Statement stmt = con.createStatement()) {
            stmt.execute(sql);
        }
    }

    /** Exposes the protected crawlerContainer field, which has no setter. */
    protected static class TestExtractorFactory extends ExtractorFactory {
        protected final Map<String, Object> components;

        protected TestExtractorFactory(final MapCrawlerContainer container) {
            this.crawlerContainer = container;
            this.components = container.components;
        }
    }

    protected static class MapCrawlerContainer implements CrawlerContainer {
        private final Map<String, Object> components;

        protected MapCrawlerContainer(final Map<String, Object> components) {
            this.components = components;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T getComponent(final String name) {
            return (T) components.get(name);
        }

        @Override
        public boolean available() {
            return true;
        }

        @Override
        public void destroy() {
            // nothing
        }
    }

    /** Prefixes the extracted text so an assertion can tell which extractor ran. */
    protected static class TaggingExtractor implements Extractor {
        private final String tag;

        protected TaggingExtractor(final String tag) {
            this.tag = tag;
        }

        @Override
        public ExtractData getText(final InputStream in, final Map<String, String> params) {
            return new ExtractData(tag + ":" + new String(InputStreamUtil.getBytes(in), StandardCharsets.UTF_8));
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
