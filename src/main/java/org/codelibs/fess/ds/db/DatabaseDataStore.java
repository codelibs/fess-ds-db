/*
 * Copyright 2012-2024 CodeLibs Project and the Others.
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

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.core.io.ReaderUtil;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.fess.Constants;
import org.codelibs.fess.app.service.FailureUrlService;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.exception.MultipleCrawlingAccessException;
import org.codelibs.fess.crawler.extractor.ExtractorBuilder;
import org.codelibs.fess.ds.AbstractDataStore;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.es.config.exentity.DataConfig;
import org.codelibs.fess.exception.DataStoreCrawlingException;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.exception.FessSystemException;
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsAction;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsKeyObject;
import org.codelibs.fess.mylasta.direction.FessConfig;
import org.codelibs.fess.util.ComponentUtil;

public class DatabaseDataStore extends AbstractDataStore {
    private static final Logger logger = LogManager.getLogger(DatabaseDataStore.class);

    private static final String SQL_PARAM = "sql";

    private static final String URL_PARAM = "url";

    private static final String PASSWORD_PARAM = "password";

    private static final String USERNAME_PARAM = "username";

    private static final String DRIVER_PARAM = "driver";

    private static final String FETCH_SIZE_PARAM = "fetch_size";

    private static final String DEFAULT_MIMETYPE = "default_mimetype";

    private static final String INFO_PREFIX = "info.";

    private static final String COLUMN_LABEL_PREFIX = "column_label.";

    @Override
    protected String getName() {
        return this.getClass().getSimpleName();
    }

    protected String getDriverClass(final DataStoreParams paramMap) {
        final String driverName = paramMap.getAsString(DRIVER_PARAM);
        if (StringUtil.isBlank(driverName)) {
            throw new DataStoreException("JDBC driver is null");
        }
        return driverName;
    }

    protected String getUsername(final DataStoreParams paramMap) {
        return paramMap.getAsString(USERNAME_PARAM);
    }

    protected String getPassword(final DataStoreParams paramMap) {
        return paramMap.getAsString(PASSWORD_PARAM);
    }

    protected String getUrl(final DataStoreParams paramMap) {
        return paramMap.getAsString(URL_PARAM);
    }

    protected Integer getFetchSize(final DataStoreParams paramMap) {
        final String value = paramMap.getAsString(FETCH_SIZE_PARAM);
        if (StringUtil.isNotBlank(value)) {
            if ("MIN_VALUE".equals(value)) {
                return Integer.MIN_VALUE;
            }

            try {
                return Integer.parseInt(value);
            } catch (final NumberFormatException e) {
                logger.debug("Failed to pase {}", value, e);
            }
        }
        return null;
    }

    protected String getSql(final DataStoreParams paramMap) {
        final String sql = paramMap.getAsString(SQL_PARAM);
        if (StringUtil.isBlank(sql)) {
            throw new DataStoreException("sql is null");
        }
        return sql;
    }

    @Override
    protected void storeData(final DataConfig config, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap) {

        final CrawlerStatsHelper crawlerStatsHelper = ComponentUtil.getCrawlerStatsHelper();
        final long readInterval = getReadInterval(paramMap);
        final String scriptType = getScriptType(paramMap);

        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            Class.forName(getDriverClass(paramMap));

            con = getConnection(paramMap);

            final String sql = getSql(paramMap);
            final Integer fetchSize = getFetchSize(paramMap);
            if (logger.isDebugEnabled()) {
                logger.debug("sql: {}, fetch_size: {}", sql, fetchSize);
            }
            stmt = con.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
            if (fetchSize != null) {
                stmt.setFetchSize(fetchSize);
            }
            rs = stmt.executeQuery(sql); // SQL generated by an administrator
            boolean loop = true;
            int count = 0;
            while (rs.next() && loop && alive) {
                count++;
                final StatsKeyObject statsKey = new StatsKeyObject(config.getId() + "#" + count);
                paramMap.put(Constants.CRAWLER_STATS_KEY, statsKey);
                final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
                final Map<String, Object> crawlingContext = new HashMap<>();
                try {
                    crawlerStatsHelper.begin(statsKey);
                    crawlingContext.put("doc", dataMap);
                    final ResultSetParamMap params = new ResultSetParamMap(config, crawlingContext, rs, paramMap);
                    if (logger.isDebugEnabled()) {
                        logger.debug("params: {}", params);
                    }

                    crawlerStatsHelper.record(statsKey, StatsAction.PARSED);

                    for (final Map.Entry<String, String> entry : scriptMap.entrySet()) {
                        final Object convertValue = convertValue(scriptType, entry.getValue(), params);
                        if (logger.isDebugEnabled()) {
                            logger.debug("{}: {} -> {}", entry.getKey(), entry.getValue(), convertValue);
                        }
                        if (convertValue != null) {
                            dataMap.put(entry.getKey(), convertValue);
                        }
                    }

                    crawlerStatsHelper.record(statsKey, StatsAction.EVALUATED);

                    if (logger.isDebugEnabled()) {
                        logger.debug("dataMap: {}", dataMap);
                    }
                    callback.store(paramMap, dataMap);
                    crawlerStatsHelper.record(statsKey, StatsAction.FINISHED);
                } catch (final CrawlingAccessException e) {
                    logger.warn("Crawling Access Exception at : {}", dataMap, e);

                    Throwable target = e;
                    if (target instanceof final MultipleCrawlingAccessException ex) {
                        final Throwable[] causes = ex.getCauses();
                        if (causes.length > 0) {
                            target = causes[causes.length - 1];
                        }
                    }

                    String errorName;
                    final Throwable cause = target.getCause();
                    if (cause != null) {
                        errorName = cause.getClass().getCanonicalName();
                    } else {
                        errorName = target.getClass().getCanonicalName();
                    }

                    String url;
                    if (target instanceof final DataStoreCrawlingException dce) {
                        url = dce.getUrl();
                        if (dce.aborted()) {
                            loop = false;
                        }
                    } else {
                        url = sql + ":" + rs.getRow();
                    }
                    final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
                    failureUrlService.store(config, errorName, url, target);
                    crawlerStatsHelper.record(statsKey, StatsAction.ACCESS_EXCEPTION);
                } catch (final Throwable t) {
                    logger.warn("Crawling Access Exception at : {}", dataMap, t);
                    final String url = sql + ":" + rs.getRow();
                    final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
                    failureUrlService.store(config, t.getClass().getCanonicalName(), url, t);
                    crawlerStatsHelper.record(statsKey, StatsAction.EXCEPTION);
                } finally {
                    crawlerStatsHelper.done(statsKey);
                }

                if (readInterval > 0) {
                    sleep(readInterval);
                }
            }
        } catch (final Exception e) {
            throw new DataStoreException("Failed to crawl data in DB.", e);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (final SQLException e) {
                logger.warn("Failed to close a result set.", e);
            } finally {
                try {
                    if (stmt != null) {
                        stmt.close();
                    }
                } catch (final SQLException e) {
                    logger.warn("Failed to close a statement.", e);
                } finally {
                    try {
                        if (con != null) {
                            con.close();
                        }
                    } catch (final SQLException e) {
                        logger.warn("Failed to close a db connection.", e);
                    }
                }
            }

        }
    }

    protected Connection getConnection(final DataStoreParams paramMap) throws SQLException {
        final String jdbcUrl = getUrl(paramMap);

        final String username = getUsername(paramMap);
        if (logger.isDebugEnabled()) {
            logger.debug("jdbc: {} : {}", jdbcUrl, username);
        }

        final Properties info = new Properties();
        if (username != null) {
            info.put("user", username);
        }

        final String password = getPassword(paramMap);
        if (password != null) {
            info.put("password", password);
        }

        for (final String key : paramMap.asMap().keySet()) {
            if (key.startsWith(INFO_PREFIX)) {
                final String k = key.substring(INFO_PREFIX.length());
                final Object v = paramMap.get(key);
                if (logger.isDebugEnabled()) {
                    logger.debug("jdbc: info: {}={}", k, v);
                }
                info.put(k, v);
            }
        }

        return DriverManager.getConnection(jdbcUrl, info);
    }

    protected static class ResultSetParamMap implements Map<String, Object> {
        private final Map<String, Object> paramMap = new HashMap<>();

        public ResultSetParamMap(final DataConfig config, final Map<String, Object> crawlingContext, final ResultSet resultSet,
                final DataStoreParams paramMap) {
            this.paramMap.putAll(paramMap.asMap());
            this.paramMap.put("crawlingConfig", config);
            this.paramMap.put("crawlingContext", crawlingContext);

            try {
                final ResultSetMetaData metaData = resultSet.getMetaData();
                final int columnCount = metaData.getColumnCount();
                for (int i = 0; i < columnCount; i++) {
                    try {
                        final String label = metaData.getColumnLabel(i + 1);
                        final String value = getColumnValue(resultSet, i + 1);
                        this.paramMap.put(label, value);
                    } catch (final IOException | SQLException e) {
                        logger.warn("Failed to parse data in a result set. The column is {}.", i + 1, e);
                    }
                }
            } catch (final Exception e) {
                throw new FessSystemException("Failed to access meta data.", e);
            }
        }

        protected String getColumnValue(final ResultSet resultSet, final int columnIndex) throws IOException, SQLException {
            final Object obj = resultSet.getObject(columnIndex);
            if (obj instanceof final Blob value) {
                try (final InputStream in = value.getBinaryStream()) {
                    final FessConfig fessConfig = ComponentUtil.getFessConfig();
                    final ExtractorBuilder builder = ComponentUtil.getExtractorFactory().builder(in, null);
                    if (paramMap.get(COLUMN_LABEL_PREFIX + fessConfig.getIndexFieldMimetype()) instanceof final String mimetypeField
                            && paramMap.get(mimetypeField) instanceof final String mimetype) {
                        builder.mimeType(mimetype);
                    } else if (paramMap.get(COLUMN_LABEL_PREFIX + fessConfig.getIndexFieldFilename()) instanceof final String filenameField
                            && paramMap.get(filenameField) instanceof final String filename) {
                        builder.filename(filename);
                    } else if (paramMap.get(DEFAULT_MIMETYPE) instanceof final String defaultMimetype) {
                        builder.mimeType(defaultMimetype);
                    }
                    return builder.extract().getContent();
                }
            }
            if (obj instanceof final byte[] value) {
                return new String(value, StandardCharsets.UTF_8);
            } else if (obj instanceof final Clob value) {
                try (final Reader reader = value.getCharacterStream()) {
                    return ReaderUtil.readText(reader);
                }
            } else if (obj instanceof final NClob value) {
                try (final Reader reader = value.getCharacterStream()) {
                    return ReaderUtil.readText(reader);
                }
            } else if (obj instanceof final Ref value) {
                return value.getObject().toString();
            } else if (obj instanceof final InputStream value) {
                try {
                    final FessConfig fessConfig = ComponentUtil.getFessConfig();
                    final ExtractorBuilder builder = ComponentUtil.getExtractorFactory().builder(value, null);
                    if (paramMap.get(COLUMN_LABEL_PREFIX + fessConfig.getIndexFieldMimetype()) instanceof final String mimetypeField
                            && paramMap.get(mimetypeField) instanceof final String mimetype) {
                        builder.mimeType(mimetype);
                    } else if (paramMap.get(COLUMN_LABEL_PREFIX + fessConfig.getIndexFieldFilename()) instanceof final String filenameField
                            && paramMap.get(filenameField) instanceof final String filename) {
                        builder.filename(filename);
                    } else if (paramMap.get(DEFAULT_MIMETYPE) instanceof final String defaultMimetype) {
                        builder.mimeType(defaultMimetype);
                    }
                    return builder.extract().getContent();
                } finally {
                    IOUtils.closeQuietly(value);
                }
            } else if (obj instanceof final Reader value) {
                try {
                    return ReaderUtil.readText(value);
                } finally {
                    IOUtils.closeQuietly(value);
                }
            } else if (obj instanceof final Array value) {
                final ResultSet subResultSet = value.getResultSet();
                final StringBuilder buf = new StringBuilder();
                for (int i = 0; i < subResultSet.getMetaData().getColumnCount(); i++) {
                    buf.append(subResultSet.getString(i + 1)).append(' ');
                }
                return buf.toString().trim();
            } else if (obj == null) {
                return StringUtil.EMPTY;
            }
            return obj.toString();
        }

        @Override
        public void clear() {
            paramMap.clear();
        }

        @Override
        public boolean containsKey(final Object key) {
            return paramMap.containsKey(key);
        }

        @Override
        public boolean containsValue(final Object value) {
            return paramMap.containsValue(value);
        }

        @Override
        public Set<java.util.Map.Entry<String, Object>> entrySet() {
            return paramMap.entrySet();
        }

        @Override
        public Object get(final Object key) {
            return paramMap.get(key);
        }

        @Override
        public boolean isEmpty() {
            return paramMap.isEmpty();
        }

        @Override
        public Set<String> keySet() {
            return paramMap.keySet();
        }

        @Override
        public Object put(final String key, final Object value) {
            return paramMap.put(key, value);
        }

        @Override
        public void putAll(final Map<? extends String, ? extends Object> m) {
            paramMap.putAll(m);
        }

        @Override
        public Object remove(final Object key) {
            return paramMap.remove(key);
        }

        @Override
        public int size() {
            return paramMap.size();
        }

        @Override
        public Collection<Object> values() {
            return paramMap.values();
        }

        @Override
        public String toString() {
            return paramMap.toString();
        }

    }

}
