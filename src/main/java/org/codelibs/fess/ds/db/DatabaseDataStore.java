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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
import org.codelibs.fess.exception.DataStoreCrawlingException;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.exception.FessSystemException;
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsAction;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsKeyObject;
import org.codelibs.fess.mylasta.direction.FessConfig;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;

/**
 * Database Data Store implementation for Fess.
 * This data store enables crawling and indexing data from database sources via JDBC connections.
 * It supports various database types and column data types including BLOBs, CLOBs, and binary data
 * with automatic content extraction for search indexing.
 */
public class DatabaseDataStore extends AbstractDataStore {
    private static final Logger logger = LogManager.getLogger(DatabaseDataStore.class);

    /**
     * Default constructor.
     */
    public DatabaseDataStore() {
        super();
    }

    private static final String SQL_PARAM = "sql";

    private static final String URL_PARAM = "url";

    private static final String PASSWORD_PARAM = "password";

    private static final String USERNAME_PARAM = "username";

    private static final String DRIVER_PARAM = "driver";

    private static final String FETCH_SIZE_PARAM = "fetch_size";

    private static final String QUERY_TIMEOUT_PARAM = "query_timeout";

    /** Requests the MySQL row-by-row streaming mode. */
    private static final String MIN_VALUE_PARAM_VALUE = "MIN_VALUE";

    private static final String DEFAULT_MIMETYPE = "default_mimetype";

    private static final String INFO_PREFIX = "info.";

    private static final String COLUMN_LABEL_PREFIX = "column_label.";

    /** A {@code key=value} pair in the query part of a JDBC URL. */
    private static final Pattern URL_PROPERTY_PATTERN = Pattern.compile("([?&;])([^=&;\\s]+)=([^&;\\s]*)");

    @Override
    protected String getName() {
        return this.getClass().getSimpleName();
    }

    /**
     * Retrieves the JDBC driver class name from the parameter map.
     *
     * @param paramMap the parameter map containing configuration
     * @return the JDBC driver class name
     * @throws DataStoreException if the driver parameter is null, empty, or blank
     */
    protected String getDriverClass(final DataStoreParams paramMap) {
        final String driverName = paramMap.getAsString(DRIVER_PARAM);
        if (StringUtil.isBlank(driverName)) {
            throw new DataStoreException("The " + DRIVER_PARAM + " parameter is required.");
        }
        return driverName;
    }

    /**
     * Retrieves the database username from the parameter map.
     *
     * @param paramMap the parameter map containing configuration
     * @return the database username, or null if not specified
     */
    protected String getUsername(final DataStoreParams paramMap) {
        return paramMap.getAsString(USERNAME_PARAM);
    }

    /**
     * Retrieves the database password from the parameter map.
     *
     * @param paramMap the parameter map containing configuration
     * @return the database password, or null if not specified
     */
    protected String getPassword(final DataStoreParams paramMap) {
        return paramMap.getAsString(PASSWORD_PARAM);
    }

    /**
     * Retrieves the database URL from the parameter map.
     *
     * @param paramMap the parameter map containing configuration
     * @return the database URL
     * @throws DataStoreException if the url parameter is null, empty, or blank
     */
    protected String getUrl(final DataStoreParams paramMap) {
        final String url = paramMap.getAsString(URL_PARAM);
        if (StringUtil.isBlank(url)) {
            // Without this the driver decides what a missing URL means, which ranges
            // from a NullPointerException to "No suitable driver".
            throw new DataStoreException("The " + URL_PARAM + " parameter is required.");
        }
        return url;
    }

    /**
     * Retrieves the fetch size from the parameter map.
     *
     * <p>
     * The literal {@code MIN_VALUE}, and the number it stands for, request the
     * MySQL idiom of streaming the result set row by row. Any other negative
     * value is a mistake - {@code Statement#setFetchSize} rejects it - so it is
     * reported and ignored rather than passed on.
     * </p>
     *
     * @param paramMap the parameter map containing configuration
     * @return the fetch size, or null when unset or unusable
     */
    protected Integer getFetchSize(final DataStoreParams paramMap) {
        final String value = paramMap.getAsString(FETCH_SIZE_PARAM);
        if (StringUtil.isBlank(value)) {
            return null;
        }
        final String trimmedValue = value.trim();
        if (MIN_VALUE_PARAM_VALUE.equals(trimmedValue)) {
            return Integer.MIN_VALUE;
        }

        try {
            final int fetchSize = Integer.parseInt(trimmedValue);
            if (fetchSize == Integer.MIN_VALUE) {
                // The same MySQL idiom, written as a number rather than as MIN_VALUE.
                return Integer.MIN_VALUE;
            }
            if (fetchSize < 0) {
                logger.warn("{}={} is negative and will be ignored. Use {} to stream rows one by one on MySQL.", FETCH_SIZE_PARAM,
                        trimmedValue, MIN_VALUE_PARAM_VALUE);
                return null;
            }
            return fetchSize;
        } catch (final NumberFormatException e) {
            logger.warn("{}={} is not a number and will be ignored.", FETCH_SIZE_PARAM, trimmedValue, e);
            return null;
        }
    }

    /**
     * Retrieves the query timeout, in seconds, from the parameter map.
     *
     * <p>
     * Without one, a query that never returns holds the crawler thread forever:
     * the data store only checks whether it should stop between rows, so
     * stopping the job cannot interrupt a call that is blocked inside the
     * driver. Zero means no limit, which is the JDBC default.
     * </p>
     *
     * @param paramMap the parameter map containing configuration
     * @return the timeout in seconds, or null when unset or unusable
     */
    protected Integer getQueryTimeout(final DataStoreParams paramMap) {
        final String value = paramMap.getAsString(QUERY_TIMEOUT_PARAM);
        if (StringUtil.isBlank(value)) {
            return null;
        }
        final String trimmedValue = value.trim();
        try {
            final int queryTimeout = Integer.parseInt(trimmedValue);
            if (queryTimeout < 0) {
                logger.warn("{}={} is negative and will be ignored. Use 0 for no limit.", QUERY_TIMEOUT_PARAM, trimmedValue);
                return null;
            }
            return queryTimeout;
        } catch (final NumberFormatException e) {
            logger.warn("{}={} is not a number and will be ignored.", QUERY_TIMEOUT_PARAM, trimmedValue, e);
            return null;
        }
    }

    /**
     * Retrieves the SQL query from the parameter map.
     *
     * @param paramMap the parameter map containing configuration
     * @return the SQL query string
     * @throws DataStoreException if the SQL parameter is null, empty, or blank
     */
    protected String getSql(final DataStoreParams paramMap) {
        final String sql = paramMap.getAsString(SQL_PARAM);
        if (StringUtil.isBlank(sql)) {
            throw new DataStoreException("The " + SQL_PARAM + " parameter is required.");
        }
        return sql;
    }

    /**
     * Replacement for a value that must not reach the log.
     */
    protected static final String MASKED = "****";

    /**
     * Whether a parameter or connection property name denotes a secret.
     *
     * <p>
     * The same pattern the admin UI uses to decide which data store parameters to
     * encrypt at rest ({@code app.encrypt.property.pattern}), so a value stored as
     * a secret is also treated as one when logging.
     * </p>
     *
     * @param name the parameter or property name
     * @return true if the value must be masked
     */
    protected static boolean isSensitiveName(final String name) {
        return name != null && name.matches(ComponentUtil.getFessConfig().getAppEncryptPropertyPattern());
    }

    /**
     * Hides credentials embedded in a JDBC URL.
     *
     * <p>
     * Both forms occur in practice: {@code //user:password@host} and a query
     * parameter such as {@code ?password=secret}. A URL is otherwise useful in a
     * log, so the rest is left intact.
     * </p>
     *
     * @param url the JDBC URL, may be null
     * @return the URL with any credential replaced
     */
    protected static String maskUrl(final String url) {
        if (url == null) {
            return null;
        }
        final String withoutUserInfo = url.replaceAll("//[^/@\\s]*:[^/@\\s]*@", "//" + MASKED + ":" + MASKED + "@");
        final Matcher matcher = URL_PROPERTY_PATTERN.matcher(withoutUserInfo);
        final StringBuilder buf = new StringBuilder(withoutUserInfo.length());
        while (matcher.find()) {
            matcher.appendReplacement(buf, isSensitiveName(matcher.group(2)) ? matcher.group(1) + matcher.group(2) + "=" + MASKED
                    : Matcher.quoteReplacement(matcher.group()));
        }
        matcher.appendTail(buf);
        return buf.toString();
    }

    @Override
    protected void storeData(final DataConfig config, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap) {

        final CrawlerStatsHelper crawlerStatsHelper = ComponentUtil.getCrawlerStatsHelper();
        final FessConfig fessConfig = ComponentUtil.getFessConfig();
        final long readInterval = getReadInterval(paramMap);
        final String scriptType = getScriptType(paramMap);

        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            final String driverClass = getDriverClass(paramMap);
            try {
                Class.forName(driverClass);
            } catch (final ClassNotFoundException e) {
                throw new DataStoreException("The JDBC driver " + driverClass
                        + " is not on the crawler classpath. Deploy the driver jar to WEB-INF/lib or WEB-INF/env/crawler/lib.", e);
            }

            try {
                con = getConnection(paramMap);
            } catch (final SQLException e) {
                throw new DataStoreException("Failed to connect to " + maskUrl(getUrl(paramMap)) + ".", e);
            }

            final String sql = getSql(paramMap);
            final Integer fetchSize = getFetchSize(paramMap);
            final Integer queryTimeout = getQueryTimeout(paramMap);
            if (logger.isDebugEnabled()) {
                logger.debug("sql: {}, fetch_size: {}, query_timeout: {}", sql, fetchSize, queryTimeout);
            }
            stmt = con.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
            if (queryTimeout != null) {
                try {
                    stmt.setQueryTimeout(queryTimeout);
                } catch (final SQLException e) {
                    // Not every driver supports it. Say so rather than refusing to crawl,
                    // but do not pretend the query is bounded.
                    logger.warn("{}={} was rejected by the driver. The query is not bounded by a timeout.", QUERY_TIMEOUT_PARAM,
                            queryTimeout, e);
                }
            }
            if (fetchSize != null) {
                try {
                    stmt.setFetchSize(fetchSize);
                } catch (final SQLException e) {
                    // The fetch size is a tuning hint, and it is driver specific: MIN_VALUE
                    // means row-by-row streaming on MySQL and is rejected outright by
                    // PostgreSQL. Losing the whole crawl over it would be out of all
                    // proportion, so carry on with the driver default.
                    logger.warn("{}={} was rejected by the driver. Crawling with the driver default instead.", FETCH_SIZE_PARAM, fetchSize,
                            e);
                }
            }
            try {
                rs = stmt.executeQuery(sql); // SQL generated by an administrator
            } catch (final SQLException e) {
                throw new DataStoreException("Failed to execute the query.", e);
            }
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

                    if (dataMap.get(fessConfig.getIndexFieldUrl()) instanceof final String url) {
                        statsKey.setUrl(url);
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
                        url = getFailureUrl(config, dataMap, rs);
                    }
                    final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
                    failureUrlService.store(config, errorName, url, target);
                    crawlerStatsHelper.record(statsKey, StatsAction.ACCESS_EXCEPTION);
                } catch (final Throwable t) {
                    logger.warn("Crawling Access Exception at : {}", dataMap, t);
                    final String url = getFailureUrl(config, dataMap, rs);
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
        } catch (final DataStoreException e) {
            // Already carries a message that says what went wrong. Relabelling it as a
            // generic crawl failure is how a missing parameter used to become
            // indistinguishable from a broken query.
            throw e;
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

    /**
     * Identifies a row that failed, for the failure URL list.
     *
     * <p>
     * The document URL is used when the scripts got far enough to produce one,
     * which is what makes the entry recognisable in the admin UI. Otherwise the
     * row is identified by data configuration and row number. The query itself is
     * deliberately not used: it is unbounded in length for a keyword field, it
     * repeats identically for every failed row, and it can carry literals that do
     * not belong in a stored record.
     * </p>
     *
     * @param config the data configuration
     * @param dataMap the document built so far
     * @param resultSet the result set positioned on the failed row
     * @return a URL identifying the failed row
     */
    protected String getFailureUrl(final DataConfig config, final Map<String, Object> dataMap, final ResultSet resultSet) {
        if (dataMap.get(ComponentUtil.getFessConfig().getIndexFieldUrl()) instanceof final String url && StringUtil.isNotBlank(url)) {
            return url;
        }
        String row = "unknown";
        try {
            row = Integer.toString(resultSet.getRow());
        } catch (final SQLException e) {
            // getRow() fails on a broken connection, which is exactly when a row fails.
            // Losing the row number must not cost the failure record itself.
            logger.debug("Failed to get the current row number.", e);
        }
        return "datastore://" + config.getId() + "/" + row;
    }

    /**
     * Creates a database connection using the parameters specified in the parameter map.
     * Supports connection properties with "info." prefix for additional JDBC connection properties.
     *
     * @param paramMap the parameter map containing database connection configuration
     * @return a database connection
     * @throws SQLException if a database access error occurs
     */
    protected Connection getConnection(final DataStoreParams paramMap) throws SQLException {
        final String jdbcUrl = getUrl(paramMap);

        final String username = getUsername(paramMap);
        if (logger.isDebugEnabled()) {
            logger.debug("jdbc: {} : {}", maskUrl(jdbcUrl), username);
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
                    logger.debug("jdbc: info: {}={}", k, isSensitiveName(k) ? MASKED : v);
                }
                info.put(k, v);
            }
        }

        return DriverManager.getConnection(jdbcUrl, info);
    }

    /**
     * A Map implementation that wraps ResultSet data for script processing.
     * This class provides access to database column values and metadata, making them available
     * for script evaluation during the data extraction process.
     */
    protected static class ResultSetParamMap implements Map<String, Object> {
        private final Map<String, Object> paramMap = new HashMap<>();

        /**
         * Constructor that initializes the parameter map with ResultSet data.
         *
         * @param config the data configuration
         * @param crawlingContext the crawling context
         * @param resultSet the database result set
         * @param paramMap the data store parameters
         */
        public ResultSetParamMap(final DataConfig config, final Map<String, Object> crawlingContext, final ResultSet resultSet,
                final DataStoreParams paramMap) {
            this.paramMap.putAll(paramMap.asMap());
            this.paramMap.put("crawlingConfig", config);
            this.paramMap.put("crawlingContext", crawlingContext);

            final String[] labels;
            try {
                final ResultSetMetaData metaData = resultSet.getMetaData();
                labels = new String[metaData.getColumnCount()];
                for (int i = 0; i < labels.length; i++) {
                    labels[i] = metaData.getColumnLabel(i + 1);
                }
            } catch (final SQLException e) {
                // Only genuine meta data failures reach here. A failure while converting
                // a column must not be relabelled as one, or the reported cause is a lie.
                throw new FessSystemException("Failed to access meta data.", e);
            }

            // The extractor hints have to be resolved before any large object is
            // converted. Reading them lazily made a hint apply only when its column
            // happened to precede the large object in the SELECT list, so reordering the
            // select list silently changed which extractor ran.
            final Set<Integer> resolved = new HashSet<>();
            for (final String hintColumn : hintColumnLabels()) {
                for (int i = 0; i < labels.length; i++) {
                    if (hintColumn.equals(labels[i])) {
                        if (resolved.add(i)) {
                            readColumnInto(resultSet, labels, i);
                        }
                        break;
                    }
                }
            }

            for (int i = 0; i < labels.length; i++) {
                // Each column is read exactly once: the hint columns were read above.
                if (!resolved.contains(i)) {
                    readColumnInto(resultSet, labels, i);
                }
            }
        }

        /**
         * Reads one column and publishes it under its label.
         *
         * <p>
         * A column that cannot be read is dropped with a warning so the rest of the
         * row stays usable. Extraction failures are deliberately not caught here: they
         * mean the document has no content, which the caller records as a failed row
         * rather than indexing an empty document.
         * </p>
         *
         * @param resultSet the database result set
         * @param labels the column labels of the result set
         * @param index the zero-based column position
         */
        protected void readColumnInto(final ResultSet resultSet, final String[] labels, final int index) {
            try {
                paramMap.put(labels[index], getColumnValue(resultSet, index + 1));
            } catch (final IOException | SQLException e) {
                logger.warn("Failed to parse data in a result set. The column is {}.", labels[index], e);
            }
        }

        /**
         * The column labels named by the extractor hint parameters, if any.
         *
         * @return the labels of the mimetype and filename columns
         */
        protected List<String> hintColumnLabels() {
            final FessConfig fessConfig = ComponentUtil.getFessConfig();
            final List<String> hintLabels = new ArrayList<>(2);
            if (paramMap.get(COLUMN_LABEL_PREFIX + fessConfig.getIndexFieldMimetype()) instanceof final String mimetypeField) {
                hintLabels.add(mimetypeField);
            }
            if (paramMap.get(COLUMN_LABEL_PREFIX + fessConfig.getIndexFieldFilename()) instanceof final String filenameField) {
                hintLabels.add(filenameField);
            }
            return hintLabels;
        }

        /**
         * Extracts text from binary content, applying whichever type hint is configured.
         *
         * @param in the binary content
         * @return the extracted text
         */
        protected String extractContent(final InputStream in) {
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

        /**
         * Extracts and converts a column value from the ResultSet to a String.
         * Handles various data types including BLOBs, CLOBs, binary data, and arrays.
         *
         * @param resultSet the database result set
         * @param columnIndex the column index (1-based)
         * @return the column value as a String
         * @throws IOException if an I/O error occurs during data extraction
         * @throws SQLException if a database access error occurs
         */
        protected String getColumnValue(final ResultSet resultSet, final int columnIndex) throws IOException, SQLException {
            final Object obj = resultSet.getObject(columnIndex);
            if (obj instanceof final Blob value) {
                try (final InputStream in = value.getBinaryStream()) {
                    return extractContent(in);
                }
            }
            if (obj instanceof final byte[] value) {
                // Which of these two branches a binary column takes is decided by the
                // driver, not by the schema: H2 hands back a Blob while MySQL and
                // PostgreSQL hand back a byte array. Both have to extract, otherwise the
                // same table yields different content depending on the driver.
                try (final InputStream in = new ByteArrayInputStream(value)) {
                    return extractContent(in);
                }
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
                    return extractContent(value);
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
                // Array#getResultSet() yields one row per element, with the index in
                // column 1 and the value in column 2. The rows have to be stepped
                // through: reading before the first next() throws, and the column count
                // is 2 however many elements the array holds.
                try (final ResultSet subResultSet = value.getResultSet()) {
                    final StringBuilder buf = new StringBuilder();
                    while (subResultSet.next()) {
                        final String element = subResultSet.getString(2);
                        if (element != null) {
                            if (buf.length() > 0) {
                                buf.append(' ');
                            }
                            buf.append(element);
                        }
                    }
                    return buf.toString();
                }
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

        /**
         * Renders the map with secrets masked.
         *
         * <p>
         * The whole data store parameter map is copied in here so scripts can see
         * it, credentials included, and this map is logged once per row at DEBUG.
         * Without masking, turning on debug logging writes the database password to
         * the log as many times as there are rows.
         * </p>
         */
        @Override
        public String toString() {
            return paramMap.entrySet()
                    .stream()
                    .map(entry -> entry.getKey() + "=" + (isSensitiveName(entry.getKey()) ? MASKED : entry.getValue()))
                    .collect(Collectors.joining(", ", "{", "}"));
        }

    }

}
