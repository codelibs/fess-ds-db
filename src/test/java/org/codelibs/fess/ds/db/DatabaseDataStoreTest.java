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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.util.ComponentUtil;
import org.codelibs.fess.ds.db.UnitDsTestCase;

public class DatabaseDataStoreTest extends UnitDsTestCase {
    private static final java.util.concurrent.atomic.AtomicInteger DB_SEQ = new java.util.concurrent.atomic.AtomicInteger();

    public DatabaseDataStore dataStore;

    /**
     * A fresh in-memory database name per call. H2 fixes the credentials on the
     * connection that creates the database, so sharing one name across tests
     * makes them depend on execution order.
     */
    private String newJdbcUrl() {
        return "jdbc:h2:mem:conn" + DB_SEQ.incrementAndGet();
    }

    @Override
    protected String prepareConfigFile() {
        return "test_app.xml";
    }

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    @Override
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        dataStore = new DatabaseDataStore();
    }

    @Override
    public void tearDown(TestInfo testInfo) throws Exception {
        ComponentUtil.setFessConfig(null);
        super.tearDown(testInfo);
    }

    @Test
    public void test_getDriverClass_validDriver() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("driver", "org.h2.Driver");

        final String result = dataStore.getDriverClass(paramMap);
        assertEquals("org.h2.Driver", result);
    }

    @Test
    public void test_getDriverClass_nullDriver() {
        final DataStoreParams paramMap = new DataStoreParams();

        try {
            dataStore.getDriverClass(paramMap);
            fail("Should throw DataStoreException");
        } catch (final DataStoreException e) {
            assertEquals("The driver parameter is required.", e.getMessage());
        }
    }

    @Test
    public void test_getDriverClass_emptyDriver() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("driver", "");

        try {
            dataStore.getDriverClass(paramMap);
            fail("Should throw DataStoreException");
        } catch (final DataStoreException e) {
            assertEquals("The driver parameter is required.", e.getMessage());
        }
    }

    @Test
    public void test_getDriverClass_blankDriver() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("driver", "   ");

        try {
            dataStore.getDriverClass(paramMap);
            fail("Should throw DataStoreException");
        } catch (final DataStoreException e) {
            assertEquals("The driver parameter is required.", e.getMessage());
        }
    }

    @Test
    public void test_getSql_validSql() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("sql", "SELECT * FROM users");

        final String result = dataStore.getSql(paramMap);
        assertEquals("SELECT * FROM users", result);
    }

    @Test
    public void test_getSql_nullSql() {
        final DataStoreParams paramMap = new DataStoreParams();

        try {
            dataStore.getSql(paramMap);
            fail("Should throw DataStoreException");
        } catch (final DataStoreException e) {
            assertEquals("The sql parameter is required.", e.getMessage());
        }
    }

    @Test
    public void test_getSql_emptySql() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("sql", "");

        try {
            dataStore.getSql(paramMap);
            fail("Should throw DataStoreException");
        } catch (final DataStoreException e) {
            assertEquals("The sql parameter is required.", e.getMessage());
        }
    }

    @Test
    public void test_getSql_blankSql() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("sql", "   ");

        try {
            dataStore.getSql(paramMap);
            fail("Should throw DataStoreException");
        } catch (final DataStoreException e) {
            assertEquals("The sql parameter is required.", e.getMessage());
        }
    }

    @Test
    public void test_getUsername_validUsername() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("username", "testuser");

        final String result = dataStore.getUsername(paramMap);
        assertEquals("testuser", result);
    }

    @Test
    public void test_getUsername_nullUsername() {
        final DataStoreParams paramMap = new DataStoreParams();

        final String result = dataStore.getUsername(paramMap);
        assertNull(result);
    }

    @Test
    public void test_getUsername_emptyUsername() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("username", "");

        final String result = dataStore.getUsername(paramMap);
        assertEquals("", result);
    }

    @Test
    public void test_getPassword_validPassword() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("password", "secret123");

        final String result = dataStore.getPassword(paramMap);
        assertEquals("secret123", result);
    }

    @Test
    public void test_getPassword_nullPassword() {
        final DataStoreParams paramMap = new DataStoreParams();

        final String result = dataStore.getPassword(paramMap);
        assertNull(result);
    }

    @Test
    public void test_getPassword_emptyPassword() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("password", "");

        final String result = dataStore.getPassword(paramMap);
        assertEquals("", result);
    }

    @Test
    public void test_getUrl_validUrl() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("url", "jdbc:h2:mem:test");

        final String result = dataStore.getUrl(paramMap);
        assertEquals("jdbc:h2:mem:test", result);
    }

    @Test
    public void test_getUrl_nullUrl() {
        final DataStoreParams paramMap = new DataStoreParams();

        try {
            dataStore.getUrl(paramMap);
            fail("Should throw DataStoreException");
        } catch (final DataStoreException e) {
            assertEquals("The url parameter is required.", e.getMessage());
        }
    }

    @Test
    public void test_getUrl_emptyUrl() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("url", "");

        try {
            dataStore.getUrl(paramMap);
            fail("Should throw DataStoreException");
        } catch (final DataStoreException e) {
            assertEquals("The url parameter is required.", e.getMessage());
        }
    }

    @Test
    public void test_getFetchSize_validNumber() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("fetch_size", "1000");

        final Integer result = dataStore.getFetchSize(paramMap);
        assertEquals(Integer.valueOf(1000), result);
    }

    @Test
    public void test_getFetchSize_minValue() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("fetch_size", "MIN_VALUE");

        final Integer result = dataStore.getFetchSize(paramMap);
        assertEquals(Integer.valueOf(Integer.MIN_VALUE), result);
    }

    @Test
    public void test_getFetchSize_zero() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("fetch_size", "0");

        final Integer result = dataStore.getFetchSize(paramMap);
        assertEquals(Integer.valueOf(0), result);
    }

    @Test
    public void test_getFetchSize_negativeNumber() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("fetch_size", "-100");

        // Statement#setFetchSize rejects negative values, so passing one on would end
        // the crawl. Only Integer.MIN_VALUE has a meaning, and it has its own test.
        assertNull(dataStore.getFetchSize(paramMap));
    }

    @Test
    public void test_getFetchSize_nullValue() {
        final DataStoreParams paramMap = new DataStoreParams();

        final Integer result = dataStore.getFetchSize(paramMap);
        assertNull(result);
    }

    @Test
    public void test_getFetchSize_emptyValue() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("fetch_size", "");

        final Integer result = dataStore.getFetchSize(paramMap);
        assertNull(result);
    }

    @Test
    public void test_getFetchSize_blankValue() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("fetch_size", "   ");

        final Integer result = dataStore.getFetchSize(paramMap);
        assertNull(result);
    }

    @Test
    public void test_getFetchSize_invalidNumber() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("fetch_size", "abc123");

        final Integer result = dataStore.getFetchSize(paramMap);
        assertNull(result);
    }

    @Test
    public void test_getFetchSize_invalidNumberFormat() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("fetch_size", "12.34");

        final Integer result = dataStore.getFetchSize(paramMap);
        assertNull(result);
    }

    @Test
    public void test_getFetchSize_maxValue() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("fetch_size", String.valueOf(Integer.MAX_VALUE));

        final Integer result = dataStore.getFetchSize(paramMap);
        assertEquals(Integer.valueOf(Integer.MAX_VALUE), result);
    }

    @Test
    public void test_getQueryTimeout_validValues() {
        final DataStoreParams paramMap = new DataStoreParams();

        paramMap.put("query_timeout", "30");
        assertEquals(Integer.valueOf(30), dataStore.getQueryTimeout(paramMap));

        // Zero is the JDBC default and means no limit, so it is passed through.
        paramMap.put("query_timeout", "0");
        assertEquals(Integer.valueOf(0), dataStore.getQueryTimeout(paramMap));

        paramMap.put("query_timeout", "  30  ");
        assertEquals(Integer.valueOf(30), dataStore.getQueryTimeout(paramMap));
    }

    @Test
    public void test_getQueryTimeout_unusableValues() {
        final DataStoreParams paramMap = new DataStoreParams();
        assertNull(dataStore.getQueryTimeout(paramMap));

        paramMap.put("query_timeout", "");
        assertNull(dataStore.getQueryTimeout(paramMap));

        // setQueryTimeout rejects a negative value, so it is reported and dropped.
        paramMap.put("query_timeout", "-1");
        assertNull(dataStore.getQueryTimeout(paramMap));

        paramMap.put("query_timeout", "30s");
        assertNull(dataStore.getQueryTimeout(paramMap));
    }

    @Test
    public void test_maskUrl_hidesUserInfo() {
        assertEquals("jdbc:mysql://****:****@db.example.com:3306/app",
                DatabaseDataStore.maskUrl("jdbc:mysql://scott:tiger@db.example.com:3306/app"));
    }

    @Test
    public void test_maskUrl_hidesSensitiveQueryParameters() {
        // The user name is worth keeping in a log; the password is not.
        assertEquals("jdbc:mysql://db.example.com/app?user=scott&password=****&useSSL=true",
                DatabaseDataStore.maskUrl("jdbc:mysql://db.example.com/app?user=scott&password=tiger&useSSL=true"));
    }

    @Test
    public void test_maskUrl_leavesAnOrdinaryUrlAlone() {
        assertEquals("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1", DatabaseDataStore.maskUrl("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1"));
        assertNull(DatabaseDataStore.maskUrl(null));
    }

    @Test
    public void test_isSensitiveName_followsTheEncryptedPropertyPattern() {
        assertTrue(DatabaseDataStore.isSensitiveName("password"));
        assertTrue(DatabaseDataStore.isSensitiveName("info.password"));
        assertTrue(DatabaseDataStore.isSensitiveName("api_token"));
        assertFalse(DatabaseDataStore.isSensitiveName("username"));
        assertFalse(DatabaseDataStore.isSensitiveName("url"));
    }

    @Test
    public void test_getName() {
        final String result = dataStore.getName();
        assertEquals("DatabaseDataStore", result);
    }

    @Test
    public void test_getConnection_invalidUrl() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("url", "invalid_url");

        try {
            dataStore.getConnection(paramMap);
            fail("Should throw SQLException");
        } catch (final SQLException e) {
            // Expected - invalid URL should cause SQLException
            assertNotNull(e);
        }
    }

    @Test
    public void test_getConnection_passesInfoParametersToTheDriver() throws Exception {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("url", newJdbcUrl());
        paramMap.put("username", "testuser");
        paramMap.put("password", "testpass");
        // INIT is an H2 connection setting that runs SQL as the connection opens.
        // Observing its effect is what proves the "info." prefixed parameters reach
        // the driver rather than being dropped.
        paramMap.put("info.INIT", "CREATE TABLE info_probe (id INT)");

        try (Connection conn = dataStore.getConnection(paramMap); java.sql.Statement stmt = conn.createStatement()) {
            try (java.sql.ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM info_probe")) {
                assertTrue(rs.next());
            }
        }
    }

    @Test
    public void test_getConnection_withCredentials() throws Exception {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("url", newJdbcUrl());
        paramMap.put("username", "testuser");
        paramMap.put("password", "testpass");

        try (Connection conn = dataStore.getConnection(paramMap)) {
            assertNotNull(conn);
            assertTrue(conn.isValid(5));
        }
    }

    @Test
    public void test_getConnection_nullUrl() throws Exception {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("username", "testuser");
        paramMap.put("password", "testpass");

        // A missing URL is a configuration mistake, reported as such rather than left
        // to the driver to turn into "No suitable driver" or a NullPointerException.
        try {
            dataStore.getConnection(paramMap);
            fail("Should throw DataStoreException");
        } catch (final DataStoreException e) {
            assertEquals("The url parameter is required.", e.getMessage());
        }
    }

    @Test
    public void test_getConnection_emptyUrl() throws Exception {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("url", "");
        paramMap.put("username", "testuser");
        paramMap.put("password", "testpass");

        try {
            dataStore.getConnection(paramMap);
            fail("Should throw DataStoreException");
        } catch (final DataStoreException e) {
            assertEquals("The url parameter is required.", e.getMessage());
        }
    }

    @Test
    public void test_getConnection_withoutCredentials() throws Exception {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("url", newJdbcUrl());

        // Neither user nor password is put into the Properties, so the driver sees
        // an empty credential set.
        try (Connection conn = dataStore.getConnection(paramMap)) {
            assertNotNull(conn);
            assertTrue(conn.isValid(5));
        }
    }

    /**
     * Pins the public parameter names. These keys are part of the data store
     * configuration contract, so renaming one silently breaks every existing
     * data config. Keys without a getter (default_mimetype, info., column_label.)
     * are covered by the storeData tests.
     */
    @Test
    public void test_parameter_names_areStable() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("driver", "org.h2.Driver");
        paramMap.put("url", "jdbc:h2:mem:contract");
        paramMap.put("username", "scott");
        paramMap.put("password", "tiger");
        paramMap.put("sql", "SELECT 1");
        paramMap.put("fetch_size", "500");

        assertEquals("org.h2.Driver", dataStore.getDriverClass(paramMap));
        assertEquals("jdbc:h2:mem:contract", dataStore.getUrl(paramMap));
        assertEquals("scott", dataStore.getUsername(paramMap));
        assertEquals("tiger", dataStore.getPassword(paramMap));
        assertEquals("SELECT 1", dataStore.getSql(paramMap));
        assertEquals(Integer.valueOf(500), dataStore.getFetchSize(paramMap));
    }

    @Test
    public void test_getDriverClass_withSpecialCharacters() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("driver", "com.mysql.cj.jdbc.Driver");

        final String result = dataStore.getDriverClass(paramMap);
        assertEquals("com.mysql.cj.jdbc.Driver", result);
    }

    @Test
    public void test_getSql_withComplexQuery() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("sql", "SELECT id, name, email FROM users WHERE created_at > '2023-01-01' ORDER BY id");

        final String result = dataStore.getSql(paramMap);
        assertEquals("SELECT id, name, email FROM users WHERE created_at > '2023-01-01' ORDER BY id", result);
    }

    @Test
    public void test_getFetchSize_boundaryValues() {
        final DataStoreParams paramMap = new DataStoreParams();

        // Test with 1 (minimum reasonable fetch size)
        paramMap.put("fetch_size", "1");
        Integer result = dataStore.getFetchSize(paramMap);
        assertEquals(Integer.valueOf(1), result);

        // Test with very large number (but still valid)
        paramMap.put("fetch_size", "999999");
        result = dataStore.getFetchSize(paramMap);
        assertEquals(Integer.valueOf(999999), result);
    }

    @Test
    public void test_getFetchSize_leadingTrailingSpaces() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("fetch_size", "  100  ");

        // Surrounding whitespace is trimmed, so a pasted value still works.
        assertEquals(Integer.valueOf(100), dataStore.getFetchSize(paramMap));
    }

    @Test
    public void test_parameterExtraction_withNullDataStoreParams() {
        try {
            dataStore.getDriverClass(null);
            fail("Should throw NullPointerException");
        } catch (final NullPointerException e) {
            // Expected
            assertNotNull(e);
        }
    }

    @Test
    public void test_parameterExtraction_withSpecialValues() {
        final DataStoreParams paramMap = new DataStoreParams();

        // Test with special characters in username/password
        paramMap.put("username", "user@domain.com");
        paramMap.put("password", "pass!@#$%^&*()");

        assertEquals("user@domain.com", dataStore.getUsername(paramMap));
        assertEquals("pass!@#$%^&*()", dataStore.getPassword(paramMap));
    }

    @Test
    public void test_parameterExtraction_withUnicodeValues() {
        final DataStoreParams paramMap = new DataStoreParams();

        // Test with Unicode characters
        paramMap.put("username", "用户名");
        paramMap.put("password", "密码123");

        assertEquals("用户名", dataStore.getUsername(paramMap));
        assertEquals("密码123", dataStore.getPassword(paramMap));
    }

    // ============================================
    // Additional comprehensive test cases
    // ============================================

    /**
     * Test getDriverClass with various driver names
     */
    @Test
    public void test_getDriverClass_withVariousDriverNames() {
        final DataStoreParams paramMap = new DataStoreParams();

        // PostgreSQL driver
        paramMap.put("driver", "org.postgresql.Driver");
        assertEquals("org.postgresql.Driver", dataStore.getDriverClass(paramMap));

        // Oracle driver
        paramMap.put("driver", "oracle.jdbc.driver.OracleDriver");
        assertEquals("oracle.jdbc.driver.OracleDriver", dataStore.getDriverClass(paramMap));

        // SQL Server driver
        paramMap.put("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver");
        assertEquals("com.microsoft.sqlserver.jdbc.SQLServerDriver", dataStore.getDriverClass(paramMap));
    }

    /**
     * Test getSql with SQL containing special characters and line breaks
     */
    @Test
    public void test_getSql_withMultilineQuery() {
        final DataStoreParams paramMap = new DataStoreParams();
        final String multilineSql = "SELECT id, name, email\n" + "FROM users\n" + "WHERE created_at > '2023-01-01'\n" + "ORDER BY id DESC";
        paramMap.put("sql", multilineSql);

        final String result = dataStore.getSql(paramMap);
        assertEquals(multilineSql, result);
    }

    /**
     * Test getSql with SQL containing special database-specific syntax
     */
    @Test
    public void test_getSql_withSpecialSyntax() {
        final DataStoreParams paramMap = new DataStoreParams();

        // SQL with parameters placeholders (though not used in this implementation)
        paramMap.put("sql", "SELECT * FROM users WHERE age > ? AND status = ?");
        assertEquals("SELECT * FROM users WHERE age > ? AND status = ?", dataStore.getSql(paramMap));

        // SQL with joins
        paramMap.put("sql", "SELECT u.*, p.* FROM users u JOIN profiles p ON u.id = p.user_id");
        assertEquals("SELECT u.*, p.* FROM users u JOIN profiles p ON u.id = p.user_id", dataStore.getSql(paramMap));

        // SQL with subqueries
        paramMap.put("sql", "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)");
        assertEquals("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)", dataStore.getSql(paramMap));
    }

    /**
     * Test getUrl with various JDBC URL formats
     */
    @Test
    public void test_getUrl_withVariousFormats() {
        final DataStoreParams paramMap = new DataStoreParams();

        // PostgreSQL URL
        paramMap.put("url", "jdbc:postgresql://localhost:5432/testdb");
        assertEquals("jdbc:postgresql://localhost:5432/testdb", dataStore.getUrl(paramMap));

        // MySQL URL with parameters
        paramMap.put("url", "jdbc:mysql://localhost:3306/testdb?useSSL=false&serverTimezone=UTC");
        assertEquals("jdbc:mysql://localhost:3306/testdb?useSSL=false&serverTimezone=UTC", dataStore.getUrl(paramMap));

        // Oracle URL
        paramMap.put("url", "jdbc:oracle:thin:@localhost:1521:orcl");
        assertEquals("jdbc:oracle:thin:@localhost:1521:orcl", dataStore.getUrl(paramMap));

        // SQL Server URL
        paramMap.put("url", "jdbc:sqlserver://localhost:1433;databaseName=testdb");
        assertEquals("jdbc:sqlserver://localhost:1433;databaseName=testdb", dataStore.getUrl(paramMap));
    }

    /**
     * Test getFetchSize with boundary values near Integer limits
     */
    @Test
    public void test_getFetchSize_boundaryValuesNearLimits() {
        final DataStoreParams paramMap = new DataStoreParams();

        // Integer.MIN_VALUE + 1 is just a negative number: no driver assigns it a
        // meaning, and setFetchSize would reject it.
        paramMap.put("fetch_size", String.valueOf(Integer.MIN_VALUE + 1));
        Integer result = dataStore.getFetchSize(paramMap);
        assertNull(result);

        // Test with Integer.MAX_VALUE - 1
        paramMap.put("fetch_size", String.valueOf(Integer.MAX_VALUE - 1));
        result = dataStore.getFetchSize(paramMap);
        assertEquals(Integer.valueOf(Integer.MAX_VALUE - 1), result);
    }

    /**
     * Test getFetchSize with overflow values (beyond Integer range)
     */
    @Test
    public void test_getFetchSize_withOverflowValues() {
        final DataStoreParams paramMap = new DataStoreParams();

        // Long value that exceeds Integer.MAX_VALUE
        paramMap.put("fetch_size", "9999999999999");
        final Integer result = dataStore.getFetchSize(paramMap);
        // Should return null due to NumberFormatException
        assertNull(result);
    }

    /**
     * Test getFetchSize with various invalid formats
     */
    @Test
    public void test_getFetchSize_withVariousInvalidFormats() {
        final DataStoreParams paramMap = new DataStoreParams();

        // Hexadecimal format
        paramMap.put("fetch_size", "0x100");
        assertNull(dataStore.getFetchSize(paramMap));

        // Scientific notation
        paramMap.put("fetch_size", "1e3");
        assertNull(dataStore.getFetchSize(paramMap));

        // With comma separator
        paramMap.put("fetch_size", "1,000");
        assertNull(dataStore.getFetchSize(paramMap));

        // Mixed alphanumeric
        paramMap.put("fetch_size", "100abc");
        assertNull(dataStore.getFetchSize(paramMap));
    }

    /**
     * Test getUsername and getPassword with empty strings vs null
     */
    @Test
    public void test_credentials_emptyVsNull() {
        final DataStoreParams paramMap = new DataStoreParams();

        // Empty username should return empty string, not null
        paramMap.put("username", "");
        assertEquals("", dataStore.getUsername(paramMap));

        // Empty password should return empty string, not null
        paramMap.put("password", "");
        assertEquals("", dataStore.getPassword(paramMap));

        // Clear and test - getAsString returns empty string when key doesn't exist
        paramMap.asMap().clear();
        assertEquals("", dataStore.getUsername(paramMap));
        assertEquals("", dataStore.getPassword(paramMap));
    }

    /**
     * Test getConnection with various URL formats to ensure proper error handling
     */
    @Test
    public void test_getConnection_withMalformedUrls() {
        final DataStoreParams paramMap = new DataStoreParams();

        // Missing protocol
        paramMap.put("url", "h2:mem:test");
        try {
            dataStore.getConnection(paramMap);
            fail("Should throw SQLException for malformed URL");
        } catch (final SQLException e) {
            assertNotNull(e);
        }

        // Incomplete URL
        paramMap.put("url", "jdbc:");
        try {
            dataStore.getConnection(paramMap);
            fail("Should throw SQLException for incomplete URL");
        } catch (final SQLException e) {
            assertNotNull(e);
        }
    }

    /**
     * Test getConnection with multiple info parameters
     */
    @Test
    public void test_getConnection_withMultipleInfoParameters() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("url", "jdbc:h2:mem:test");
        paramMap.put("username", "sa");
        paramMap.put("password", "");
        paramMap.put("info.connectionTimeout", "5000");
        paramMap.put("info.socketTimeout", "3000");
        paramMap.put("info.useSSL", "false");
        paramMap.put("info.characterEncoding", "UTF-8");

        try {
            final Connection conn = dataStore.getConnection(paramMap);
            // If H2 is available, connection should succeed
            assertNotNull(conn);
            conn.close();
        } catch (final SQLException e) {
            // Expected if H2 driver is not available
            assertTrue(e.getMessage().contains("No suitable driver") || e.getMessage().contains("Driver not found"));
        }
    }

    /**
     * Test getName returns expected class name
     */
    @Test
    public void test_getName_consistency() {
        final String name = dataStore.getName();
        assertEquals("DatabaseDataStore", name);
        assertEquals(dataStore.getClass().getSimpleName(), name);
    }

    /**
     * Test parameter extraction with whitespace-only values
     */
    @Test
    public void test_parameters_withWhitespaceOnly() {
        final DataStoreParams paramMap = new DataStoreParams();

        // Username with only tabs
        paramMap.put("username", "\t\t");
        assertEquals("\t\t", dataStore.getUsername(paramMap));

        // Password with mixed whitespace
        paramMap.put("password", " \t \n ");
        assertEquals(" \t \n ", dataStore.getPassword(paramMap));

        // A whitespace-only URL is not a URL, so it is rejected like a missing one.
        paramMap.put("url", "   ");
        try {
            dataStore.getUrl(paramMap);
            fail("Should throw DataStoreException");
        } catch (final DataStoreException e) {
            assertEquals("The url parameter is required.", e.getMessage());
        }
    }

    /**
     * Test SQL parameter with very long query
     */
    @Test
    public void test_getSql_withVeryLongQuery() {
        final DataStoreParams paramMap = new DataStoreParams();
        final StringBuilder longSql = new StringBuilder("SELECT ");
        for (int i = 0; i < 100; i++) {
            if (i > 0) {
                longSql.append(", ");
            }
            longSql.append("column").append(i);
        }
        longSql.append(" FROM large_table WHERE id > 0");

        paramMap.put("sql", longSql.toString());
        assertEquals(longSql.toString(), dataStore.getSql(paramMap));
        assertTrue(dataStore.getSql(paramMap).length() > 1000);
    }

    /**
     * Test driver class parameter with trailing/leading spaces
     * Note: StringUtil.isBlank only checks for blank strings (null, empty, or whitespace-only),
     * not strings with leading/trailing spaces around non-whitespace content.
     */
    @Test
    public void test_getDriverClass_withSurroundingSpaces() {
        final DataStoreParams paramMap = new DataStoreParams();

        // Driver name with surrounding spaces is accepted (not treated as blank)
        paramMap.put("driver", "  org.h2.Driver  ");
        final String result = dataStore.getDriverClass(paramMap);
        assertEquals("  org.h2.Driver  ", result);
    }

    /**
     * Test getFetchSize with MIN_VALUE case sensitivity
     */
    @Test
    public void test_getFetchSize_minValueCaseSensitivity() {
        final DataStoreParams paramMap = new DataStoreParams();

        // Exact match should work
        paramMap.put("fetch_size", "MIN_VALUE");
        assertEquals(Integer.valueOf(Integer.MIN_VALUE), dataStore.getFetchSize(paramMap));

        // Lowercase should not work - should try to parse as number and fail
        paramMap.put("fetch_size", "min_value");
        assertNull(dataStore.getFetchSize(paramMap));

        // Mixed case should not work
        paramMap.put("fetch_size", "Min_Value");
        assertNull(dataStore.getFetchSize(paramMap));
    }

    /**
     * Test parameter combination scenarios
     */
    @Test
    public void test_parameterCombination_fullConfiguration() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("driver", "org.h2.Driver");
        paramMap.put("url", "jdbc:h2:mem:testdb");
        paramMap.put("username", "sa");
        paramMap.put("password", "");
        paramMap.put("sql", "SELECT * FROM users");
        paramMap.put("fetch_size", "100");

        assertEquals("org.h2.Driver", dataStore.getDriverClass(paramMap));
        assertEquals("jdbc:h2:mem:testdb", dataStore.getUrl(paramMap));
        assertEquals("sa", dataStore.getUsername(paramMap));
        assertEquals("", dataStore.getPassword(paramMap));
        assertEquals("SELECT * FROM users", dataStore.getSql(paramMap));
        assertEquals(Integer.valueOf(100), dataStore.getFetchSize(paramMap));
    }

    /**
     * Test parameter combination scenarios with minimal configuration
     */
    @Test
    public void test_parameterCombination_minimalConfiguration() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("driver", "org.h2.Driver");
        paramMap.put("sql", "SELECT 1");

        paramMap.put("url", "jdbc:h2:mem:minimal");

        assertEquals("org.h2.Driver", dataStore.getDriverClass(paramMap));
        assertEquals("SELECT 1", dataStore.getSql(paramMap));
        assertEquals("jdbc:h2:mem:minimal", dataStore.getUrl(paramMap));
        assertNull(dataStore.getUsername(paramMap));
        assertNull(dataStore.getPassword(paramMap));
        assertNull(dataStore.getFetchSize(paramMap));
    }

    /**
     * Test info parameter prefix filtering
     */
    @Test
    public void test_infoParameters_prefixFiltering() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("url", "jdbc:h2:mem:test");
        paramMap.put("info.ssl", "true");
        paramMap.put("info.timeout", "5000");
        paramMap.put("infoNotAPrefix", "shouldBeIgnored");
        paramMap.put("other", "value");

        try {
            final Connection conn = dataStore.getConnection(paramMap);
            // If connection succeeds, verify it's not null
            assertNotNull(conn);
            conn.close();
        } catch (final SQLException e) {
            // Expected if H2 is not available
            assertTrue(e.getMessage().contains("No suitable driver") || e.getMessage().contains("Driver not found"));
        }
    }

    /**
     * Test getConnection with username but no password
     */
    @Test
    public void test_getConnection_usernameWithoutPassword() throws Exception {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("url", newJdbcUrl());
        paramMap.put("username", "testuser");
        // password is intentionally not set

        try (Connection conn = dataStore.getConnection(paramMap)) {
            assertNotNull(conn);
            assertTrue(conn.isValid(5));
        }
    }

    /**
     * Test getConnection with password but no username
     */
    @Test
    public void test_getConnection_passwordWithoutUsername() throws Exception {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("url", newJdbcUrl());
        paramMap.put("password", "testpass");
        // username is intentionally not set

        try (Connection conn = dataStore.getConnection(paramMap)) {
            assertNotNull(conn);
            assertTrue(conn.isValid(5));
        }
    }

    /**
     * Test getFetchSize with exactly Integer.MIN_VALUE as string
     */
    @Test
    public void test_getFetchSize_exactMinValueString() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("fetch_size", String.valueOf(Integer.MIN_VALUE));

        final Integer result = dataStore.getFetchSize(paramMap);
        assertEquals(Integer.valueOf(Integer.MIN_VALUE), result);
    }

    /**
     * Test SQL injection patterns (ensuring they're passed as-is, not validated)
     * Note: This implementation trusts administrators to write safe SQL
     */
    @Test
    public void test_getSql_withSqlInjectionPatterns() {
        final DataStoreParams paramMap = new DataStoreParams();

        // SQL with comment
        paramMap.put("sql", "SELECT * FROM users -- comment");
        assertEquals("SELECT * FROM users -- comment", dataStore.getSql(paramMap));

        // SQL with union
        paramMap.put("sql", "SELECT * FROM users UNION SELECT * FROM admin");
        assertEquals("SELECT * FROM users UNION SELECT * FROM admin", dataStore.getSql(paramMap));

        // SQL with semicolon
        paramMap.put("sql", "SELECT * FROM users; DROP TABLE users;");
        assertEquals("SELECT * FROM users; DROP TABLE users;", dataStore.getSql(paramMap));
    }

    /**
     * Test parameter retrieval doesn't modify the original map
     */
    @Test
    public void test_parameterRetrieval_doesNotModifyMap() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("driver", "org.h2.Driver");
        paramMap.put("sql", "SELECT 1");
        paramMap.put("username", "user");

        final int originalSize = paramMap.asMap().size();

        dataStore.getDriverClass(paramMap);
        dataStore.getSql(paramMap);
        dataStore.getUsername(paramMap);
        dataStore.getPassword(paramMap); // doesn't exist
        dataStore.getFetchSize(paramMap); // doesn't exist

        assertEquals(originalSize, paramMap.asMap().size());
        assertEquals("org.h2.Driver", paramMap.get("driver"));
        assertEquals("SELECT 1", paramMap.get("sql"));
        assertEquals("user", paramMap.get("username"));
    }

    /**
     * Test URL with special characters
     */
    @Test
    public void test_getUrl_withSpecialCharacters() {
        final DataStoreParams paramMap = new DataStoreParams();

        // URL with encoded characters
        paramMap.put("url", "jdbc:mysql://localhost:3306/test?user=root&password=p@ss%20word");
        assertEquals("jdbc:mysql://localhost:3306/test?user=root&password=p@ss%20word", dataStore.getUrl(paramMap));

        // URL with IPv6 address
        paramMap.put("url", "jdbc:postgresql://[::1]:5432/testdb");
        assertEquals("jdbc:postgresql://[::1]:5432/testdb", dataStore.getUrl(paramMap));
    }

    /**
     * Test credentials with SQL special characters
     */
    @Test
    public void test_credentials_withSqlSpecialCharacters() {
        final DataStoreParams paramMap = new DataStoreParams();

        paramMap.put("username", "user'--");
        paramMap.put("password", "'; DROP TABLE users;--");

        assertEquals("user'--", dataStore.getUsername(paramMap));
        assertEquals("'; DROP TABLE users;--", dataStore.getPassword(paramMap));
    }
}
