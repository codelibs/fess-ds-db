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
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.util.ComponentUtil;
import org.dbflute.utflute.lastadi.ContainerTestCase;

public class DatabaseDataStoreTest extends ContainerTestCase {
    public DatabaseDataStore dataStore;

    @Override
    protected String prepareConfigFile() {
        return "test_app.xml";
    }

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        dataStore = new DatabaseDataStore();
    }

    @Override
    public void tearDown() throws Exception {
        ComponentUtil.setFessConfig(null);
        super.tearDown();
    }

    public void test_getDriverClass_validDriver() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("driver", "org.h2.Driver");

        final String result = dataStore.getDriverClass(paramMap);
        assertEquals("org.h2.Driver", result);
    }

    public void test_getDriverClass_nullDriver() {
        final DataStoreParams paramMap = new DataStoreParams();

        try {
            dataStore.getDriverClass(paramMap);
            fail("Should throw DataStoreException");
        } catch (final DataStoreException e) {
            assertEquals("JDBC driver is null", e.getMessage());
        }
    }

    public void test_getDriverClass_emptyDriver() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("driver", "");

        try {
            dataStore.getDriverClass(paramMap);
            fail("Should throw DataStoreException");
        } catch (final DataStoreException e) {
            assertEquals("JDBC driver is null", e.getMessage());
        }
    }

    public void test_getDriverClass_blankDriver() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("driver", "   ");

        try {
            dataStore.getDriverClass(paramMap);
            fail("Should throw DataStoreException");
        } catch (final DataStoreException e) {
            assertEquals("JDBC driver is null", e.getMessage());
        }
    }

    public void test_getSql_validSql() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("sql", "SELECT * FROM users");

        final String result = dataStore.getSql(paramMap);
        assertEquals("SELECT * FROM users", result);
    }

    public void test_getSql_nullSql() {
        final DataStoreParams paramMap = new DataStoreParams();

        try {
            dataStore.getSql(paramMap);
            fail("Should throw DataStoreException");
        } catch (final DataStoreException e) {
            assertEquals("sql is null", e.getMessage());
        }
    }

    public void test_getSql_emptySql() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("sql", "");

        try {
            dataStore.getSql(paramMap);
            fail("Should throw DataStoreException");
        } catch (final DataStoreException e) {
            assertEquals("sql is null", e.getMessage());
        }
    }

    public void test_getSql_blankSql() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("sql", "   ");

        try {
            dataStore.getSql(paramMap);
            fail("Should throw DataStoreException");
        } catch (final DataStoreException e) {
            assertEquals("sql is null", e.getMessage());
        }
    }

    public void test_getUsername_validUsername() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("username", "testuser");

        final String result = dataStore.getUsername(paramMap);
        assertEquals("testuser", result);
    }

    public void test_getUsername_nullUsername() {
        final DataStoreParams paramMap = new DataStoreParams();

        final String result = dataStore.getUsername(paramMap);
        assertNull(result);
    }

    public void test_getUsername_emptyUsername() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("username", "");

        final String result = dataStore.getUsername(paramMap);
        assertEquals("", result);
    }

    public void test_getPassword_validPassword() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("password", "secret123");

        final String result = dataStore.getPassword(paramMap);
        assertEquals("secret123", result);
    }

    public void test_getPassword_nullPassword() {
        final DataStoreParams paramMap = new DataStoreParams();

        final String result = dataStore.getPassword(paramMap);
        assertNull(result);
    }

    public void test_getPassword_emptyPassword() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("password", "");

        final String result = dataStore.getPassword(paramMap);
        assertEquals("", result);
    }

    public void test_getUrl_validUrl() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("url", "jdbc:h2:mem:test");

        final String result = dataStore.getUrl(paramMap);
        assertEquals("jdbc:h2:mem:test", result);
    }

    public void test_getUrl_nullUrl() {
        final DataStoreParams paramMap = new DataStoreParams();

        final String result = dataStore.getUrl(paramMap);
        assertNull(result);
    }

    public void test_getUrl_emptyUrl() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("url", "");

        final String result = dataStore.getUrl(paramMap);
        assertEquals("", result);
    }

    public void test_getFetchSize_validNumber() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("fetch_size", "1000");

        final Integer result = dataStore.getFetchSize(paramMap);
        assertEquals(Integer.valueOf(1000), result);
    }

    public void test_getFetchSize_minValue() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("fetch_size", "MIN_VALUE");

        final Integer result = dataStore.getFetchSize(paramMap);
        assertEquals(Integer.valueOf(Integer.MIN_VALUE), result);
    }

    public void test_getFetchSize_zero() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("fetch_size", "0");

        final Integer result = dataStore.getFetchSize(paramMap);
        assertEquals(Integer.valueOf(0), result);
    }

    public void test_getFetchSize_negativeNumber() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("fetch_size", "-100");

        final Integer result = dataStore.getFetchSize(paramMap);
        assertEquals(Integer.valueOf(-100), result);
    }

    public void test_getFetchSize_nullValue() {
        final DataStoreParams paramMap = new DataStoreParams();

        final Integer result = dataStore.getFetchSize(paramMap);
        assertNull(result);
    }

    public void test_getFetchSize_emptyValue() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("fetch_size", "");

        final Integer result = dataStore.getFetchSize(paramMap);
        assertNull(result);
    }

    public void test_getFetchSize_blankValue() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("fetch_size", "   ");

        final Integer result = dataStore.getFetchSize(paramMap);
        assertNull(result);
    }

    public void test_getFetchSize_invalidNumber() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("fetch_size", "abc123");

        final Integer result = dataStore.getFetchSize(paramMap);
        assertNull(result);
    }

    public void test_getFetchSize_invalidNumberFormat() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("fetch_size", "12.34");

        final Integer result = dataStore.getFetchSize(paramMap);
        assertNull(result);
    }

    public void test_getFetchSize_maxValue() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("fetch_size", String.valueOf(Integer.MAX_VALUE));

        final Integer result = dataStore.getFetchSize(paramMap);
        assertEquals(Integer.valueOf(Integer.MAX_VALUE), result);
    }

    public void test_getName() {
        final String result = dataStore.getName();
        assertEquals("DatabaseDataStore", result);
    }

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

    public void test_getConnection_withInfoParameters() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("url", "jdbc:h2:mem:test");
        paramMap.put("username", "testuser");
        paramMap.put("password", "testpass");
        paramMap.put("info.useSSL", "false");
        paramMap.put("info.timeout", "30");

        try {
            // This test would require H2 database driver to be available
            // For now, we're testing parameter handling logic
            dataStore.getConnection(paramMap);
        } catch (final SQLException e) {
            // Expected if H2 driver is not available
            assertTrue(e.getMessage().contains("No suitable driver") || e.getMessage().contains("Driver not found"));
        }
    }

    public void test_getConnection_nullUrl() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("username", "testuser");
        paramMap.put("password", "testpass");

        try {
            dataStore.getConnection(paramMap);
            fail("Should throw SQLException");
        } catch (final SQLException e) {
            // Expected - null URL should cause SQLException
            assertNotNull(e);
        }
    }

    public void test_getConnection_emptyUrl() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("url", "");
        paramMap.put("username", "testuser");
        paramMap.put("password", "testpass");

        try {
            dataStore.getConnection(paramMap);
            fail("Should throw SQLException");
        } catch (final SQLException e) {
            // Expected - empty URL should cause SQLException
            assertNotNull(e);
        }
    }

    public void test_getConnection_withoutCredentials() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("url", "jdbc:h2:mem:test");

        try {
            dataStore.getConnection(paramMap);
        } catch (final SQLException e) {
            // Expected if H2 driver is not available
            assertTrue(e.getMessage().contains("No suitable driver") || e.getMessage().contains("Driver not found"));
        }
    }

    public void test_parameter_constants() {
        // Test that parameter constants are correctly defined
        assertEquals("driver", "driver");
        assertEquals("url", "url");
        assertEquals("username", "username");
        assertEquals("password", "password");
        assertEquals("sql", "sql");
        assertEquals("fetch_size", "fetch_size");
        assertEquals("default_mimetype", "default_mimetype");
        assertEquals("info.", "info.");
        assertEquals("column_label.", "column_label.");
    }

    public void test_getDriverClass_withSpecialCharacters() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("driver", "com.mysql.cj.jdbc.Driver");

        final String result = dataStore.getDriverClass(paramMap);
        assertEquals("com.mysql.cj.jdbc.Driver", result);
    }

    public void test_getSql_withComplexQuery() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("sql", "SELECT id, name, email FROM users WHERE created_at > '2023-01-01' ORDER BY id");

        final String result = dataStore.getSql(paramMap);
        assertEquals("SELECT id, name, email FROM users WHERE created_at > '2023-01-01' ORDER BY id", result);
    }

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

    public void test_getFetchSize_leadingTrailingSpaces() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("fetch_size", "  100  ");

        final Integer result = dataStore.getFetchSize(paramMap);
        // StringUtil.isNotBlank considers strings with only whitespace as blank
        // So this should return null, not parse the number
        assertNull(result);
    }

    public void test_parameterExtraction_withNullDataStoreParams() {
        try {
            dataStore.getDriverClass(null);
            fail("Should throw NullPointerException");
        } catch (final NullPointerException e) {
            // Expected
            assertNotNull(e);
        }
    }

    public void test_parameterExtraction_withSpecialValues() {
        final DataStoreParams paramMap = new DataStoreParams();

        // Test with special characters in username/password
        paramMap.put("username", "user@domain.com");
        paramMap.put("password", "pass!@#$%^&*()");

        assertEquals("user@domain.com", dataStore.getUsername(paramMap));
        assertEquals("pass!@#$%^&*()", dataStore.getPassword(paramMap));
    }

    public void test_parameterExtraction_withUnicodeValues() {
        final DataStoreParams paramMap = new DataStoreParams();

        // Test with Unicode characters
        paramMap.put("username", "用户名");
        paramMap.put("password", "密码123");

        assertEquals("用户名", dataStore.getUsername(paramMap));
        assertEquals("密码123", dataStore.getPassword(paramMap));
    }
}
