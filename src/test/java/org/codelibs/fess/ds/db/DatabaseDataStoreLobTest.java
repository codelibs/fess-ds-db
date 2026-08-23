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
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codelibs.core.io.InputStreamUtil;
import org.codelibs.fess.crawler.container.CrawlerContainer;
import org.codelibs.fess.crawler.entity.ExtractData;
import org.codelibs.fess.crawler.exception.ExtractException;
import org.codelibs.fess.crawler.extractor.Extractor;
import org.codelibs.fess.crawler.extractor.ExtractorFactory;
import org.codelibs.fess.crawler.helper.ContentLengthHelper;
import org.codelibs.fess.crawler.helper.impl.MimeTypeHelperImpl;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.util.ComponentUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Covers the large object branches of
 * {@code DatabaseDataStore.ResultSetParamMap#getColumnValue}: the ones that hand
 * the column to an extractor, and the ones that do not.
 *
 * <p>
 * {@code ExtractorBuilder} resolves {@code extractorFactory},
 * {@code contentLengthHelper} and {@code mimeTypeHelper} through a
 * {@link CrawlerContainer}, which the unit-test container does not provide.
 * This test supplies a map-backed one, with extractors that tag their output so
 * an assertion can tell which extractor ran - or whether one ran at all.
 * </p>
 */
public class DatabaseDataStoreLobTest extends AbstractDatabaseDataStoreTestCase {

    private static final String PDF_MIMETYPE = "application/pdf";

    // ------------------------------------------------------------------
    // which branch reaches an extractor
    // ------------------------------------------------------------------

    @Test
    public void test_blobIsHandedToTheExtractor() throws Exception {
        createBlobTable("hello");

        final List<Map<String, Object>> docs = runStoreData(blobParams(), scripts("content", "PAYLOAD"));

        assertNoRowFailure();
        // With no MIME hint the builder sniffs the content, which resolves to text/plain here.
        assertEquals("text:hello", docs.get(0).get("content"));
    }

    @Test
    public void test_defaultMimetypeSelectsTheExtractor() throws Exception {
        createBlobTable("hello");

        final DataStoreParams paramMap = blobParams();
        paramMap.put("default_mimetype", PDF_MIMETYPE);

        final List<Map<String, Object>> docs = runStoreData(paramMap, scripts("content", "PAYLOAD"));

        assertNoRowFailure();
        assertEquals("pdf:hello", docs.get(0).get("content"));
    }

    @Test
    public void test_columnLabelMimetypeSelectsTheExtractor() throws Exception {
        createBlobTableWithMimetype("hello", PDF_MIMETYPE);

        final DataStoreParams paramMap = params("SELECT mime, payload FROM doc");
        paramMap.put("column_label.mimetype", "MIME");

        final List<Map<String, Object>> docs = runStoreData(paramMap, scripts("content", "PAYLOAD"));

        assertNoRowFailure();
        assertEquals("pdf:hello", docs.get(0).get("content"));
    }

    /**
     * The hint applies wherever its column sits in the SELECT list. It used to be
     * looked up in the map that was still being built, so it only took effect
     * when its column happened to come first.
     */
    @Test
    public void test_columnLabelMimetypeAppliesWhenItsColumnComesAfterTheBlob() throws Exception {
        createBlobTableWithMimetype("hello", PDF_MIMETYPE);

        final DataStoreParams paramMap = params("SELECT payload, mime FROM doc");
        paramMap.put("column_label.mimetype", "MIME");

        final List<Map<String, Object>> docs = runStoreData(paramMap, scripts("content", "PAYLOAD"));

        assertNoRowFailure();
        assertEquals("pdf:hello", docs.get(0).get("content"));
    }

    @Test
    public void test_columnLabelFilenameSelectsTheExtractor() throws Exception {
        execute("CREATE TABLE doc (id INT PRIMARY KEY, name VARCHAR(50), payload BLOB)");
        try (Connection con = connect(); PreparedStatement ps = con.prepareStatement("INSERT INTO doc VALUES (1, ?, ?)")) {
            ps.setString(1, "manual.txt");
            ps.setBytes(2, "hello".getBytes(StandardCharsets.UTF_8));
            ps.execute();
        }

        final DataStoreParams paramMap = params("SELECT name, payload FROM doc");
        paramMap.put("column_label.filename", "NAME");

        final List<Map<String, Object>> docs = runStoreData(paramMap, scripts("content", "PAYLOAD"));

        assertNoRowFailure();
        assertEquals("text:hello", docs.get(0).get("content"));
    }

    // ------------------------------------------------------------------
    // extraction failures
    // ------------------------------------------------------------------

    /**
     * An extraction failure is reported as itself. It used to be rethrown as
     * {@code FessSystemException("Failed to access meta data.")}, which named a
     * cause that had nothing to do with what went wrong.
     */
    @Test
    public void test_extractionFailureIsReportedAsItself() throws Exception {
        createBlobTable("hello");
        // A MIME type of its own: addExtractor appends, and a composite only moves on to
        // the next extractor when the previous one reports the data as unsupported.
        extractorFactory.addExtractor("application/x-failing", new FailingExtractor());

        final DataStoreParams paramMap = blobParams();
        paramMap.put("default_mimetype", "application/x-failing");

        final List<Map<String, Object>> docs = runStoreData(paramMap, scripts("content", "PAYLOAD"));

        // The row is still recorded as failed rather than indexed with no content.
        assertTrue(docs.isEmpty());
        assertEquals(1, failureUrlService.throwables.size());
        final Throwable recorded = failureUrlService.throwables.get(0);
        assertTrue(recorded.getClass().getName(), recorded instanceof ExtractException);
        assertEquals("Failed to extract data.", recorded.getMessage());
        assertEquals("cannot extract this", recorded.getCause().getMessage());
    }

    /**
     * Pins current behaviour: only the extractor path is bounded. A CLOB is read
     * into memory whole, however large it is.
     */
    @Test
    public void test_blobIsBoundedByContentLengthButClobIsNot() throws Exception {
        final int size = 2 * 1024 * 1024;
        execute("CREATE TABLE doc (id INT PRIMARY KEY, body CLOB, payload BLOB)");
        try (Connection con = connect(); PreparedStatement ps = con.prepareStatement("INSERT INTO doc VALUES (1, ?, ?)")) {
            ps.setString(1, "c".repeat(size));
            ps.setBytes(2, "b".repeat(size).getBytes(StandardCharsets.UTF_8));
            ps.execute();
        }

        // Lower the extractor bound below the payload size. The CLOB has no such bound.
        final ContentLengthHelper contentLengthHelper = new ContentLengthHelper();
        contentLengthHelper.setDefaultMaxLength(1024L);
        extractorFactory.components.put("contentLengthHelper", contentLengthHelper);

        final List<Map<String, Object>> docs = runStoreData(params("SELECT body FROM doc"), scripts("content", "BODY"));

        assertNoRowFailure();
        assertEquals(size, ((String) docs.get(0).get("content")).length());
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private DataStoreParams blobParams() {
        return params("SELECT payload FROM doc");
    }

    private void createBlobTable(final String content) throws Exception {
        execute("CREATE TABLE doc (id INT PRIMARY KEY, payload BLOB)");
        try (Connection con = connect(); PreparedStatement ps = con.prepareStatement("INSERT INTO doc VALUES (1, ?)")) {
            ps.setBytes(1, content.getBytes(StandardCharsets.UTF_8));
            ps.execute();
        }
    }

    private void createBlobTableWithMimetype(final String content, final String mimetype) throws Exception {
        execute("CREATE TABLE doc (id INT PRIMARY KEY, mime VARCHAR(50), payload BLOB)");
        try (Connection con = connect(); PreparedStatement ps = con.prepareStatement("INSERT INTO doc VALUES (1, ?, ?)")) {
            ps.setString(1, mimetype);
            ps.setBytes(2, content.getBytes(StandardCharsets.UTF_8));
            ps.execute();
        }
    }

    private static class FailingExtractor implements Extractor {
        @Override
        public ExtractData getText(final InputStream in, final Map<String, String> params) {
            throw new ExtractException("cannot extract this");
        }
    }
}
