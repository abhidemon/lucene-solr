/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.update.processor;

import java.io.File;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.schema.IndexSchema;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Before;

/**
 * Created by abhi on 06/03/18.
 */
public class LearnSchemaUpdateProcessorFactoryTest extends UpdateProcessorTestBase {

  private static final String SOLRCONFIG_XML = "solrconfig-learn-schema-fields-URP-chains.xml";
  private static final String SCHEMA_XML     = "schema-add-schema-fields-update-processor.xml";

  private static File tmpSolrHome;
  private static File tmpConfDir;

  private static final String collection = "collection1";
  private static final String confDir = collection + "/conf";

  @Before
  private void initManagedSchemaCore() throws Exception {
    tmpSolrHome = createTempDir().toFile();
    tmpConfDir = new File(tmpSolrHome, confDir);
    File testHomeConfDir = new File(TEST_HOME(), confDir);
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, SOLRCONFIG_XML), tmpConfDir);
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, SCHEMA_XML), tmpConfDir);

    // initCore will trigger an upgrade to managed schema, since the solrconfig*.xml has
    // <schemaFactory class="ManagedIndexSchemaFactory" ... />
    initCore(SOLRCONFIG_XML, SCHEMA_XML, tmpSolrHome.getPath());
  }

  public void testSingleField() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    final String fieldName = "newfield1";
    assertNull(schema.getFieldOrNull(fieldName));
    String dateString = "2010-11-12T13:14:15.168Z";
    DateTimeFormatter dateTimeFormatter = ISODateTimeFormat.dateTime();
    Date date = dateTimeFormatter.parseDateTime(dateString).toDate();
    SolrInputDocument d = processAdd("add-fields-no-run-processor", doc(f("id", "1"), f(fieldName, date)));
    assertNotNull(d);
    schema = h.getCore().getLatestSchema();
    assertNotNull(schema.getFieldOrNull(fieldName));
    assertEquals("pdates", schema.getFieldType(fieldName).getTypeName());
  }


}
