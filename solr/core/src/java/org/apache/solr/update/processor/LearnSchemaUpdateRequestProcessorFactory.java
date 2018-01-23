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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.apache.solr.update.processor.FieldMutatingUpdateProcessor.FieldNameSelector;

import static org.apache.solr.update.processor.AddSchemaFieldsUpdateProcessorFactory.validateSelectorParams;
import static org.apache.solr.update.processor.FieldMutatingUpdateProcessor.SELECT_ALL_FIELDS;

/**
 * Created by abhi on 21/01/18.
 */
public class LearnSchemaUpdateRequestProcessorFactory extends UpdateRequestProcessorFactory
    implements SolrCoreAware, UpdateRequestProcessorFactory.RunAlways  {

  private List<AddSchemaFieldsUpdateProcessorFactory.TypeMapping> typeMappings = Collections.emptyList();
  private FieldMutatingUpdateProcessorFactory.SelectorParams inclusions = new FieldMutatingUpdateProcessorFactory.SelectorParams();
  private Collection<FieldMutatingUpdateProcessorFactory.SelectorParams> exclusions = new ArrayList<>();
  private SolrResourceLoader solrResourceLoader = null;
  private String defaultFieldType;


  @Override
  public void inform(SolrCore core) {

  }

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    return null;
  }

  @Override
  public void init(NamedList args) {
    inclusions = FieldMutatingUpdateProcessorFactory.parseSelectorParams(args);
    validateSelectorParams(inclusions);
    inclusions.fieldNameMatchesSchemaField = false;  // Explicitly (non-configurably) require unknown field names
    exclusions = FieldMutatingUpdateProcessorFactory.parseSelectorExclusionParams(args);

    AddSchemaFieldsUpdateProcessorFactory.getDefaultFieldType(args);


    super.init(args);
  }

  private class LearnSchemaUpdateRequestProcessor extends UpdateRequestProcessor {

    public LearnSchemaUpdateRequestProcessor(UpdateRequestProcessor next) {
      super(next);
    }

    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {

      final SolrInputDocument doc = cmd.getSolrInputDocument();
      final SolrCore core = cmd.getReq().getCore();

      Map<String,List<SolrInputField>> unknownFields = new HashMap<>();
      FieldNameSelector selector = SELECT_ALL_FIELDS;
      AddSchemaFieldsUpdateProcessorFactory.getUnknownFields(selector, doc, unknownFields);

      for (final Map.Entry<String,List<SolrInputField>> entry : unknownFields.entrySet()) {
        String fieldName = entry.getKey();
        String fieldTypeName = AddSchemaFieldsUpdateProcessorFactory.getDefaultFieldType(args);

      }

      super.processAdd(cmd);

    }
  }

}
