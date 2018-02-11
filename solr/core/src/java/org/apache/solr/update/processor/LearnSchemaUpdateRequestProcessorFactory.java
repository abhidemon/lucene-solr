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
import org.apache.solr.schema.MostRelavantFieldTypes;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.apache.solr.update.processor.FieldMutatingUpdateProcessor.FieldNameSelector;

import org.apache.solr.update.processor.SchemaMutatingUpdateRequestProcessorFactory.TypeMapping;
import org.apache.solr.update.processor.SchemaMutatingUpdateRequestProcessorFactory.CopyFieldDef;
import static org.apache.solr.update.processor.SchemaMutatingUpdateRequestProcessorFactory.DEFAULT_FIELD_TYPE_PARAM;
import static org.apache.solr.update.processor.SchemaMutatingUpdateRequestProcessorFactory.getDefaultFieldType;


import static org.apache.solr.update.processor.AddSchemaFieldsUpdateProcessorFactory.validateSelectorParams;
import static org.apache.solr.update.processor.FieldMutatingUpdateProcessor.SELECT_ALL_FIELDS;
import static org.apache.solr.update.processor.SchemaMutatingUpdateRequestProcessorFactory.getUnknownFields;
import static org.apache.solr.update.processor.SchemaMutatingUpdateRequestProcessorFactory.mapValueClassesToFieldType;
import static org.apache.solr.update.processor.SchemaMutatingUpdateRequestProcessorFactory.parseTypeMappings;
//return FieldMutatingUpdateProcessor.SELECT_ALL_FIELDS;
/**
 * Created by abhi on 21/01/18.
 */
public class LearnSchemaUpdateRequestProcessorFactory extends UpdateRequestProcessorFactory
    implements SolrCoreAware, UpdateRequestProcessorFactory.RunAlways  {

  private List<TypeMapping> typeMappings = Collections.emptyList();
  private FieldMutatingUpdateProcessorFactory.SelectorParams inclusions = new FieldMutatingUpdateProcessorFactory.SelectorParams();
  private Collection<FieldMutatingUpdateProcessorFactory.SelectorParams> exclusions = new ArrayList<>();
  private SolrResourceLoader solrResourceLoader = null;
  private String defaultFieldType;


  @Override
  public void inform(SolrCore core) {
    solrResourceLoader = core.getResourceLoader();

    for (TypeMapping typeMapping : typeMappings) {
      typeMapping.populateValueClasses(core);
    }
  }

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    return new LearnSchemaUpdateRequestProcessor(next);
  }

  @Override
  public void init(NamedList args) {
    inclusions = FieldMutatingUpdateProcessorFactory.parseSelectorParams(args);
    validateSelectorParams(inclusions);
    inclusions.fieldNameMatchesSchemaField = false;  // Explicitly (non-configurably) require unknown field names
    exclusions = FieldMutatingUpdateProcessorFactory.parseSelectorExclusionParams(args);
    for (FieldMutatingUpdateProcessorFactory.SelectorParams exclusion : exclusions) {
      validateSelectorParams(exclusion);
    }
    defaultFieldType = getDefaultFieldType(args);
    typeMappings = parseTypeMappings(args);

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
      getUnknownFields(selector, doc, unknownFields);

      for (final Map.Entry<String,List<SolrInputField>> entry : unknownFields.entrySet()) {
        String fieldName = entry.getKey();
        String fieldTypeName = defaultFieldType;
        List<SolrInputField> inputFields = entry.getValue();

        for (SolrInputField inputField : inputFields){

          if (inputField.getValue() instanceof  List){
            // Can be a candidate of MultiValued FieldType
            for (Object val : (List)inputField.getValue()){
              SolrInputField innerInputField = new SolrInputField(inputField.getName());
              innerInputField.setValue(val);
              TypeMapping typeMapping = mapValueClassesToFieldType(Collections.singletonList(innerInputField), typeMappings);
              if (typeMapping!=null) {
                MostRelavantFieldTypes.trainSchema(cmd.getReq(), fieldName, typeMapping.fieldTypeName, true);
              }
            }
          }else{
            TypeMapping typeMapping = mapValueClassesToFieldType(Collections.singletonList(inputField), typeMappings);
            if (typeMapping!=null) {
              MostRelavantFieldTypes.trainSchema(cmd.getReq(), fieldName, typeMapping.fieldTypeName, false);
            }
          }
        }

      }
      super.processAdd(cmd);
    }
  }

}
