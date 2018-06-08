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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.TrainedFieldType;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.apache.solr.update.processor.FieldMutatingUpdateProcessor.FieldNameSelector;

import org.apache.solr.update.processor.SchemaMutatingUpdateRequestProcessorFactory.TypeMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.update.processor.SchemaMutatingUpdateRequestProcessorFactory.getDefaultFieldType;


import static org.apache.solr.update.processor.AddSchemaFieldsUpdateProcessorFactory.validateSelectorParams;
import static org.apache.solr.update.processor.FieldMutatingUpdateProcessor.SELECT_ALL_FIELDS;
import static org.apache.solr.update.processor.SchemaMutatingUpdateRequestProcessorFactory.getUnknownFields;
import static org.apache.solr.update.processor.SchemaMutatingUpdateRequestProcessorFactory.parseExpectedCategoriesFromExternalClassifier;
import static org.apache.solr.update.processor.SchemaMutatingUpdateRequestProcessorFactory.parseRegexMappings;
import static org.apache.solr.update.processor.SchemaMutatingUpdateRequestProcessorFactory.parseTypeMappings;
import static org.apache.solr.update.processor.SchemaMutatingUpdateRequestProcessorFactory.SupportedTypes;

/**
 * Created by abhi on 21/01/18.
 */
public class LearnSchemaUpdateRequestProcessorFactory extends UpdateRequestProcessorFactory
    implements SolrCoreAware, UpdateRequestProcessorFactory.RunAlways  {

  private static final Logger log = LoggerFactory.getLogger(LearnSchemaUpdateRequestProcessorFactory.class);

  public static final String CREATE_TRAININGID_IF_ABSENT = "createTrainingIdIfAbsent";

  private List<TypeMapping> typeMappings = Collections.emptyList();
  private List<SchemaMutatingUpdateRequestProcessorFactory.RegexMapping> regexMappings = Collections.emptyList();
  private FieldMutatingUpdateProcessorFactory.SelectorParams inclusions = new FieldMutatingUpdateProcessorFactory.SelectorParams();
  private Collection<FieldMutatingUpdateProcessorFactory.SelectorParams> exclusions = new ArrayList<>();
  private SolrResourceLoader solrResourceLoader = null;
  private String defaultFieldType;
  private SchemaMutatingUpdateRequestProcessorFactory.TypeTree typeTree;
  private Map<SupportedTypes, String> mostAccomodatingFieldTypes;
  private List<String> epectedExternalCategories = Collections.emptyList();

  @Override
  public void inform(SolrCore core) {
    solrResourceLoader = core.getResourceLoader();

    for (TypeMapping typeMapping : typeMappings) {
      typeMapping.populateValueClasses(core);
    }
  }

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    return new LearnSchemaUpdateRequestProcessor(this, next);
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
    epectedExternalCategories = parseExpectedCategoriesFromExternalClassifier(args);
    regexMappings = parseRegexMappings(args);

    typeTree = SchemaMutatingUpdateRequestProcessorFactory.parseTypeTree(args);
    mostAccomodatingFieldTypes = SchemaMutatingUpdateRequestProcessorFactory.getMostAccomodatingFieldTypes(typeTree);
    super.init(args);
  }

  private class LearnSchemaUpdateRequestProcessor extends UpdateRequestProcessor {

    //A back reference for getting details of the factory
    LearnSchemaUpdateRequestProcessorFactory learnSchemaUpdateRequestProcessorFactory;

    public LearnSchemaUpdateRequestProcessor(LearnSchemaUpdateRequestProcessorFactory learnSchemaUpdateRequestProcessorFactory, UpdateRequestProcessor next) {
      super(next);
      this.learnSchemaUpdateRequestProcessorFactory = learnSchemaUpdateRequestProcessorFactory;
    }

    private String mapValueClassesToFieldType(List<SolrInputField> fields){
      SolrInputField field;
      if (fields.size()==1 && (field = fields.get(0)) !=null && field.getValue()!=null && regexMappings.size()>0 ){
        String val = field.getValue().toString();

        for (SchemaMutatingUpdateRequestProcessorFactory.RegexMapping regexMapping: regexMappings){

          for (String regexpression : regexMapping.regexExpressions) {
            if ( val.matches(regexpression) ){
              return regexMapping.fieldTypeName;
            }
          }

        }
      }
      TypeMapping typeMapping = SchemaMutatingUpdateRequestProcessorFactory.mapValueClassesToFieldType(fields, typeMappings);
      return typeMapping==null? null:typeMapping.fieldTypeName;
    }

    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {

      final SolrInputDocument doc = cmd.getSolrInputDocument();
      final SolrCore core = cmd.getReq().getCore();

      Map<String,List<SolrInputField>> unknownFields = new HashMap<>();
      FieldNameSelector selector = SELECT_ALL_FIELDS;
      getUnknownFields(selector, doc, unknownFields);
      String trainingId = null;

      for (final Map.Entry<String,List<SolrInputField>> entry : unknownFields.entrySet()) {
        String fieldName = entry.getKey();
        String fieldTypeName = defaultFieldType;
        List<SolrInputField> inputFields = entry.getValue();

        boolean lookForSemanticTypes = Boolean.valueOf(cmd.getReq().getParams().get("allowSemanticInference"));

        for (SolrInputField inputField : inputFields){
          if (inputField.getValue() instanceof  List){
            // Can be a candidate of MultiValued FieldType
            for (Object val : (List)inputField.getValue()){
              SolrInputField innerInputField = new SolrInputField(inputField.getName());
              innerInputField.setValue(val);
              try{
                fieldTypeName = mapValueClassesToFieldType(Collections.singletonList(innerInputField));
              }catch (NullPointerException e){
                log.error("Field :"+inputField.getName()+" in productId: "+doc.get("id")+" has null values. Training ID: "+trainingId);
                //throw e;
                continue;
              }
              fieldTypeName = fieldTypeName==null?defaultFieldType:fieldTypeName;
              if (lookForSemanticTypes && defaultFieldType.equals(fieldTypeName) && !fieldName.equals("id")){
                try{
                  String modelBasedType = APIBasedInference.predictCategory(inputField.getValue()).result;
                  TrainedFieldType.addStatsAboutCurrentType(cmd.getReq(), fieldName, modelBasedType);
                }catch (Exception e){
                  log.error("Some problem in APIBasedInference for "+inputField.getValue()+". Error : "+e.getMessage(), e);
                }
              }
              trainingId = TrainedFieldType.trainSchema(cmd.getReq(), fieldName, fieldTypeName, true, learnSchemaUpdateRequestProcessorFactory);
            }
          }else{
            try{
              fieldTypeName = mapValueClassesToFieldType(Collections.singletonList(inputField));
            }catch (NullPointerException e){
              log.error("Field :"+inputField.getName()+" in productId: "+doc.get("id")+" has null values. Training ID: "+trainingId);
              //throw e;
              continue;
            }
            fieldTypeName = fieldTypeName==null?defaultFieldType:fieldTypeName;

            if (lookForSemanticTypes && fieldTypeName.equals(defaultFieldType) && !fieldName.equals("id")){
              //Trying the model only if this is a text.
              try{

                String modelBasedType = APIBasedInference.predictCategory(inputField.getValue()).result;
                TrainedFieldType.addStatsAboutCurrentType(cmd.getReq(), fieldName, modelBasedType);

              }catch (Exception e){
                log.error("Some problem in APIBasedInference for "+inputField.getValue()+". Error : "+e.getMessage(), e);
              }
            }
            trainingId = TrainedFieldType.trainSchema(cmd.getReq(), fieldName, fieldTypeName, false, learnSchemaUpdateRequestProcessorFactory);
          }
        }
        String uniqueIdField = cmd.getReq().getParams().get("uniqueIdField");

        if(uniqueIdField!=null){
          SolrInputField valueAtUniqueID = doc.get(uniqueIdField);
          if(valueAtUniqueID==null){
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No value found in the doc for 'uniqueIdField' :"+uniqueIdField);
          }
          //The below line should not give NPE. If it does, that means {{trainingIdToUniqueIdsEncountered}}
          // has not been initialised properly.
          TrainedFieldType.trainingIdToUniqueIdsEncountered.get(trainingId).add(valueAtUniqueID.getValue());
        }
      }
      super.processAdd(cmd);
    }
  }

  public Map<SupportedTypes, String> getMostAccomodatingFieldTypes() {
    return mostAccomodatingFieldTypes;
  }
}
