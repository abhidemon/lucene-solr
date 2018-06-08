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

package org.apache.solr.schema;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.solr.common.SolrException;
import org.apache.solr.core.PluginBag;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import org.apache.solr.update.processor.LearnSchemaUpdateRequestProcessorFactory;
import org.apache.solr.update.processor.SchemaMutatingUpdateRequestProcessorFactory.SupportedTypes;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;

import static org.apache.solr.update.processor.LearnSchemaUpdateRequestProcessorFactory.CREATE_TRAININGID_IF_ABSENT;

/**
 * Created by abhi on 07/01/18.
 */
public class TrainedFieldType {

  static final BitSet _string = BitSet.valueOf(new byte[]{16});
  static final BitSet _double = BitSet.valueOf(new byte[]{8});
  static final BitSet _long = BitSet.valueOf(new byte[]{4});
  static final BitSet _boolean = BitSet.valueOf(new byte[]{2});
  static final BitSet _date = BitSet.valueOf(new byte[]{1});
  static final Map<String, TrainedFieldType> trainedCoreToMostRelevantFieldTypesMapping = new ConcurrentHashMap<>();

  public static final Map<String, Set<Object>> trainingIdToUniqueIdsEncountered = new ConcurrentHashMap<>();

  private final Map<String, Map<String, AtomicLong>> fieldNameWiseStatsMapping = new ConcurrentHashMap<>();
  private final Map<String, SupportedTypes> fieldNameToSupportedFieldTypesMapping = new ConcurrentHashMap<>();
  private long createdTimeStamp = System.currentTimeMillis();


  public void addAllowedFieldType(String fieldName, String fieldTypeName, boolean multiValued, Map<SupportedTypes, String> mostAccomodatingFieldTypes, boolean addToStats){
    if (!fieldNameToSupportedFieldTypesMapping.containsKey(fieldName)){
      fieldNameToSupportedFieldTypesMapping.putIfAbsent(fieldName, new SupportedTypes(mostAccomodatingFieldTypes));
    }
    fieldNameToSupportedFieldTypesMapping.get(fieldName).addFieldForSupport(fieldTypeName, multiValued);

    if (addToStats){
      addStatsAboutCurrentType(fieldName, fieldTypeName);
    }

  }

  public static String addStatsAboutCurrentType(SolrQueryRequest req, String fieldName, String fieldTypeName){
    String trainingId = verifyAndGetTrainingId(req);
    trainedCoreToMostRelevantFieldTypesMapping.get( trainingId )
        .addStatsAboutCurrentType(fieldName, fieldTypeName);
    return trainingId;
  }

  public void addStatsAboutCurrentType(String fieldName, String fieldTypeName){
    synchronized (this){
      if (!fieldNameWiseStatsMapping.containsKey(fieldName)){
        fieldNameWiseStatsMapping.putIfAbsent(fieldName, new HashMap<String, AtomicLong>());
      }
      Map<String, AtomicLong> stats = fieldNameWiseStatsMapping.get(fieldName);
      if (stats.containsKey(fieldTypeName)){
        stats.get(fieldTypeName).incrementAndGet();
      }else{
        stats.put(fieldTypeName, new AtomicLong(1L));
      }
    }
  }

  public Map<String, String> getMostRelevantFieldTypes(){

    return fieldNameToSupportedFieldTypesMapping.entrySet().stream()
        .collect(Collectors.toMap(
              Map.Entry::getKey,
              e -> e.getValue().getMostRelevantFieldTypes()
          )
        );
  }

  /**
   * Check if 'trainingId' is present in the request.
   * If yes, then check if it is valid or not.
   * If invalid, throw error or insertTheTraining Id based on CREATE_TRAININGID_IF_ABSENT flag.
   * @param req
   * @return
   */
  private static String verifyAndGetTrainingId(SolrQueryRequest req){
    String trainingId = req.getParams().get(IndexSchema.TRAIN_ID);
    if ( trainingId==null){
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Param '"+IndexSchema.TRAIN_ID+"' cannot be null!.");
    }
    if ( !trainedCoreToMostRelevantFieldTypesMapping.containsKey(trainingId) ){
      if ( req.getParams().getBool(CREATE_TRAININGID_IF_ABSENT, false) ){
        addTrainingIdIfAbsent(trainingId);
      }else{
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown trainingId:"+trainingId);
      }
    }
    return trainingId;
  }

  /**
   * Add the new fieldTypeName for the fieldName, to be decided the best fieldTypeName for this field.
   */
  public static String trainSchema(SolrQueryRequest req, String fieldName, String fieldTypeName, boolean multiValued, LearnSchemaUpdateRequestProcessorFactory lsURSP){
    String trainingId = verifyAndGetTrainingId(req);
    trainedCoreToMostRelevantFieldTypesMapping.get( trainingId )
        .addAllowedFieldType( fieldName, fieldTypeName , multiValued, lsURSP.getMostAccomodatingFieldTypes(), false);
    return trainingId;
  }

  public Map<String, Map<String, AtomicLong>> getFieldNameWiseStatsMapping(){
    return fieldNameWiseStatsMapping;
  }

  private Map<String, Object> getFieldWiseStats(){
    Map<String, Object> statsData = new LinkedHashMap<>();
    for(Map.Entry<String, Map<String, AtomicLong>> entry : fieldNameWiseStatsMapping.entrySet()){
      List<Map<String, Object>> anomalyData = new ArrayList<>();
      String fieldName = entry.getKey();
      Map<String, AtomicLong> fieldTypeStats = entry.getValue();
      int total = fieldTypeStats.size();
      double LOWER_THRESHOLD = 5;// 5%
      double UPPER_THRESHOLD = 80; //40%

      for (Map.Entry<String, AtomicLong> fieldTypeFreq : fieldTypeStats.entrySet()){
        // TODO : Here, take the TypeGroup's frequency instead of a SingleType's
        double perc = (fieldTypeFreq.getValue().get()*100.0)/total;
        if (perc<LOWER_THRESHOLD){
          Map<String, Object> mp = new HashMap<>();
          mp.put("fieldType", fieldTypeFreq.getKey());
          mp.put("frequency", perc);
          anomalyData.add( mp );
        }
      }
      if (anomalyData.size()>0){
        statsData.put("fieldName", anomalyData);
      }
    }
    return statsData;
    }

  private Map<String, Object> getFieldNameAndSupportedType(){

    Map<String, Object> fieldSchema = new LinkedHashMap<>();

    for(Map.Entry<String, SupportedTypes> entry : fieldNameToSupportedFieldTypesMapping.entrySet()){

      String fieldName = entry.getKey();

      SupportedTypes supportedTypes = entry.getValue();
      String fieldType = supportedTypes.getMostRelevantFieldTypes();

      fieldSchema.put(fieldName, fieldType);
    }
    return fieldSchema;
  }

  private Map<String, Object> convertTrainingMetaDataToSchemaFormat(){
    //{add-field-type:[{"name":"myNewTxtField3","class":"solr.TextField","positionIncrementGap":"100"},{"name":"myNewTxtField2","class":"solr.TextField","positionIncrementGap":"100"}]}
    Map<String, Object> addSchemaData = new HashMap<>();
    //addSchemaData.put("add-field-type", )
    List fieldSchemas = new ArrayList();
    for(Map.Entry<String, SupportedTypes> entry : fieldNameToSupportedFieldTypesMapping.entrySet()){
      Map<String, Object> fieldSchema = new LinkedHashMap<>();
      String fieldName = entry.getKey();
      fieldSchema.put("name",fieldName);
      SupportedTypes supportedTypes = entry.getValue();
      String fieldType = supportedTypes.getMostRelevantFieldTypes();
      fieldSchema.put("type", fieldType);
      fieldSchema.put("multivalued", supportedTypes.isMultiValued());
      fieldSchemas.add(fieldSchema);
    }
    addSchemaData.put("add-field-type", fieldSchemas);
    return addSchemaData;
  }

  public static void getUniqueIdCounts(SolrQueryRequest req, SolrQueryResponse rsp){
    String trainingId = verifyAndGetTrainingId(req);
    int totCount = 0;
    List<String> errors = new LinkedList<>();
    Set<Object> dd = trainingIdToUniqueIdsEncountered.get(trainingId);
    if (dd!=null)
      totCount = dd.size();
    else
      errors.add("No counters found for trainingID:"+trainingId);
    rsp.add("count", totCount);
    rsp.add("errors",errors);
  }

  public static void getTrainedSchema(SolrQueryRequest req, SolrQueryResponse rsp){
    String trainingId = verifyAndGetTrainingId(req);
    TrainedFieldType mostRelavantFieldTypes = trainedCoreToMostRelevantFieldTypesMapping.get(trainingId);
    if ( Boolean.valueOf(req.getParams().get("sendStatsBasedType"))){
      rsp.add("stats", mostRelavantFieldTypes.getFieldWiseStats());

    }else if (Boolean.valueOf(req.getParams().get("enableStats"))){
      rsp.add("stats", mostRelavantFieldTypes.getFieldWiseStats());

    }else{
      rsp.add(IndexSchema.SCHEMA, mostRelavantFieldTypes.convertTrainingMetaDataToSchemaFormat());
    }

  }

  public static void getStatsBasedSchema(SolrQueryRequest req, SolrQueryResponse rsp){

    String trainingId = verifyAndGetTrainingId(req);
    TrainedFieldType mostRelavantFieldTypes = trainedCoreToMostRelevantFieldTypesMapping.get(trainingId);
    if ( Boolean.valueOf(req.getParams().get("sendStatsBasedSchema"))){
      //Map<String, Map<String, AtomicLong>>
      //Map<String, Map<String, AtomicLong>> stats = ()mostRelavantFieldTypes.getFieldWiseStats();

      //mostRelavantFieldTypes.convertTrainingMetaDataToSchemaFormat();
      Map<String, Object> finalResp = mostRelavantFieldTypes.convertTrainingMetaDataToSchemaFormat();
      Map<String, Map<String, AtomicLong>> statsMapping = mostRelavantFieldTypes.getFieldNameWiseStatsMapping();
      //Map<String, AtomicLong> stats = statsMapping.get(trainingId);

      Map<String, Object> statsData = new LinkedHashMap<>();
      Map<String, Object> finalSchema = mostRelavantFieldTypes.getFieldNameAndSupportedType();

      for(Map.Entry<String, Map<String, AtomicLong>> fieldNameWiseStatsEntry : statsMapping.entrySet()){

        String fieldName = fieldNameWiseStatsEntry.getKey();
        Map<String, AtomicLong> fieldTypewiseStats = fieldNameWiseStatsEntry.getValue();

        long total = 0;
        long maxCount = 0;
        String typeWithMaxCount = null;
        for ( Map.Entry<String, AtomicLong> fieldTypeAndCounter : fieldTypewiseStats.entrySet()){

          String type = fieldTypeAndCounter.getKey();
          AtomicLong counter = fieldTypeAndCounter.getValue();
          long curCnt = counter.get();
          total += curCnt;
          if (curCnt>maxCount){
            maxCount=curCnt;
            typeWithMaxCount=type;
          }
        }

        if ( maxCount*1.0 >= 0.70*total ){

          finalSchema.put(fieldName, typeWithMaxCount);

        }

      }


      //for (stats)

      rsp.add(IndexSchema.SCHEMA, finalSchema);

    } else {
      rsp.add(IndexSchema.SCHEMA, mostRelavantFieldTypes.convertTrainingMetaDataToSchemaFormat());
    }

    if (Boolean.valueOf(req.getParams().get("enableStats"))){
      rsp.add("stats", mostRelavantFieldTypes.getFieldNameWiseStatsMapping());
    }


  }

  /**
   * Will return map with FieldName : MostSuitableFieldName.
   * trainingId
   *
   */
  public Map<String, String> getTrainedSchema(String trainingId){
    TrainedFieldType mostRelavantFieldTypes = trainedCoreToMostRelevantFieldTypesMapping.get(trainingId);
    return mostRelavantFieldTypes.getMostRelevantFieldTypes();
  }

  public static void addTrainingIdIfAbsent(String trainingId){
    TrainedFieldType prevVal = trainedCoreToMostRelevantFieldTypesMapping.putIfAbsent(trainingId, new TrainedFieldType());
    if(prevVal==null){
      //ie. this one was actually the first entry.
      TrainedFieldType.trainingIdToUniqueIdsEncountered.putIfAbsent(trainingId, new HashSet<>());
    }
  }

  public static String generateTrainingId(SolrCore core){
    TrainedFieldType existingValue = null;
    String trainingID;
    //Try to create a uniqueTraining id and insert into the map.
    do{
      trainingID = UUID.randomUUID().toString();
      existingValue = trainedCoreToMostRelevantFieldTypesMapping.putIfAbsent(trainingID, new TrainedFieldType());
    }while (existingValue!=null);
    TrainedFieldType.trainingIdToUniqueIdsEncountered.putIfAbsent(trainingID, new HashSet<>());
    return trainingID;
  }



}
