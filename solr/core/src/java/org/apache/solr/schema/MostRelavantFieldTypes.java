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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import static org.apache.solr.update.processor.LearnSchemaUpdateRequestProcessorFactory.CREATE_TRAININGID_IF_ABSENT;

/**
 * Created by abhi on 07/01/18.
 */
public class MostRelavantFieldTypes {

  static final BitSet _string = BitSet.valueOf(new byte[]{16});
  static final BitSet _double = BitSet.valueOf(new byte[]{8});
  static final BitSet _long = BitSet.valueOf(new byte[]{4});
  static final BitSet _boolean = BitSet.valueOf(new byte[]{2});
  static final BitSet _date = BitSet.valueOf(new byte[]{1});
  static final Map<String, MostRelavantFieldTypes> trainedCoreToMostRelevantFieldTypesMapping = new ConcurrentHashMap<>();

  public static final Map<String, Set<Object>> trainingIdToUniqueIdsEncountered = new ConcurrentHashMap<>();

  private final Map<String, Map<String, AtomicLong>> fieldNameWiseStatsMapping = new ConcurrentHashMap<>();
  private final Map<String, BitSetReprForFieldType> fieldNameToFieldTypesMapping = new ConcurrentHashMap<>();
  private long createdTimeStamp = System.currentTimeMillis();

  private static BitSetReprForFieldType getBitSetForFieldType(String fieldTypeName, boolean isMultiValued){
    switch (fieldTypeName){

      //Note: Ignore the mapped fieldTypeNames For multiValued, take the value of isMultiValued, final for creating BitSetRepr
      case "text_general"  :
      case "string"        : return new BitSetReprForFieldType((BitSet)_string.clone(), isMultiValued );

      case "tlong"         :
      case "plongs"        :
      case "long"          : return new BitSetReprForFieldType((BitSet)_long.clone(), isMultiValued );

      case "tdouble"       :
      case "pdoubles"      :
      case "double"        : return new BitSetReprForFieldType((BitSet)_double.clone(), isMultiValued );

      case "pdates"        :
      case "tdate"        :
      case "date"          : return new BitSetReprForFieldType((BitSet)_date.clone(), isMultiValued );

      case "booleans"      :
      case "boolean"       : return new BitSetReprForFieldType((BitSet)_boolean.clone(), isMultiValued );
      default : throw new RuntimeException("No BitSetMapping found for FieldType : "+fieldTypeName);
    }
  }

  public void addAllowedFieldType(String fieldName, String fieldTypeName, boolean multiValued){
    BitSetReprForFieldType bitSetRepr = getBitSetForFieldType(fieldTypeName, multiValued);
    BitSetReprForFieldType previousEntry = fieldNameToFieldTypesMapping.putIfAbsent(fieldName, bitSetRepr);
    synchronized (this){
      if (fieldNameWiseStatsMapping.containsKey(fieldName)){
        Map<String, AtomicLong> stats = fieldNameWiseStatsMapping.get(fieldName);
        if (stats.containsKey(fieldTypeName)){
          stats.get(fieldTypeName).incrementAndGet();
        }else{
          stats.put(fieldTypeName, new AtomicLong(1L));
        }
      }
    }
    if (previousEntry!=null){
      previousEntry.applyOR_Oprn(bitSetRepr);
    }
  }

  public Map<String, String> getMostRelevantFieldTypes(){

    return fieldNameToFieldTypesMapping.entrySet().stream()
        .collect(Collectors.toMap(
              Map.Entry::getKey,
              e -> e.getValue().getMostSuitableFieldType()
          )
        );
  }

  /**
   *
   */
  private static class BitSetReprForFieldType {

    BitSet bitSetRepresentation;
    boolean isMultiValued = false;

    public BitSetReprForFieldType(BitSet bitSet, boolean isMultiValued) {
      bitSetRepresentation = bitSet;
      this.isMultiValued = isMultiValued;
    }

    // We want an instance level lock here
    private synchronized void applyOR_Oprn(BitSetReprForFieldType anotherFieldType){
      bitSetRepresentation.or( anotherFieldType.bitSetRepresentation );
      isMultiValued |= anotherFieldType.isMultiValued;
    }
    // We want an instance level lock here
    private synchronized void applyOR_Oprn(String anotherFieldType, boolean isMultiValued){
      BitSetReprForFieldType bisetRepr = getBitSetForFieldType(anotherFieldType, isMultiValued);
      bitSetRepresentation.or( bisetRepr.bitSetRepresentation );
      this.isMultiValued |= bisetRepr.isMultiValued;
    }

    /**
     * Follow the following rule..
     * 1xxxx -> String
     *
     * 01x00 -> Double : 8,12
     * 01000 -> Double : 8
     * 01100 -> Double : 12
     * 00100 -> Long   : 4
     * 00010 -> Boolean: 2
     * 00001 -> Date   : 1
     *
     * default: String
     *
     */
    String getMostSuitableFieldType() {

      long supportFieldTypeCode = bitSetRepresentation.toLongArray()[0];
      switch (supportFieldTypeCode+""){
        case "8":
        case "12": return isMultiValued ? "pdoubles" : "pdouble";
        case "4" : return isMultiValued ? "plongs"   : "plong";
        case "2" : return isMultiValued ? "booleans" : "boolean" ;
        case "1" : return "pdate" ;
        default  : return isMultiValued ? "strings"  : "string";
      }

    }
    //--

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
   * @param req
   * @param fieldName
   * @param fieldTypeName
   */
  public static String trainSchema(SolrQueryRequest req, String fieldName, String fieldTypeName, boolean multiValued){
    String trainingId = verifyAndGetTrainingId(req);
    trainedCoreToMostRelevantFieldTypesMapping.get( trainingId )
        .addAllowedFieldType( fieldName, fieldTypeName , multiValued);
    return trainingId;
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

  private Map<String, Object> convertTrainingMetaDataToSchemaFormat(){
    //{add-field-type:[{"name":"myNewTxtField3","class":"solr.TextField","positionIncrementGap":"100"},{"name":"myNewTxtField2","class":"solr.TextField","positionIncrementGap":"100"}]}
    Map<String, Object> addSchemaData = new HashMap<>();
    //addSchemaData.put("add-field-type", )
    List fieldSchemas = new ArrayList();
    for(Map.Entry<String, BitSetReprForFieldType> entry : fieldNameToFieldTypesMapping.entrySet()){
      Map<String, Object> fieldSchema = new LinkedHashMap<>();
      String fieldName = entry.getKey();
      fieldSchema.put("name",fieldName);
      BitSetReprForFieldType bitsetRepr = entry.getValue();
      String fieldType = bitsetRepr.getMostSuitableFieldType();
      fieldSchema.put("type", fieldType);
      fieldSchema.put("multivalued", bitsetRepr.isMultiValued);
      fieldSchemas.add(fieldSchema);
    }
    addSchemaData.put("add-field-type", fieldSchemas);
    return addSchemaData;
  }

  public static void getUniqueIdCounts(SolrQueryRequest req, SolrQueryResponse rsp){
    TrainedFieldType.getUniqueIdCounts(req, rsp);
    /*String trainingId = verifyAndGetTrainingId(req);
    int totCount = 0;
    List<String> errors = new LinkedList<>();
    Set<Object> dd = trainingIdToUniqueIdsEncountered.get(trainingId);
    if (dd!=null)
      totCount = dd.size();
    else
      errors.add("No counters found for trainingID:"+trainingId);
    rsp.add("count", totCount);
    rsp.add("errors",errors);*/
  }
  public static void getTrainedSchema(SolrQueryRequest req, SolrQueryResponse rsp){
    TrainedFieldType.getTrainedSchema(req, rsp);
    /*String trainingId = verifyAndGetTrainingId(req);
    MostRelavantFieldTypes mostRelavantFieldTypes = trainedCoreToMostRelevantFieldTypesMapping.get(trainingId);
    rsp.add(IndexSchema.SCHEMA, mostRelavantFieldTypes.convertTrainingMetaDataToSchemaFormat());
    try{
      if ( Boolean.valueOf(req.getParams().get("enableStats"))){
        rsp.add("stats", mostRelavantFieldTypes.getFieldWiseStats());
      }
    }catch (Exception e){
      e.printStackTrace();
    }
*/
  }

  /**
   * Will return map with FieldName : MostSuitableFieldName.
   * @param trainingId
   * @return
   */
  public static Map<String, String> getTrainedSchema(String trainingId){
    MostRelavantFieldTypes mostRelavantFieldTypes = trainedCoreToMostRelevantFieldTypesMapping.get(trainingId);
    return mostRelavantFieldTypes.getMostRelevantFieldTypes();
  }

  public static void addTrainingIdIfAbsent(String trainingId){
    MostRelavantFieldTypes prevVal = trainedCoreToMostRelevantFieldTypesMapping.putIfAbsent(trainingId, new MostRelavantFieldTypes());
    if(prevVal==null){
      //ie. this one was actually the first entry.
      MostRelavantFieldTypes.trainingIdToUniqueIdsEncountered.putIfAbsent(trainingId, new HashSet<>());
    }
  }

  public static String generateTrainingId(SolrCore core){
    MostRelavantFieldTypes existingValue = null;
    String trainingID;
    //Try to create a uniqueTraining id and insert into the map.
    do{
      trainingID = UUID.randomUUID().toString();
      existingValue = trainedCoreToMostRelevantFieldTypesMapping.putIfAbsent(trainingID, new MostRelavantFieldTypes());
    }while (existingValue!=null);
    MostRelavantFieldTypes.trainingIdToUniqueIdsEncountered.putIfAbsent(trainingID, new HashSet<>());
    return trainingID;
  }



}
