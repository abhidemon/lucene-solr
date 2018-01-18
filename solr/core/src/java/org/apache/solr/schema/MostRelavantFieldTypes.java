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

import javax.lang.model.type.TypeMirror;
import javax.lang.model.type.UnknownTypeException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

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

  private Map<String, BitSetReprForFieldType> fieldNameToFieldTypesMapping = new ConcurrentHashMap<>();
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
      case "date"          : return new BitSetReprForFieldType((BitSet)_date.clone(), isMultiValued );

      case "booleans"      :
      case "boolean"       : return new BitSetReprForFieldType((BitSet)_double.clone(), isMultiValued );
      default : throw new RuntimeException("No BitSetMapping found for FieldType : "+fieldTypeName);
    }
  }

  public void addAllowedFieldType(String fieldName, String fieldTypeName, boolean multiValued){
    BitSetReprForFieldType bitSetRepr = getBitSetForFieldType(fieldTypeName, multiValued);
    BitSetReprForFieldType previousEntry = fieldNameToFieldTypesMapping.putIfAbsent(fieldName, bitSetRepr);
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

  private static String getTrainingIdAndVerify(SolrQueryRequest req){
    String trainingId = req.getParams().get(IndexSchema.TRAIN_ID);
    if ( trainingId==null || !trainedCoreToMostRelevantFieldTypesMapping.containsKey(trainingId) ){
      String errmsg = trainingId==null?"Param '"+IndexSchema.TRAIN_ID+"' cannot be null!.":"Unknown trainingId:"+trainingId;
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, errmsg);
    }
    return trainingId;
  }

  /**
   * Add the new fieldTypeName for the fieldName, to be decided the best fieldTypeName for this field.
   * @param req
   * @param fieldName
   * @param fieldTypeName
   */
  public static void trainSchema(SolrQueryRequest req, String fieldName, String fieldTypeName, boolean multiValued){
    String trainingId = getTrainingIdAndVerify(req);
    trainedCoreToMostRelevantFieldTypesMapping.get( trainingId )
        .addAllowedFieldType( fieldName, fieldTypeName , multiValued);
  }

  public Map<String, Object> convertTrainingMetaDataToSchemaFormat(){
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
      fieldSchema.put("fieldType", fieldType);
      fieldSchema.put("multivalued", bitsetRepr.isMultiValued);
      fieldSchemas.add(fieldSchema);
    }
    addSchemaData.put("add-field-type", fieldSchemas);
    return addSchemaData;
  }

  public static void getTrainedSchema(SolrQueryRequest req, SolrQueryResponse rsp){
    String trainingId = getTrainingIdAndVerify(req);
    MostRelavantFieldTypes mostRelavantFieldTypes = trainedCoreToMostRelevantFieldTypesMapping.get(trainingId);
    rsp.add(IndexSchema.SCHEMA, mostRelavantFieldTypes.convertTrainingMetaDataToSchemaFormat());
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

  public static String generateTrainingId(SolrCore core){
    String trainingID = UUID.randomUUID().toString();
    MostRelavantFieldTypes existingValue = null;
    do{
      existingValue = trainedCoreToMostRelevantFieldTypesMapping.putIfAbsent(trainingID, new MostRelavantFieldTypes());
    }while (existingValue!=null);
    return trainingID;
  }



}
