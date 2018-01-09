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
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.SolrCore;
import org.apache.zookeeper.KeeperException;
import org.noggit.JSONUtil;

/**
 * Created by abhi on 07/01/18.
 */
public class MostRelavantFieldTypes {

  static final BitSet _string = BitSet.valueOf(new byte[]{16});
  static final BitSet _double = BitSet.valueOf(new byte[]{8});
  static final BitSet _long = BitSet.valueOf(new byte[]{4});
  static final BitSet _boolean = BitSet.valueOf(new byte[]{2});
  static final BitSet _date = BitSet.valueOf(new byte[]{1});
  static final Map<String, MostRelavantFieldTypes> cacheOfTrainedCoreToMostRelevantFieldTypesMapping = new ConcurrentHashMap<>();

  ZkStateReader zkStateReader;





  private Map<String, BitSetReprForFieldType> fieldNameToFieldTypesMapping = new ConcurrentHashMap<>();

  private static BitSet getBitSetForFieldType(String fieldTypeName){
    switch (fieldTypeName){

      case "string" : return  (BitSet)_string.clone();

      case "tlong":
      case "long"   : return (BitSet)_long.clone();

      case "tdouble" :
      case "double" : return (BitSet)_double.clone();

      case "date" : return (BitSet)_date.clone();

      case "boolean" : return (BitSet)_boolean.clone();
      default : throw new RuntimeException("No BitSetMapping found for FieldType : "+fieldTypeName);
    }
  }

  public void addAllowedFieldType(String fieldName, String fieldTypeName){

    BitSetReprForFieldType previousEntry = fieldNameToFieldTypesMapping.putIfAbsent(fieldName, new BitSetReprForFieldType(fieldTypeName));
    if (previousEntry!=null){
      previousEntry.applyOR_Oprn(fieldTypeName);
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
  private class BitSetReprForFieldType {

    BitSet bitSetRepresentation;

    BitSetReprForFieldType(String fieldTypeName){
      bitSetRepresentation = getBitSetForFieldType(fieldTypeName);
    }

    // We want an instance level lock here
    private synchronized void applyOR_Oprn(BitSet anotherFieldType){
      bitSetRepresentation.or(anotherFieldType);
    }
    // We want an instance level lock here
    void applyOR_Oprn(String anotherFieldType){
      bitSetRepresentation.or( getBitSetForFieldType(anotherFieldType) );
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
        case "12": return "double";
        case "4" : return "long";
        case "2" : return "boolean";
        case "1" : return "date";
        default: return "string";
      }

    }
    //--

  }

  private static String getPropertyPath(String coreName, String fieldName, String fieldTypeName){
    return "/train/"+coreName+"/schema/"+fieldName+"/"+fieldTypeName;
  }

  private byte[] getPropertyData(Map<String, Object> newProps) {
    if (newProps != null) {
      String propertyDataStr = JSONUtil.toJSON(newProps);
      if (propertyDataStr == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Invalid property specification");
      }
      return propertyDataStr.getBytes(StandardCharsets.UTF_8);
    }
    return null;
  }

  /**
   * Add the new fieldTypeName for the fieldName, to be decided the best fieldTypeName for this field.
   * @param core
   * @param fieldName
   * @param fieldTypeName
   */
  public static void trainSchema(SolrCore core, String fieldName, String fieldTypeName){

    String coreName = core.getCoreDescriptor().getCollectionName();
    SolrZkClient client = core.getCoreContainer().getZkController().getZkClient();
    try {
      int bute
      client.makePath(
          getPropertyPath(coreName, fieldName, fieldTypeName),
          null,



      );
    } catch (KeeperException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    cacheOfTrainedCoreToMostRelevantFieldTypesMapping.putIfAbsent(coreName, new MostRelavantFieldTypes());
    cacheOfTrainedCoreToMostRelevantFieldTypesMapping.get( coreName )
        .addAllowedFieldType( fieldName, fieldTypeName );

  }

  /**
   * Will return map with FieldName -> MostSuitableFieldName.
   * @param core
   * @return
   */
  public static Map<String, String> getTrainedSchema(SolrCore core){
    MostRelavantFieldTypes mostRelavantFieldTypes = cacheOfTrainedCoreToMostRelevantFieldTypesMapping.get(
        core.getCoreDescriptor().getCollectionName());
    return mostRelavantFieldTypes.getMostRelevantFieldTypes();
  }

/*  private static String trainCoreName(SolrCore core){
    return core.getCoreDescriptor().getCollectionName() + "_train";
  }*/



}
