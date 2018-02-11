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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.IndexSchema;

import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;

/**
 * Created by abhi on 22/01/18.
 */
public class SchemaMutatingUpdateRequestProcessorFactory  {

  public static final String DEFAULT_FIELD_TYPE_PARAM = "defaultFieldType";
  private static final String TYPE_MAPPING_PARAM = "typeMapping";
  private static final String VALUE_CLASS_PARAM = "valueClass";
  private static final String FIELD_TYPE_PARAM = "fieldType";
  private static final String COPY_FIELD_PARAM = "copyField";
  private static final String DEST_PARAM = "dest";
  private static final String MAX_CHARS_PARAM = "maxChars";
  private static final String IS_DEFAULT_PARAM = "default";

  public static class TypeMapping {
    public String fieldTypeName;
    public Collection<String> valueClassNames;
    public Collection<CopyFieldDef> copyFieldDefs;
    public Set<Class<?>> valueClasses;
    public Boolean isDefault;

    public TypeMapping(String fieldTypeName, Collection<String> valueClassNames, boolean isDefault,
                       Collection<CopyFieldDef> copyFieldDefs) {
      this.fieldTypeName = fieldTypeName;
      this.valueClassNames = valueClassNames;
      this.isDefault = isDefault;
      this.copyFieldDefs = copyFieldDefs;
      // this.valueClasses population is delayed until the schema is available
    }

    public void populateValueClasses(SolrCore core) {
      IndexSchema schema = core.getLatestSchema();
      ClassLoader loader = core.getResourceLoader().getClassLoader();
      if (null == schema.getFieldTypeByName(fieldTypeName)) {
        throw new SolrException(SERVER_ERROR, "fieldType '" + fieldTypeName + "' not found in the schema");
      }
      valueClasses = new HashSet<>();
      for (String valueClassName : valueClassNames) {
        try {
          valueClasses.add(loader.loadClass(valueClassName));
        } catch (ClassNotFoundException e) {
          throw new SolrException(SERVER_ERROR,
              "valueClass '" + valueClassName + "' not found for fieldType '" + fieldTypeName + "'");
        }
      }
    }

    public boolean isDefault() {
      return isDefault;
    }
  }

  public static class CopyFieldDef {
    private final String destGlob;
    private final Integer maxChars;

    public CopyFieldDef(String destGlob, Integer maxChars) {
      this.destGlob = destGlob;
      this.maxChars = maxChars;
      if (destGlob.contains("*") && (!destGlob.startsWith("*") && !destGlob.endsWith("*"))) {
        throw new SolrException(SERVER_ERROR, "dest '" + destGlob +
            "' is invalid. Must either be a plain field name or start or end with '*'");
      }
    }

    public Integer getMaxChars() {
      return maxChars;
    }

    public String getDest(String srcFieldName) {
      if (!destGlob.contains("*")) {
        return destGlob;
      } else if (destGlob.startsWith("*")) {
        return srcFieldName + destGlob.substring(1);
      } else {
        return destGlob.substring(0,destGlob.length()-1) + srcFieldName;
      }
    }
  }

  public static String getDefaultFieldType(NamedList args){
    Object defaultFieldTypeParam = args.remove(DEFAULT_FIELD_TYPE_PARAM);
    if (null != defaultFieldTypeParam) {
      if ( ! (defaultFieldTypeParam instanceof CharSequence)) {
        throw new SolrException(SERVER_ERROR, "Init param '" + DEFAULT_FIELD_TYPE_PARAM + "' must be a <str>");
      }
      return defaultFieldTypeParam.toString();
    }
    return null;
  }

  /**
   * Recursively find unknown fields in the given doc and its child documents, if any.
   */
  public static void getUnknownFields
  (FieldMutatingUpdateProcessor.FieldNameSelector selector, SolrInputDocument doc, Map<String,List<SolrInputField>> unknownFields) {
    for (final String fieldName : doc.getFieldNames()) {
      if (selector.shouldMutate(fieldName)) { // returns false if the field already exists in the current schema
        List<SolrInputField> solrInputFields = unknownFields.get(fieldName);
        if (null == solrInputFields) {
          solrInputFields = new ArrayList<>();
          unknownFields.put(fieldName, solrInputFields);
        }
        solrInputFields.add(doc.getField(fieldName));
      }
    }
    List<SolrInputDocument> childDocs = doc.getChildDocuments();
    if (null != childDocs) {
      for (SolrInputDocument childDoc : childDocs) {
        getUnknownFields(selector, childDoc, unknownFields);
      }
    }
  }

  /**
   * Maps all given field values' classes to a typeMapping object
   *
   * @param fields one or more (same-named) field values from one or more documents
   */
  public static TypeMapping mapValueClassesToFieldType(List<SolrInputField> fields, List<TypeMapping> typeMappings) {
    NEXT_TYPE_MAPPING: for (TypeMapping typeMapping : typeMappings) {
      for (SolrInputField field : fields) {
        NEXT_FIELD_VALUE: for (Object fieldValue : field.getValues()) {
          for (Class<?> valueClass : typeMapping.valueClasses) {
            if (valueClass.isInstance(fieldValue)) {
              continue NEXT_FIELD_VALUE;
            }
          }
          // This fieldValue is not an instance of any of the mapped valueClass-s,
          // so mapping fails - go try the next type mapping.
          continue NEXT_TYPE_MAPPING;
        }
      }
      // Success! Each of this field's values is an instance of a mapped valueClass
      return typeMapping;
    }
    // At least one of this field's values is not an instance of any of the mapped valueClass-s
    // Return the typeMapping marked as default, if we have one, else return null to use fallback type
    List<TypeMapping> defaultMappings = typeMappings.stream().filter(TypeMapping::isDefault).collect(Collectors.toList());
    if (defaultMappings.size() > 1) {
      throw new SolrException(SERVER_ERROR, "Only one typeMapping can be default");
    } else if (defaultMappings.size() == 1) {
      return defaultMappings.get(0);
    } else {
      return null;
    }
  }

  public static List<TypeMapping> parseTypeMappings(NamedList args) {
    List<TypeMapping> typeMappings = new ArrayList<>();
    List<Object> typeMappingsParams = args.getAll(TYPE_MAPPING_PARAM);
    for (Object typeMappingObj : typeMappingsParams) {
      if (null == typeMappingObj) {
        throw new SolrException(SERVER_ERROR, "'" + TYPE_MAPPING_PARAM + "' init param cannot be null");
      }
      if ( ! (typeMappingObj instanceof NamedList) ) {
        throw new SolrException(SERVER_ERROR, "'" + TYPE_MAPPING_PARAM + "' init param must be a <lst>");
      }
      NamedList typeMappingNamedList = (NamedList)typeMappingObj;

      Object fieldTypeObj = typeMappingNamedList.remove(FIELD_TYPE_PARAM);
      if (null == fieldTypeObj) {
        throw new SolrException(SERVER_ERROR,
            "Each '" + TYPE_MAPPING_PARAM + "' <lst/> must contain a '" + FIELD_TYPE_PARAM + "' <str>");
      }
      if ( ! (fieldTypeObj instanceof CharSequence)) {
        throw new SolrException(SERVER_ERROR, "'" + FIELD_TYPE_PARAM + "' init param must be a <str>");
      }
      if (null != typeMappingNamedList.get(FIELD_TYPE_PARAM)) {
        throw new SolrException(SERVER_ERROR,
            "Each '" + TYPE_MAPPING_PARAM + "' <lst/> may contain only one '" + FIELD_TYPE_PARAM + "' <str>");
      }
      String fieldType = fieldTypeObj.toString();

      Collection<String> valueClasses
          = typeMappingNamedList.removeConfigArgs(VALUE_CLASS_PARAM);
      if (valueClasses.isEmpty()) {
        throw new SolrException(SERVER_ERROR,
            "Each '" + TYPE_MAPPING_PARAM + "' <lst/> must contain at least one '" + VALUE_CLASS_PARAM + "' <str>");
      }

      // isDefault (optional)
      Boolean isDefault = false;
      Object isDefaultObj = typeMappingNamedList.remove(IS_DEFAULT_PARAM);
      if (null != isDefaultObj) {
        if ( ! (isDefaultObj instanceof Boolean)) {
          throw new SolrException(SERVER_ERROR, "'" + IS_DEFAULT_PARAM + "' init param must be a <bool>");
        }
        if (null != typeMappingNamedList.get(IS_DEFAULT_PARAM)) {
          throw new SolrException(SERVER_ERROR,
              "Each '" + COPY_FIELD_PARAM + "' <lst/> may contain only one '" + IS_DEFAULT_PARAM + "' <bool>");
        }
        isDefault = Boolean.parseBoolean(isDefaultObj.toString());
      }

      Collection<CopyFieldDef> copyFieldDefs = new ArrayList<>();
      while (typeMappingNamedList.get(COPY_FIELD_PARAM) != null) {
        Object copyFieldObj = typeMappingNamedList.remove(COPY_FIELD_PARAM);
        if ( ! (copyFieldObj instanceof NamedList)) {
          throw new SolrException(SERVER_ERROR, "'" + COPY_FIELD_PARAM + "' init param must be a <lst>");
        }
        NamedList copyFieldNamedList = (NamedList)copyFieldObj;
        // dest
        Object destObj = copyFieldNamedList.remove(DEST_PARAM);
        if (null == destObj) {
          throw new SolrException(SERVER_ERROR,
              "Each '" + COPY_FIELD_PARAM + "' <lst/> must contain a '" + DEST_PARAM + "' <str>");
        }
        if ( ! (destObj instanceof CharSequence)) {
          throw new SolrException(SERVER_ERROR, "'" + COPY_FIELD_PARAM + "' init param must be a <str>");
        }
        if (null != copyFieldNamedList.get(COPY_FIELD_PARAM)) {
          throw new SolrException(SERVER_ERROR,
              "Each '" + COPY_FIELD_PARAM + "' <lst/> may contain only one '" + COPY_FIELD_PARAM + "' <str>");
        }
        String dest = destObj.toString();
        // maxChars (optional)
        Integer maxChars = 0;
        Object maxCharsObj = copyFieldNamedList.remove(MAX_CHARS_PARAM);
        if (null != maxCharsObj) {
          if ( ! (maxCharsObj instanceof Integer)) {
            throw new SolrException(SERVER_ERROR, "'" + MAX_CHARS_PARAM + "' init param must be a <int>");
          }
          if (null != copyFieldNamedList.get(MAX_CHARS_PARAM)) {
            throw new SolrException(SERVER_ERROR,
                "Each '" + COPY_FIELD_PARAM + "' <lst/> may contain only one '" + MAX_CHARS_PARAM + "' <str>");
          }
          maxChars = Integer.parseInt(maxCharsObj.toString());
        }
        copyFieldDefs.add(new CopyFieldDef(dest, maxChars));
      }
      typeMappings.add(new TypeMapping(fieldType, valueClasses, isDefault, copyFieldDefs));

      if (0 != typeMappingNamedList.size()) {
        throw new SolrException(SERVER_ERROR,
            "Unexpected '" + TYPE_MAPPING_PARAM + "' init sub-param(s): '" + typeMappingNamedList.toString() + "'");
      }
      args.remove(TYPE_MAPPING_PARAM);
    }
    return typeMappings;
  }




}
