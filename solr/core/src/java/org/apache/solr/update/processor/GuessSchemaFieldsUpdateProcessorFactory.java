package org.apache.solr.update.processor;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.ManagedIndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;
import static org.apache.solr.update.processor.AddSchemaFieldsUpdateProcessorFactory.parseTypeMappings;
import static org.apache.solr.update.processor.FieldMutatingUpdateProcessor.SELECT_ALL_FIELDS;

/**
 * Created by abhidemon on 10/12/17.
 */
public class GuessSchemaFieldsUpdateProcessorFactory  extends UpdateRequestProcessorFactory
    implements SolrCoreAware, UpdateRequestProcessorFactory.RunAlways  {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String TYPE_MAPPING_PARAM = "typeMapping";
  private static final String VALUE_CLASS_PARAM = "valueClass";
  private static final String FIELD_TYPE_PARAM = "fieldType";
  private static final String DEFAULT_FIELD_TYPE_PARAM = "defaultFieldType";
  private static final String COPY_FIELD_PARAM = "copyField";
  private static final String DEST_PARAM = "dest";
  private static final String MAX_CHARS_PARAM = "maxChars";
  private static final String IS_DEFAULT_PARAM = "default";

  private List<AddSchemaFieldsUpdateProcessorFactory.TypeMapping> typeMappings = Collections.emptyList();
  private FieldMutatingUpdateProcessorFactory.SelectorParams inclusions = new FieldMutatingUpdateProcessorFactory.SelectorParams();
  private Collection<FieldMutatingUpdateProcessorFactory.SelectorParams> exclusions = new ArrayList<>();
  private SolrResourceLoader solrResourceLoader = null;
  private String defaultFieldType;
  GuessSchemaFieldsUpdateProcessor gsfup;
  AddSchemaFieldsUpdateProcessorFactory addSchemaFieldsUpdateProcessorFactory = new AddSchemaFieldsUpdateProcessorFactory();


  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {

    gsfup = new GuessSchemaFieldsUpdateProcessor(next);
    gsfup.addSchemaFieldsUpdateProcessor = (AddSchemaFieldsUpdateProcessorFactory.AddSchemaFieldsUpdateProcessor)addSchemaFieldsUpdateProcessorFactory.getInstance(req, rsp, next);
    return gsfup;

  }

  @Override
  public void init(NamedList args) {
    inclusions = FieldMutatingUpdateProcessorFactory.parseSelectorParams(args);
    addSchemaFieldsUpdateProcessorFactory.validateSelectorParams(inclusions);
    inclusions.fieldNameMatchesSchemaField = false;  // Explicitly (non-configurably) require unknown field names
    exclusions = FieldMutatingUpdateProcessorFactory.parseSelectorExclusionParams(args);
    for (FieldMutatingUpdateProcessorFactory.SelectorParams exclusion : exclusions) {
      addSchemaFieldsUpdateProcessorFactory.validateSelectorParams(exclusion);
    }
    Object defaultFieldTypeParam = args.remove(DEFAULT_FIELD_TYPE_PARAM);
    if (null != defaultFieldTypeParam) {
      if ( ! (defaultFieldTypeParam instanceof CharSequence)) {
        throw new SolrException(SERVER_ERROR, "Init param '" + DEFAULT_FIELD_TYPE_PARAM + "' must be a <str>");
      }
      defaultFieldType = defaultFieldTypeParam.toString();
    }

    typeMappings = parseTypeMappings(args);
    if (null == defaultFieldType && typeMappings.stream().noneMatch(AddSchemaFieldsUpdateProcessorFactory.TypeMapping::isDefault)) {
      throw new SolrException(SERVER_ERROR, "Must specify either '" + DEFAULT_FIELD_TYPE_PARAM +
          "' or declare one typeMapping as default.");
    }

    super.init(args);
  }


  @Override
  public void inform(SolrCore core) {
    solrResourceLoader = core.getResourceLoader();

    for (AddSchemaFieldsUpdateProcessorFactory.TypeMapping typeMapping : typeMappings) {
      typeMapping.populateValueClasses(core);
    }
  }


  private class GuessSchemaFieldsUpdateProcessor extends UpdateRequestProcessor {

    AddSchemaFieldsUpdateProcessorFactory.AddSchemaFieldsUpdateProcessor addSchemaFieldsUpdateProcessor;

    public GuessSchemaFieldsUpdateProcessor(UpdateRequestProcessor next) {
      super(next);
    }



    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {

      final SolrInputDocument doc = cmd.getSolrInputDocument();
      final SolrCore core = cmd.getReq().getCore();
      IndexSchema oldSchema = cmd.getReq().getSchema();
      while (true){
        List<SchemaField> newFields = new ArrayList<>();
        Map<String,Map<Integer,List<AddSchemaFieldsUpdateProcessorFactory.CopyFieldDef>>> newCopyFields = new HashMap<>();

        Map<String,List<SolrInputField>> unknownFields = new HashMap<>();
        addSchemaFieldsUpdateProcessor.getUnknownFields(SELECT_ALL_FIELDS, doc, unknownFields);
        for (final Map.Entry<String,List<SolrInputField>> entry : unknownFields.entrySet()) {
          String fieldName = entry.getKey();
          String fieldTypeName = defaultFieldType;
          AddSchemaFieldsUpdateProcessorFactory.TypeMapping typeMapping = addSchemaFieldsUpdateProcessor.mapValueClassesToFieldType(entry.getValue());
          if (typeMapping != null) {
            fieldTypeName = typeMapping.fieldTypeName;
            if (!typeMapping.copyFieldDefs.isEmpty()) {
              newCopyFields.put(fieldName,
                  typeMapping.copyFieldDefs.stream().collect(Collectors.groupingBy(AddSchemaFieldsUpdateProcessorFactory.CopyFieldDef::getMaxChars)));
            }
          }
          newFields.add(oldSchema.newField(fieldName, fieldTypeName, Collections.<String,Object>emptyMap()));
        }
        if (newFields.isEmpty() && newCopyFields.isEmpty()) {
          // nothing to do - no fields will be added - exit from the retry loop
          log.debug("No fields or copyFields to add to the schema.");
          break;
        }

        // Need to hold the lock during the entire attempt to ensure that
        // the schema on the request is the latest
        synchronized (oldSchema.getSchemaUpdateLock()) {
          try {
            IndexSchema newSchema = oldSchema.addFields(newFields, Collections.emptyMap(), false);
            // Add copyFields
            for (String srcField : newCopyFields.keySet()) {
              for (Integer maxChars : newCopyFields.get(srcField).keySet()) {
                newSchema = newSchema.addCopyFields(srcField,
                    newCopyFields.get(srcField).get(maxChars).stream().map(f -> f.getDest(srcField)).collect(Collectors.toList()),
                    maxChars);
              }
            }
            if (null != newSchema) {
              ((ManagedIndexSchema)newSchema).persistManagedSchema(false);
              core.setLatestSchema(newSchema);
              cmd.getReq().updateSchemaToLatest();
              log.debug("Successfully added field(s) and copyField(s) to the schema.");
              break; // success - exit from the retry loop
            } else {
              throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Failed to add fields and/or copyFields.");
            }
          } catch (ManagedIndexSchema.FieldExistsException e) {
            log.error("At least one field to be added already exists in the schema - retrying.");
            oldSchema = core.getLatestSchema();
            cmd.getReq().updateSchemaToLatest();
          } catch (ManagedIndexSchema.SchemaChangedInZkException e) {
            log.debug("Schema changed while processing request - retrying.");
            oldSchema = core.getLatestSchema();
            cmd.getReq().updateSchemaToLatest();
          }
        }
      }


      super.processAdd(cmd);
    }
  }

}
