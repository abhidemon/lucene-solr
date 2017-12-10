package org.apache.solr.update.processor;

import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.plugin.SolrCoreAware;

/**
 * Created by abhidemon on 10/12/17.
 */
public class GuessSchemaFieldsUpdateProcessorFactory extends UpdateRequestProcessorFactory
    implements SolrCoreAware, UpdateRequestProcessorFactory.RunAlways {

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    return new GuessSchemaFieldsUpdateProcessor(next);
  }

  @Override
  public void inform(SolrCore core) {

  }


  private class GuessSchemaFieldsUpdateProcessor extends UpdateRequestProcessor {

    public GuessSchemaFieldsUpdateProcessor(UpdateRequestProcessor next) {
      super(next);
    }



  }

}
