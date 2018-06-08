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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.List;
import java.util.Map;

import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by abhi on 08/06/18.
 */
public class APIBasedInference  {

      private static final Logger log = LoggerFactory.getLogger(LearnSchemaUpdateRequestProcessorFactory.class);

      static String endpoint = "http://localhost:5533/%s";

      public static CustomTypeInferObject predictCategory(Object value) throws IOException {

        String url = String.format(endpoint, String.valueOf(value).replaceAll(" ", "%20"));
        log.info("Url : "+url);
        System.out.println(url);

        URL completeUrl = new URL(url);
        BufferedReader in = new BufferedReader(
            new InputStreamReader(completeUrl.openStream()));

        String result=null;
        String inputLine;

        while ((inputLine = in.readLine()) != null){
          System.out.println(inputLine);
          result = inputLine;
        }


        in.close();

        Object val = ObjectBuilder.getVal(new JSONParser(result));

        Map<String, Object> ff = (Map<String, Object>) val;
        List<List<Object>> ss = (List<List<Object>>) ff.get("result");
        List<Object> rr = ss.get(0);

        CustomTypeInferObject customTypeInferObject = new CustomTypeInferObject();
        customTypeInferObject.lossScore = (Double)rr.get(0);
        customTypeInferObject.result    = (String)rr.get(1);

        if (customTypeInferObject.result.equals("price")){
          customTypeInferObject.result = "text";
        }

        return customTypeInferObject;

      }

/*
      Map<String, Object> getModelBasedType(Map<String, Object> stats){

          stats

      }
*/


      static class CustomTypeInferObject{
        public String result;
        public Double lossScore;
      }

      public static void main(String... args) throws IOException {

        predictCategory("red");
        predictCategory("cover");
        predictCategory("pattern");
        predictCategory("new york city");
        predictCategory("medium jacket");
        predictCategory("super fast car");
        predictCategory("Nice Kitty For Your House");
        predictCategory("Learning in Apache Solr");

      }

}
