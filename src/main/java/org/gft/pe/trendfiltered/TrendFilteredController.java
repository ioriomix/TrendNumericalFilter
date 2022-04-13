/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.gft.pe.trendfiltered;

import org.apache.streampipes.commons.exceptions.SpRuntimeException;
import org.apache.streampipes.model.DataProcessorType;
import org.apache.streampipes.model.graph.DataProcessorDescription;
import org.apache.streampipes.model.graph.DataProcessorInvocation;
import org.apache.streampipes.model.schema.PropertyScope;
import org.apache.streampipes.sdk.builder.ProcessingElementBuilder;
import org.apache.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.apache.streampipes.sdk.extractor.ProcessingElementParameterExtractor;
import org.apache.streampipes.sdk.helpers.*;
import org.apache.streampipes.sdk.utils.Assets;
import org.apache.streampipes.wrapper.siddhi.query.expression.RelationalOperator;
import org.apache.streampipes.wrapper.standalone.ConfiguredEventProcessor;
import org.apache.streampipes.wrapper.standalone.declarer.StandaloneEventProcessingDeclarer;

import java.util.List;


public class TrendFilteredController extends StandaloneEventProcessingDeclarer<TrendFilteredParameters> {

  private static final String THRESHOLD = "threshold";
  private static final String INPUT_VALUE = "value";
  private static final String OPERATION_FILTER = "operation_filter";

  private double threshold;

  private static final String INCREASE = "increase";
  private static final String OPERATION_TREND = "operation_trend";
  private static final String DURATION = "duration";


  @Override
  public DataProcessorDescription declareModel() {
    return ProcessingElementBuilder.create("org.gft.pe.trendfiltered.processor")
            .withAssets(Assets.DOCUMENTATION, Assets.ICON)
            .withLocales(Locales.EN)
            .category(DataProcessorType.AGGREGATE)

            .requiredStream(StreamRequirementsBuilder.create()
                .requiredPropertyWithUnaryMapping(EpRequirements.numberReq(),
                    Labels.withId(INPUT_VALUE), PropertyScope.MEASUREMENT_PROPERTY)
                .build())
            //.outputStrategy(OutputStrategies.keep())
            .requiredSingleValueSelection(Labels.withId(OPERATION_FILTER), Options.from("<", "<=", ">",
                    ">=", "==", "!="))
            .requiredFloatParameter(Labels.withId(THRESHOLD))

            .requiredSingleValueSelection(Labels.withId(OPERATION_TREND), Options
                    .from("Increase", "Decrease"))
            .requiredIntegerParameter(Labels.withId(INCREASE), 0, 500, 1)
            .requiredIntegerParameter(Labels.withId(DURATION))
            .outputStrategy(OutputStrategies.custom())
            .build();

  }

  @Override
  public ConfiguredEventProcessor<TrendFilteredParameters> onInvocation(DataProcessorInvocation
                                   invocationGraph, ProcessingElementParameterExtractor extractor) throws SpRuntimeException  {

    this.threshold = extractor.singleValueParameter(THRESHOLD,Double.class);

    String stringOperation = extractor.selectedSingleValue(OPERATION_FILTER, String.class);
    RelationalOperator filterOperation = RelationalOperator.GREATER_THAN;

    //String operation = "GT";

    if (stringOperation.equals("<=")) {
      filterOperation = RelationalOperator.LESSER_EQUALS;
    } else if (stringOperation.equals("<")) {
      //operation = "LT";
      filterOperation = RelationalOperator.LESSER_THAN;
    } else if (stringOperation.equals(">=")) {
      //operation = "GE";
      filterOperation = RelationalOperator.GREATER_EQUALS;
    } else if (stringOperation.equals("==")) {
      //operation = "EQ";
      filterOperation = RelationalOperator.EQUALS;
    } else if (stringOperation.equals("!=")) {
      //operation = "IE";
      filterOperation = RelationalOperator.NOT_EQUALS;
    }

    String inputValue = extractor.mappingPropertyValue(INPUT_VALUE);

    System.out.println("Parametri per TREND: ");

    String operation_trend = extractor.selectedSingleValue(OPERATION_TREND, String.class);
    int increase = extractor.singleValueParameter(INCREASE, Integer.class);
    int duration = extractor.singleValueParameter(DURATION, Integer.class);
    List<String> outputFieldSelectors = extractor.outputKeySelectors();
/*
    System.out.println("increase: " + increase);
    System.out.println("duration: " + duration);
    System.out.println("inputValue: " + inputValue);
*/
    TrendFilteredParameters params = new TrendFilteredParameters(invocationGraph,
            getOperation(operation_trend),
            increase,
            duration,
            inputValue,
            filterOperation,
            outputFieldSelectors,
            this.threshold);

    return new ConfiguredEventProcessor<>(params, Trend::new);

  }

  private TrendFilteredOperator getOperation(String operation) {
    if (operation.equals("Increase")) {
      return TrendFilteredOperator.INCREASE;
    } else {
      return TrendFilteredOperator.DECREASE;
    }
  }
}
