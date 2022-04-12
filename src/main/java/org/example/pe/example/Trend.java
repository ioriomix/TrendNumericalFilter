package org.example.pe.example;

import org.apache.streampipes.wrapper.siddhi.SiddhiAppConfig;
import org.apache.streampipes.wrapper.siddhi.SiddhiAppConfigBuilder;
import org.apache.streampipes.wrapper.siddhi.SiddhiQueryBuilder;
import org.apache.streampipes.wrapper.siddhi.engine.SiddhiEventEngine;
import org.apache.streampipes.wrapper.siddhi.engine.callback.SiddhiDebugCallback;
import org.apache.streampipes.wrapper.siddhi.model.SiddhiProcessorParams;
import org.apache.streampipes.wrapper.siddhi.query.FromClause;
import org.apache.streampipes.wrapper.siddhi.query.InsertIntoClause;
import org.apache.streampipes.wrapper.siddhi.query.SelectClause;
import org.apache.streampipes.wrapper.siddhi.query.expression.*;
import org.apache.streampipes.wrapper.siddhi.query.expression.pattern.PatternCountOperator;


import java.util.List;
public class Trend extends SiddhiEventEngine<TrendFilteredParameters> {

    public Trend() {
        super();
    }


    public Trend(SiddhiDebugCallback callback) {
        super(callback);
    }

    public FromClause fromStatement(SiddhiProcessorParams<TrendFilteredParameters> siddhiParams) {
        TrendFilteredParameters trendParameters = siddhiParams.getParams();

        String inputValue = prepareName(trendParameters.getInputValue());
        int duration = trendParameters.getDuration();
        double increase = trendParameters.getIncrease();
        increase = (increase / 100) + 1;
        RelationalOperator operator = trendParameters.getFilterOperation();
        Double threshold = trendParameters.getThreshold();


        System.out.println("increase: " + increase);
        System.out.println("duration: " + duration);
        System.out.println("inputValue: " + inputValue);

        // String filteredFieldSelector = siddhiParams.getParams().extractor().mappingPropertyValue(LIST_KEY);

        FromClause fromClause = FromClause.create();

        Expression filter = new RelationalOperatorExpression(operator, Expressions.property("e1", inputValue), Expressions.staticValue(threshold));

        System.out.println("filter.toSiddhiEpl(): " + filter.toSiddhiEpl());

/*
        StreamExpression exp1 = Expressions.filter(Expressions.stream("e1", siddhiParams.getInputStreamNames().get(0)),filter);
        StreamExpression exp2 = Expressions.filter(Expressions.stream("e2", siddhiParams.getInputStreamNames().get(0)),filter);

*/
        StreamExpression exp1 = Expressions.every(Expressions.stream("e1", siddhiParams.getInputStreamNames().get(0)));
        StreamExpression exp2 = Expressions.stream("e2", siddhiParams.getInputStreamNames().get(0));

        PropertyExpressionBase mathExp = trendParameters.getOperation() == TrendFilteredOperator.INCREASE ?
                Expressions.divide(Expressions.property(inputValue), Expressions.staticValue(increase)) :
                Expressions.multiply(Expressions.property(inputValue), Expressions.staticValue(increase));

        System.out.println("mathExp.toSiddhiEpl(): " + mathExp.toSiddhiEpl());

        RelationalOperatorExpression opExp = trendParameters.getOperation() == TrendFilteredOperator.INCREASE ?
                Expressions.le(Expressions.property("e1", inputValue), mathExp) :
                Expressions.ge(Expressions.property("e1", inputValue), mathExp);

        System.out.println("opExp.toSiddhiEpl(): " + opExp.toSiddhiEpl());

        StreamFilterExpression filterExp = Expressions.filter(exp2, Expressions.patternCount(1, PatternCountOperator.EXACTLY_N), opExp);

        System.out.println("filterExp.toSiddhiEpl(): " + filterExp.toSiddhiEpl());

        Expression sequence = (Expressions.sequence(exp1, filterExp, Expressions.within(duration, SiddhiTimeUnit.SECONDS)));

        fromClause.add(sequence);
        System.out.println("fromClause: " + fromClause.toSiddhiEpl());
        return fromClause;
    }

    private SelectClause selectStatement(SiddhiProcessorParams<TrendFilteredParameters> siddhiParams) {
        SelectClause selectClause = SelectClause.create();
        List<String> outputFieldSelectors = siddhiParams.getParams().getOutputFieldSelectors();
        outputFieldSelectors.forEach(outputFieldSelector -> selectClause.addProperty(Expressions.property("e2", outputFieldSelector, "last")));

        //outputFieldSelectors.forEach(System.out::println);

        return selectClause;
    }

    @Override
    public SiddhiAppConfig makeStatements(SiddhiProcessorParams<TrendFilteredParameters> siddhiParams,
                                          String finalInsertIntoStreamName) {


        InsertIntoClause insertIntoClause = InsertIntoClause.create(finalInsertIntoStreamName);

        return SiddhiAppConfigBuilder
                .create()
                .addQuery(SiddhiQueryBuilder
                        .create(fromStatement(siddhiParams), insertIntoClause)
                        .withSelectClause(selectStatement(siddhiParams))
                        .build())
                .build();
    }

}
