package org.gft.pe.trendfiltered;

import org.apache.streampipes.wrapper.params.binding.EventProcessorBindingParams;
import org.apache.streampipes.model.graph.DataProcessorInvocation;
import org.apache.streampipes.wrapper.siddhi.query.expression.RelationalOperator;

import java.util.List;


public class TrendFilteredParameters extends EventProcessorBindingParams {
    private RelationalOperator filterOperation;
    private String inputValue;
    private double threshold;
    private TrendFilteredOperator operation;
    private int increase;
    private int duration;

    private List<String> outputFieldSelectors;


    public TrendFilteredParameters(DataProcessorInvocation invocationGraph,
                                   TrendFilteredOperator operation, int increase, int duration,
                                   String inputValue, RelationalOperator filterOperation, List<String> outputFieldSelectors,double threshold) {
        super(invocationGraph);
        this.operation = operation;
        this.increase = increase;
        this.duration = duration;
        this.inputValue = inputValue;
        this.outputFieldSelectors = outputFieldSelectors;
        this.filterOperation = filterOperation;
        this.threshold = threshold;
    }

    public TrendFilteredOperator getOperation() {
        return operation;
    }

    public int getIncrease() {

        return increase;
    }

    public int getDuration() {
        return duration;
    }

    public String getInputValue() {
        return inputValue;
    }

    public double getThreshold() {
        return threshold;
    }

    public List<String> getOutputFieldSelectors() {
        return outputFieldSelectors;
    }

    public RelationalOperator getFilterOperation() {
        return filterOperation;
    }
}
