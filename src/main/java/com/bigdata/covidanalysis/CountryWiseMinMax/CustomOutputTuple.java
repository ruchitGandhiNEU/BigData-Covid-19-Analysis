/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bigdata.covidanalysis.CountryWiseMinMax;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author ruchit
 */
public class CustomOutputTuple implements Writable{
    
    private Double min;
    private Double max;
    private long count;
    private Double median;
    private Double stdDev;
    
    public CustomOutputTuple() {
    }

    public CustomOutputTuple(Double min, Double max, long count, Double median, Double stdDev) {
        this.min = min;
        this.max = max;
        this.count = count;
        this.median = median;
        this.stdDev = stdDev;
    }

    public Double getMin() {
        return min;
    }

    public void setMin(Double min) {
        this.min = min;
    }

    public Double getMax() {
        return max;
    }

    public void setMax(Double max) {
        this.max = max;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public Double getMedian() {
        return median;
    }

    public void setMedian(Double median) {
        this.median = median;
    }

    public Double getStdDev() {
        return stdDev;
    }

    public void setStdDev(Double stdDev) {
        this.stdDev = stdDev;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(min);
        out.writeDouble(max);
        out.writeLong(count);
        out.writeDouble(median);
        out.writeDouble(stdDev);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        setMin(in.readDouble());
        setMax(in.readDouble());
        setCount(in.readLong());
        setMedian(in.readDouble());
        setStdDev(in.readDouble());
    }

    @Override
    public String toString() {
        return min + "\t" + max + "\t" + count + "\t" + median + "\t" + stdDev;
    }

    
    
    

    
    
    
    
}
