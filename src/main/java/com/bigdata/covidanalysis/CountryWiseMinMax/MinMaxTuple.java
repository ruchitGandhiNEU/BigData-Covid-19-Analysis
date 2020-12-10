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
public class MinMaxTuple implements Writable{

    private Double min;
    private Double max;
    private long count;

    public MinMaxTuple() {
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
    
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(min);
        out.writeDouble(max);
        out.writeLong(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        setMin(in.readDouble());
        setMax(in.readDouble());
        setCount(in.readLong());
    }

    @Override
    public String toString() {
       return getMax() + "\t" + getMin() + "\t" + getCount();
    }
    
    
    
}
