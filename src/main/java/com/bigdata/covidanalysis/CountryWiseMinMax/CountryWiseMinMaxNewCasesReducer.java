/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bigdata.covidanalysis.CountryWiseMinMax;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author ruchit
 */
public class CountryWiseMinMaxNewCasesReducer extends Reducer<Text, MinMaxTuple, Text, CustomOutputTuple>{
    
    CustomOutputTuple result = new CustomOutputTuple();
    List<Double> newCases = new ArrayList<>();

    @Override
    protected void reduce(Text key, Iterable<MinMaxTuple> values, Context context) throws IOException, InterruptedException {
        result.setMin(null);
        result.setMax(null);
        result.setCount(0);
        result.setMedian(null);
        result.setStdDev(null);
        
        long count = 0;
        Double sum = 0.0;
        
        for (MinMaxTuple minMaxTuple : values) {
            sum += minMaxTuple.getMax();
            newCases.add(minMaxTuple.getMax());
            
            if(result.getMin() == null || result.getMin() > minMaxTuple.getMin()){
                result.setMin(minMaxTuple.getMin());
            }
            
            if(result.getMax() == null || result.getMax() < minMaxTuple.getMax()){
                result.setMax(minMaxTuple.getMax());
            }
            
            count += minMaxTuple.getCount();
        }
            result.setCount(count);
        Collections.sort(newCases);
        int len = newCases.size();
        
        if(len%2!=0){
            result.setMedian(newCases.get(len/2));
        }else{
            result.setMedian((newCases.get((len-1)/2)+newCases.get((len/2)))/2.0);
        }
        double mean = sum/count;
        double stdDev = 0.0;
        
        for(Double s : newCases){
            stdDev += (s-mean)*(s-mean);
        }
        
        result.setStdDev(Math.sqrt(stdDev/len));
        
        context.write(key, result);
    }
    
    
    
    
}
