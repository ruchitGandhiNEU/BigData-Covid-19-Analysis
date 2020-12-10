/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bigdata.covidanalysis.TopNCountriesWithDeathCount;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author ruchit
 */
public class TopNCountriesWithDeathCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    private TreeMap<Integer, String> tMap;
    private int N ;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        tMap = new TreeMap<>(Collections.reverseOrder());
        N =10;
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
         String country = key.toString();
        int deathCount = 0;

        for (IntWritable val : values) {
            deathCount = val.get();
            tMap.put(deathCount, country);

            if (tMap.size() > N) {
                tMap.remove(tMap.firstKey());
            }

        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        
        for (Map.Entry<Integer, String> entry : tMap.entrySet()) {
            int deathCount = entry.getKey();
            String country = entry.getValue();
            context.write(new Text(country), new IntWritable(deathCount));
        }
    }
    
    
    
    
    
}
