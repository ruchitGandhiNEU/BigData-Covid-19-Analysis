/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bigdata.covidanalysis.TopNCountriesWithDeathCount;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author ruchit
 */
public class TopNCountriesWithDeathCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    
     private TreeMap<Integer, String> tMap;
     private int N ;
    
     @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        tMap = new TreeMap<>();
        N = 10;
    }
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] tokens = value.toString().split("\\t");
        
        String country = tokens[0];
        String deaths = tokens[1];
        
        if(!country.isEmpty() && !deaths.isEmpty()){
            int deathCount = Integer.parseInt(deaths);
            
            tMap.put(deathCount,country);
            
            if(tMap.size()>N){
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
