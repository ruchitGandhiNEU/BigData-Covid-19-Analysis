/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bigdata.covidanalysis.CountryWiseDeathCount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author ruchit
 */
public class CountryWiseDeathCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>  {
    
      @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String input = value.toString();
        String[] words = input.split(",");
        
        String country = words[2];
        String deaths = words[7];

        
        if (words.length > 0 && !(words[0].equalsIgnoreCase("iso_code")) && country != null &&  !country.equals("") && deaths != null && !deaths.equals("")) {
        
            context.write(new Text(country), new IntWritable((int)(Double.parseDouble(deaths))));

        }

    }
}
