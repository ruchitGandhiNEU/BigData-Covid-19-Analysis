/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bigdata.covidanalysis.ContinentWiseCount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.FileAppender;

/**
 *
 * @author ruchit
 */
public class ContinentWiseCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

     private Logger logger = Logger.getLogger(ContinentWiseCountMapper.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      SimpleLayout layout = new SimpleLayout();    
      FileAppender appender = new FileAppender(layout,"/home/ruchit/hdlogs.log",false);    
      logger.addAppender(appender);

      logger.setLevel((Level) Level.DEBUG);
    }
     
     

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String input = value.toString();
        String[] words = input.split(",");

        //logger.info("com.bigdata.covidanalysis.ContinentWiseCount.ContinentWiseCountMapper.map() " + (words[4] != null) + (words[1] != null) + words[4] + "," + words[1] );
        if (words.length > 0 && !(words[0].equalsIgnoreCase("iso_code")) && words[4] != null &&  !words[4].equals("") && words[1] != null && !words[1].equals("")) {
            //logger.info(words[1] + words[4]);
            context.write(new Text(words[1]), new IntWritable((int)(Double.parseDouble(words[4]))));

        }

    }

}
