/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bigdata.covidanalysis.CountryWiseMinMax;

import com.bigdata.covidanalysis.ContinentWiseCount.ContinentWiseCountMapper;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

/**
 *
 * @author ruchit
 */
public class CountryWiseMinMaxMapper extends Mapper<LongWritable, Text, Text, MinMaxTuple> {

    public MinMaxTuple minMaxTuple;
    Text country;

    private Logger logger = Logger.getLogger(ContinentWiseCountMapper.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        minMaxTuple = new MinMaxTuple();
        country = new Text();

        SimpleLayout layout = new SimpleLayout();
        FileAppender appender = new FileAppender(layout, "/home/ruchit/hdlogs.log", false);
        logger.addAppender(appender);

        logger.setLevel((Level) Level.DEBUG);

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        if (key == new LongWritable(1)) { //skip the headers
            return;
        }

        String input = value.toString();
        String[] tokens = input.split(",", -1);

        Double newCases = 0.0;

        if (tokens.length > 0 && !tokens[0].equalsIgnoreCase("iso_code") && tokens[5] != null && tokens[2] != null && tokens[5].length() > 0 && tokens[2].length() > 0) {
//            logger.info("===========================================");
//            logger.info(input);
//            logger.info(minMaxTuple);
//            logger.info("===========================================");
            
            newCases = Double.parseDouble(tokens[5]);
            //Only consider positive cases greater than 0
            if(newCases<=0){
                return;
            }
            minMaxTuple.setMin(newCases);
            minMaxTuple.setMax(newCases);
            minMaxTuple.setCount(1);
            country.set(tokens[2]);

            context.write(country, minMaxTuple);
        }

    }

}
