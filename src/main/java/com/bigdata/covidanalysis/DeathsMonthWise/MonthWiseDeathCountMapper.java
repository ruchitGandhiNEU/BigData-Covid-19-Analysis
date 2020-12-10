/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bigdata.covidanalysis.DeathsMonthWise;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author ruchit
 */
public class MonthWiseDeathCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String input = value.toString();
        String[] words = input.split(",");
        
        String date = words[3];
        String deaths = words[7];

        //logger.info("com.bigdata.covidanalysis.ContinentWiseCount.ContinentWiseCountMapper.map() " + (words[4] != null) + (words[1] != null) + words[4] + "," + words[1] );
        if (words.length > 0 && !(words[0].equalsIgnoreCase("iso_code")) && date != null &&  !date.equals("") && deaths != null && !deaths.equals("")) {
            //logger.info(words[1] + words[4]);
            String[] dateSplit = date.split("-");
            String dateMonthNum = dateSplit[1];
            String dateYear = dateSplit[0];
            
            String monthString;
        switch (dateMonthNum) {
            case "01":  monthString = "January";       break;
            case "02":  monthString = "February";      break;
            case "03":  monthString = "March";         break;
            case "04":  monthString = "April";         break;
            case "05":  monthString = "May";           break;
            case "06":  monthString = "June";          break;
            case "07":  monthString = "July";          break;
            case "08":  monthString = "August";        break;
            case "09":  monthString = "September";     break;
            case "10": monthString = "October";       break;
            case "11": monthString = "November";      break;
            case "12": monthString = "December";      break;
            default: monthString = "Invalid_month"; break;
        }
            monthString += "_"+dateYear;
            
            context.write(new Text(monthString), new IntWritable((int)Double.parseDouble(deaths)));

        }
    }
    
    
    
}
