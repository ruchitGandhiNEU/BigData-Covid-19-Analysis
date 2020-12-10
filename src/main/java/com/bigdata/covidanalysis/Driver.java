/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bigdata.covidanalysis;

import com.bigdata.covidanalysis.ContinentWiseCount.ContinentWiseCountMapper;
import com.bigdata.covidanalysis.ContinentWiseCount.ContinentWiseCountReducer;
import com.bigdata.covidanalysis.CountryWiseDeathCount.CountryWiseDeathCountMapper;
import com.bigdata.covidanalysis.CountryWiseDeathCount.CountryWiseDeathCountReducer;
import com.bigdata.covidanalysis.CountryWiseMinMax.CountryWiseMinMaxNewCasesReducer;
import com.bigdata.covidanalysis.CountryWiseMinMax.CountryWiseMinMaxMapper;
import com.bigdata.covidanalysis.CountryWiseMinMax.CustomOutputTuple;
import com.bigdata.covidanalysis.CountryWiseMinMax.MinMaxTuple;
import com.bigdata.covidanalysis.DeathsMonthWise.MonthWiseDeathCountMapper;
import com.bigdata.covidanalysis.DeathsMonthWise.MonthWiseDeathCountReducer;
import com.bigdata.covidanalysis.JoinCountryData.JoinCountryDataReducer;
import com.bigdata.covidanalysis.JoinCountryData.JoinCountryMinMaxMapper;
import com.bigdata.covidanalysis.JoinCountryData.JoinCountryTopNMapper;
import com.bigdata.covidanalysis.TopNCountriesWithDeathCount.TopNCountriesWithDeathCountMapper;
import com.bigdata.covidanalysis.TopNCountriesWithDeathCount.TopNCountriesWithDeathCountReducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author ruchit
 */
public class Driver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        // control which stages of job will run. 
        // Useful for develipment purpose so that you dont have to wait for previous stages to complete 
        // to see the output of job currently under development.
        List<Integer> developmentMode = new ArrayList<>();
        developmentMode.add(1);
        developmentMode.add(2);
        developmentMode.add(3); 
        developmentMode.add(4); // 4 dependent on output of 3.
        developmentMode.add(5);
        developmentMode.add(6);

        String covidDataInputPath = args[0];
        String ContinentWiseDeathCountOutputPath = args[1];
        String MonthWiseDeathCountJobOutputPath = args[2];
        String CountryWiseDeathCountJobOutputPath = args[3];
        String TopNCountryDeathCountJobOutputPath = args[4];
        String CountryWiseMinMaxJobOutputPath = args[5];
        String JoinCountryDataJobOutputPath = args[6];

        System.out.println("//1.Total Number of cases per continent ==============================================================================================================");
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(new Path(ContinentWiseDeathCountOutputPath))) {

            fs.delete(new Path(ContinentWiseDeathCountOutputPath), true);

        }

        Job ContinentWiseDeathCountJob = Job.getInstance(conf);

        ContinentWiseDeathCountJob.setMapperClass(ContinentWiseCountMapper.class);
        ContinentWiseDeathCountJob.setReducerClass(ContinentWiseCountReducer.class);

        ContinentWiseDeathCountJob.setCombinerClass(ContinentWiseCountReducer.class);

        ContinentWiseDeathCountJob.setJarByClass(Driver.class);

        ContinentWiseDeathCountJob.setNumReduceTasks(1);

        TextInputFormat.addInputPath(ContinentWiseDeathCountJob, new Path(covidDataInputPath));
        TextOutputFormat.setOutputPath(ContinentWiseDeathCountJob, new Path(ContinentWiseDeathCountOutputPath));

        ContinentWiseDeathCountJob.setMapOutputKeyClass(Text.class);
        ContinentWiseDeathCountJob.setMapOutputValueClass(IntWritable.class);

        ContinentWiseDeathCountJob.setOutputKeyClass(Text.class);
        ContinentWiseDeathCountJob.setOutputValueClass(IntWritable.class);

        boolean ContinentWiseDeathCountJobStatus = developmentMode.contains(1) ? ContinentWiseDeathCountJob.waitForCompletion(true) : true;

        if (!ContinentWiseDeathCountJobStatus) {
            return;
        }

        System.out.println("//2.Total Deaths Month Wise ==============================================================================================================");
        Job MonthWiseDeathCountJob = Job.getInstance(conf);

        if (fs.exists(new Path(MonthWiseDeathCountJobOutputPath))) {

            fs.delete(new Path(MonthWiseDeathCountJobOutputPath), true);

        }

        MonthWiseDeathCountJob.setMapperClass(MonthWiseDeathCountMapper.class);
        MonthWiseDeathCountJob.setReducerClass(MonthWiseDeathCountReducer.class);

        MonthWiseDeathCountJob.setCombinerClass(MonthWiseDeathCountReducer.class);

        MonthWiseDeathCountJob.setJarByClass(Driver.class);

        MonthWiseDeathCountJob.setNumReduceTasks(1);

        TextInputFormat.addInputPath(MonthWiseDeathCountJob, new Path(covidDataInputPath));
        TextOutputFormat.setOutputPath(MonthWiseDeathCountJob, new Path(MonthWiseDeathCountJobOutputPath));

        MonthWiseDeathCountJob.setMapOutputKeyClass(Text.class);
        MonthWiseDeathCountJob.setMapOutputValueClass(IntWritable.class);

        MonthWiseDeathCountJob.setOutputKeyClass(Text.class);
        MonthWiseDeathCountJob.setOutputValueClass(IntWritable.class);

        boolean MonthWiseDeathCountJobStatus = developmentMode.contains(2) ? MonthWiseDeathCountJob.waitForCompletion(true) : true;

        if (!MonthWiseDeathCountJobStatus) {
            return;
        }

        System.out.println("//3. Country Wise Death Count ==============================================================================================================");
        Job CountryWiseDeathCountJob = Job.getInstance(conf);

        if (fs.exists(new Path(CountryWiseDeathCountJobOutputPath))) {

            fs.delete(new Path(CountryWiseDeathCountJobOutputPath), true);

        }

        CountryWiseDeathCountJob.setJarByClass(Driver.class);

        CountryWiseDeathCountJob.setMapperClass(CountryWiseDeathCountMapper.class);
        CountryWiseDeathCountJob.setReducerClass(CountryWiseDeathCountReducer.class);

        CountryWiseDeathCountJob.setNumReduceTasks(1);

        TextInputFormat.addInputPath(CountryWiseDeathCountJob, new Path(covidDataInputPath));
        TextOutputFormat.setOutputPath(CountryWiseDeathCountJob, new Path(CountryWiseDeathCountJobOutputPath));

        CountryWiseDeathCountJob.setMapOutputKeyClass(Text.class);
        CountryWiseDeathCountJob.setMapOutputValueClass(IntWritable.class);

        CountryWiseDeathCountJob.setOutputKeyClass(Text.class);
        CountryWiseDeathCountJob.setOutputValueClass(IntWritable.class);

        boolean CountryWiseDeathCountJobStatus = developmentMode.contains(3) ? CountryWiseDeathCountJob.waitForCompletion(true) : true;

        System.out.println("//4. Chain Mapping and Using TopTen filtering pattern to find Top N countries with Death Count ===================================");
        boolean TopNCountriesDeathCountJobStatus = true;
        if (CountryWiseDeathCountJobStatus) {

            Job TopNCountriesDeathCountJob = Job.getInstance(conf);
            TopNCountriesDeathCountJob.setJarByClass(Driver.class);
            TopNCountriesDeathCountJob.setMapperClass(TopNCountriesWithDeathCountMapper.class);
            TopNCountriesDeathCountJob.setReducerClass(TopNCountriesWithDeathCountReducer.class);
            TopNCountriesDeathCountJob.setMapOutputKeyClass(Text.class);
            TopNCountriesDeathCountJob.setMapOutputValueClass(IntWritable.class);
            TopNCountriesDeathCountJob.setOutputKeyClass(Text.class);
            TopNCountriesDeathCountJob.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(TopNCountriesDeathCountJob, new Path(CountryWiseDeathCountJobOutputPath)); //add output of previos job as input to this one
            FileOutputFormat.setOutputPath(TopNCountriesDeathCountJob, new Path(TopNCountryDeathCountJobOutputPath));
            if (fs.exists(new Path(TopNCountryDeathCountJobOutputPath))) {

                fs.delete(new Path(TopNCountryDeathCountJobOutputPath), true);

            }
            TopNCountriesDeathCountJobStatus = developmentMode.contains(4) ? TopNCountriesDeathCountJob.waitForCompletion(true) : true;
        }

        if (!TopNCountriesDeathCountJobStatus) {
            return;
        }

        System.out.println("//5. =======================================================================");
        Job CountryWiseMinMaxJob = Job.getInstance(conf, "Summerization Patterns");

        CountryWiseMinMaxJob.setJarByClass(Driver.class);

        CountryWiseMinMaxJob.setMapOutputKeyClass(Text.class);
        CountryWiseMinMaxJob.setMapOutputValueClass(MinMaxTuple.class);

        CountryWiseMinMaxJob.setInputFormatClass(TextInputFormat.class);
        CountryWiseMinMaxJob.setOutputFormatClass(TextOutputFormat.class);

        CountryWiseMinMaxJob.setOutputKeyClass(Text.class);
        CountryWiseMinMaxJob.setOutputValueClass(CustomOutputTuple.class);

        CountryWiseMinMaxJob.setMapperClass(CountryWiseMinMaxMapper.class);
        CountryWiseMinMaxJob.setReducerClass(CountryWiseMinMaxNewCasesReducer.class);

        TextInputFormat.addInputPath(CountryWiseMinMaxJob, new Path(covidDataInputPath));

        TextOutputFormat.setOutputPath(CountryWiseMinMaxJob, new Path(CountryWiseMinMaxJobOutputPath));

        if (fs.exists(new Path(CountryWiseMinMaxJobOutputPath))) {
            fs.delete(new Path(CountryWiseMinMaxJobOutputPath), true);
        }

        boolean CountryWiseMinMaxJobStatus = developmentMode.contains(5) ? CountryWiseMinMaxJob.waitForCompletion(true) : true;

        if (!CountryWiseMinMaxJobStatus) {
            return;
        }

        System.out.println("// 6. JOIN =======================================================");
        boolean JoinCountryDataJobStatus = false;
        if (CountryWiseMinMaxJobStatus && TopNCountriesDeathCountJobStatus) {

            Job JoinCountryDataJob = Job.getInstance(conf);

            JoinCountryDataJob.setJarByClass(Driver.class);
            JoinCountryDataJob.setOutputKeyClass(Text.class);
            JoinCountryDataJob.setOutputValueClass(Text.class);
            JoinCountryDataJob.setReducerClass(JoinCountryDataReducer.class);

            MultipleInputs.addInputPath(JoinCountryDataJob, new Path(TopNCountryDeathCountJobOutputPath), TextInputFormat.class, JoinCountryTopNMapper.class);
            MultipleInputs.addInputPath(JoinCountryDataJob, new Path(CountryWiseMinMaxJobOutputPath), CombineTextInputFormat.class, JoinCountryMinMaxMapper.class);

            JoinCountryDataJob.getConfiguration().set("join.type", "leftouter");
            FileOutputFormat.setOutputPath(JoinCountryDataJob, new Path(JoinCountryDataJobOutputPath));

            if (fs.exists(new Path(JoinCountryDataJobOutputPath))) {
                fs.delete(new Path(JoinCountryDataJobOutputPath), true);
            }
            
            System.out.println("Going to run JoinCountryDataJob");

            JoinCountryDataJobStatus = developmentMode.contains(6) ? JoinCountryDataJob.waitForCompletion(true) : true;

        }
         if (!JoinCountryDataJobStatus) {
            return;
        }

    }

}
