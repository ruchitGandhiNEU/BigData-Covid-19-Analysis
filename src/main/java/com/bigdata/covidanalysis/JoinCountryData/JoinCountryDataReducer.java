/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bigdata.covidanalysis.JoinCountryData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author ruchit
 */
public class JoinCountryDataReducer extends Reducer<Text, Text, Text, Text> {

    private static final Text EMPTY_TEXT = new Text();

    private List<Text> listA;
    private List<Text> listB;

    private String joinType = null;

    @Override
    protected void setup(Reducer.Context context) throws IOException, InterruptedException {
        joinType = context.getConfiguration().get("join.type");
        listA = new ArrayList();
        listB = new ArrayList();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        listA.clear();
        listB.clear();

        for (Text value : values) {
            if (value.charAt(0) == 'A') {
                listA.add(new Text(value.toString().substring(1)));
            } else if (value.charAt(0) == 'B') {
                listB.add(new Text(value.toString().substring(1)));
            }
        }

        executeJoinLogic(context);
    }

    public void executeJoinLogic(Reducer.Context context) throws IOException, InterruptedException {

        switch (joinType.toLowerCase()) {

            case "inner": {

                if (!listA.isEmpty() && !listB.isEmpty()) {
                    for (Text a : listA) {
                        for (Text b : listB) {
                            context.write(a, b);
                        }
                    }
                }

                break;
            }

            case "leftouter": {

                for (Text a : listA) {
                    if (!listB.isEmpty()) {
                        for (Text b : listB) {
                            context.write(a, b);
                        }
                    } else {
                        context.write(a, EMPTY_TEXT);
                    }
                }

                break;
            }

            case "rightouter": {

                for (Text b : listB) {
                    if (!listA.isEmpty()) {
                        for (Text a : listA) {
                            context.write(a, b);
                        }
                    } else {
                        context.write(EMPTY_TEXT, b);
                    }
                }

                break;

            }

            case "fullouter": {
                if (!listA.isEmpty()) {
                    for (Text a : listA) {
                        if (!listB.isEmpty()) {
                            for (Text b : listB) {
                                context.write(a, b);
                            }
                        } else {
                            context.write(a, EMPTY_TEXT);
                        }
                    }
                } else {
                    for (Text b : listB) {
                        context.write(EMPTY_TEXT, b);
                    }
                }

                break;

            }

            case "antijoin": {

                if (listA.isEmpty() ^ listB.isEmpty()) {
                    for (Text a : listA) {
                        context.write(a, EMPTY_TEXT);
                    }
                    for (Text b : listB) {
                        context.write(EMPTY_TEXT, b);
                    }
                }

                break;
            }

        }

    }

}
