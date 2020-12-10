package com.function;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 *
 * @author ruchit
 */
public final class Rank extends UDF{
    private int  counter;
    private String last_key;
    public int evaluate(final String key){
      if ( !key.equalsIgnoreCase(this.last_key) ) {
         this.counter = 0;
         this.last_key = key;
      }
      return this.counter++;
    }
}