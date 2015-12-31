package com.code.stash.hadoop.movies.reducers;

import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.code.stash.hadoop.movies.common.TextPair;

public class MovieMinRatingReducer extends Reducer<TextPair, TextPair, Text, Text> {
      Text movieName = new Text();
      Text movieRating = new Text();
      
      @Override
      public void reduce(TextPair key, Iterable<TextPair> values, Context context) 
         throws java.io.IOException, InterruptedException {
    	  double min = Double.MAX_VALUE;
    	  Iterator<TextPair> iterator = values.iterator();

          if (iterator.hasNext()) {

          	 TextPair firstPair = iterator.next(); 
             if (firstPair.left.toString().equals("T")) {
                 
            	 movieName.set(firstPair.right);
             }
             if("".equals(movieName.toString())){
       		  return;
       	     }
          } 
    	  
          while(iterator.hasNext()){
        	  TextPair tp = iterator.next();
        	  if(Double.parseDouble(tp.right.toString()) < min){
        		  min = Double.parseDouble(tp.right.toString());
        	  }
          }
          movieRating.set(min +"");
          context.write(movieName, movieRating);
      }
}
