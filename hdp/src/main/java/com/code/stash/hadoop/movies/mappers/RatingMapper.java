package com.code.stash.hadoop.movies.mappers;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.code.stash.hadoop.movies.common.TextPair;

public class RatingMapper extends Mapper<LongWritable, Text, TextPair, TextPair> {
	
	TextPair oKey = new TextPair();
	TextPair oVal = new TextPair();

	@Override
	public void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
		String[] tokens = StringUtils.split(value.toString(), ",");
		if (tokens.length == 4) {
			
			if(!isNumeric(tokens[1])){
				return;
			}
			oKey.set(tokens[1], "2"); // set movie id
			oVal.set("R", tokens[2]); // set Rating
			context.write(oKey, oVal);
		}
	}

	public static boolean isNumeric(String str) {
		return str.matches("-?\\d+(\\.\\d+)?"); // match a number with optional
												// '-' and decimal.
	}

}
