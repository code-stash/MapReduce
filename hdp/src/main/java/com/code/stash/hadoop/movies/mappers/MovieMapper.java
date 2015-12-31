package com.code.stash.hadoop.movies.mappers;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.code.stash.hadoop.movies.common.TextPair;

public class MovieMapper extends Mapper<LongWritable, Text, TextPair, TextPair> {

	TextPair oKey = new TextPair();
	TextPair oVal = new TextPair();

	@Override
	public void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
		String[] tokens = StringUtils.split(value.toString(), ",");
		if (tokens.length == 3 || tokens.length == 4) {
			
			if(!isNumeric(tokens[0])){
				return;
			}
			oKey.set(tokens[0], "1"); // set movie id
			oVal.set("T", tokens[1]); // set Title
			context.write(oKey, oVal);
		}
		
		
	}

	public static boolean isNumeric(String str) {
		return str.matches("-?\\d+(\\.\\d+)?"); // match a number with optional
												// '-' and decimal.
	}

}
