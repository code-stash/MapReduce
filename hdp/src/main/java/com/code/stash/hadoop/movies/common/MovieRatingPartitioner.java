package com.code.stash.hadoop.movies.common;

import org.apache.hadoop.mapreduce.Partitioner;


public class MovieRatingPartitioner extends Partitioner<TextPair, Object>  {

	@Override
	public int getPartition(TextPair key, Object value, int numPartitions) {
		 return (key.left.hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}
