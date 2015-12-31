package com.code.stash.hadoop.movies.common;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;

public class MovieRatingComparator implements RawComparator<TextPair> {



@Override
public int compare(TextPair o1, TextPair o2) {
	return o1.left.compareTo(o2.left);
}

@Override
public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	DataInputBuffer buffer = new DataInputBuffer();
	TextPair o1 = new TextPair();
	TextPair o2 = new TextPair();
  	try {
    	buffer.reset(b1, s1, l1);
    	o1.readFields(buffer);
    	buffer.reset(b2, s2, l2);
    	o2.readFields(buffer);
    	return compare(o1,o2);  
  	} 
  	catch(Exception ex) {
    	return -1;
  	}
}
}
