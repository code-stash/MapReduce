package com.code.stash.hadoop.movies.common;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;

public class MapInput {
	
	private org.apache.hadoop.fs.Path inputPath;
	
	private Class<? extends InputFormat> inputFormatClass;
    
	private Class<? extends Mapper> mapperClass;

	public Path getInputPath() {
		return inputPath;
	}

	public void setInputPath(Path inputPath) {
		this.inputPath = inputPath;
	}

	public Class<? extends InputFormat> getInputFormatClass() {
		return inputFormatClass;
	}

	public void setInputFormatClass(Class<? extends InputFormat> inputFormatClass) {
		this.inputFormatClass = inputFormatClass;
	}

	public Class<? extends Mapper> getMapperClass() {
		return mapperClass;
	}

	public void setMapperClass(Class<? extends Mapper> mapperClass) {
		this.mapperClass = mapperClass;
	}

}
