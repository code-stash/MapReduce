package com.code.stash.hadoop.movies.common;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class JobBuilder {
	
	private List<Path> outputPaths;
	private Class<?> driverClass;
	
	private Configuration configuration;

	@SuppressWarnings("rawtypes")
	private Class<? extends Partitioner> partitionerClass;

	@SuppressWarnings("rawtypes")
	private Class<? extends RawComparator> groupingComparator;

	@SuppressWarnings("rawtypes")
	private Class<? extends Reducer> reducer;


	private int numMapReduceTasks;
	private Class<?> outputKeyClass;
	private Class<?> outputValueClass;
	private Class<?> mapOutputKeyClass;
	private Class<?> mapOutputValueClass;
	private MapList mappers;
	
	public JobBuilder setConfiguration(Configuration configuration) {
		this.configuration = configuration;
		return this;
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public List<Path> getOutputPaths() {
		return outputPaths;
	}
	public Class<?> getDriverClass() {
		return driverClass;
	}
	public Class<? extends Partitioner> getPartitionerClass() {
		return partitionerClass;
	}
	public Class<? extends RawComparator> getGroupingComparator() {
		return groupingComparator;
	}
	public Class<? extends Reducer> getReducer() {
		return reducer;
	}
	
	public int getNumMapReduceTasks() {
		return numMapReduceTasks;
	}
	public Class<?> getOutputKeyClass() {
		return outputKeyClass;
	}
	public Class<?> getOutputValueClass() {
		return outputValueClass;
	}
	public Class<?> getMapOutputKeyClass() {
		return mapOutputKeyClass;
	}
	public Class<?> getMapOutputValueClass() {
		return mapOutputValueClass;
	}
	public MapList getMappers() {
		return mappers;
	}
	public JobBuilder setOutputPaths(List<Path> outputPaths) {
		this.outputPaths = outputPaths;
		return this;
	}
	public JobBuilder setDriverClass(Class<?> driverClass) {
		this.driverClass = driverClass;
		return this;
	}
	public JobBuilder setPartitionerClass(Class<? extends Partitioner> partitionerClass) {
		this.partitionerClass = partitionerClass;
		return this;
	}
	public JobBuilder setGroupingComparator(Class<? extends RawComparator> groupingComparator) {
		this.groupingComparator = groupingComparator;
		return this;
	}
	public JobBuilder setReducer(Class<? extends Reducer> reducer) {
		this.reducer = reducer;
		return this;
	}
	public JobBuilder setNumMapReduceTasks(int numMapReduceTasks) {
		this.numMapReduceTasks = numMapReduceTasks;
		return this;
	}
	public JobBuilder setOutputKeyClass(Class<?> outputKeyClass) {
		this.outputKeyClass = outputKeyClass;
		return this;
	}
	public JobBuilder setOutputValueClass(Class<?> outputValueClass) {
		this.outputValueClass = outputValueClass;
		return this;
	}
	public JobBuilder setMapOutputKeyClass(Class<?> mapOutputKeyClass) {
		this.mapOutputKeyClass = mapOutputKeyClass;
		return this;
	}
	public JobBuilder setMapOutputValueClass(Class<?> mapOutputValueClass) {
		this.mapOutputValueClass = mapOutputValueClass;
		return this;
	}
	public JobBuilder setMappers(MapList mappers) {
		this.mappers = mappers;
		return this;
	}
	public Job build() throws IOException{
		return new Job(this);
	}
}
