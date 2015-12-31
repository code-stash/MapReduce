package com.code.stash.hadoop.movies.common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Job {

	private org.apache.hadoop.mapreduce.Job mapReduceJobInstance;

	public Job(JobBuilder builder) throws IOException {

		Configuration config = builder.getConfiguration() == null ? new Configuration() : builder.getConfiguration();
		org.apache.hadoop.mapreduce.Job job = org.apache.hadoop.mapreduce.Job.getInstance(config);

		job.setJarByClass(builder.getDriverClass());
		job.setPartitionerClass(builder.getPartitionerClass());
		job.setGroupingComparatorClass(builder.getGroupingComparator());

		job.setReducerClass(builder.getReducer());
		job.setNumReduceTasks(builder.getNumMapReduceTasks());

		job.setOutputKeyClass(builder.getOutputKeyClass());
		job.setOutputValueClass(builder.getOutputValueClass());

		if (!builder.getMappers().requiresMultiInput()) {
			job.setMapperClass(builder.getMappers().get(0).getMapperClass());
		} else {
			for (MapInput input : builder.getMappers()) {
				MultipleInputs.addInputPath(job, input.getInputPath(), input.getInputFormatClass(),
						input.getMapperClass());
			}
		}

		job.setMapOutputKeyClass(builder.getMapOutputKeyClass());
		job.setMapOutputValueClass(builder.getMapOutputValueClass());

		for (Path output : builder.getOutputPaths()) {
			FileOutputFormat.setOutputPath(job, output);
		}

		mapReduceJobInstance = job;

	}

	public org.apache.hadoop.mapreduce.Job getMapReduceJobInstance() throws IOException {
		return this.mapReduceJobInstance;
	}

}
