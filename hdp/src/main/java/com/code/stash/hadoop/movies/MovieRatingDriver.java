package com.code.stash.hadoop.movies;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import com.code.stash.hadoop.movies.common.JobBuilder;
import com.code.stash.hadoop.movies.common.MapInput;
import com.code.stash.hadoop.movies.common.MapList;
import com.code.stash.hadoop.movies.common.MovieRatingComparator;
import com.code.stash.hadoop.movies.common.MovieRatingPartitioner;
import com.code.stash.hadoop.movies.common.TextPair;
import com.code.stash.hadoop.movies.mappers.MovieMapper;
import com.code.stash.hadoop.movies.mappers.RatingMapper;
import com.code.stash.hadoop.movies.reducers.MovieAvgRatingReducer;
import com.code.stash.hadoop.movies.reducers.MovieMaxRatingReducer;
import com.code.stash.hadoop.movies.reducers.MovieMinRatingReducer;

public class MovieRatingDriver {

	public static void main(String[] args) {
		MapInput movieInput = new MapInput();
		movieInput.setInputPath(new Path("movies.csv"));
		movieInput.setInputFormatClass(TextInputFormat.class);
		movieInput.setMapperClass(MovieMapper.class);

		MapInput ratingInput = new MapInput();
		ratingInput.setInputPath(new Path("ratings.csv"));
		ratingInput.setInputFormatClass(TextInputFormat.class);
		ratingInput.setMapperClass(RatingMapper.class);

		List<MapInput> inputList = new ArrayList<>();
		inputList.add(movieInput);
		inputList.add(ratingInput);

		MapList mList = new MapList(inputList);

		JobBuilder builder = new JobBuilder();
		builder.setConfiguration(new Configuration());
		builder.setDriverClass(MovieRatingDriver.class);
		builder.setPartitionerClass(MovieRatingPartitioner.class);
		builder.setGroupingComparator(MovieRatingComparator.class);
		builder.setReducer(MovieMaxRatingReducer.class);
		builder.setNumMapReduceTasks(1);
		builder.setOutputKeyClass(Text.class);
		builder.setOutputValueClass(Text.class);
		builder.setMapOutputKeyClass(TextPair.class);
		builder.setMapOutputValueClass(TextPair.class);

		builder.setMappers(mList);

		List<Path> outputPaths = new ArrayList<>();
		outputPaths.add(new Path("outputMax"));

		builder.setOutputPaths(outputPaths);

		com.code.stash.hadoop.movies.common.Job maxJob = null;
		try {
			maxJob = builder.build();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		builder.setReducer(MovieMinRatingReducer.class);
		outputPaths = new ArrayList<>();
		outputPaths.add(new Path("outputMin"));

		builder.setOutputPaths(outputPaths);

		com.code.stash.hadoop.movies.common.Job minJob = null;
		try {
			minJob = builder.build();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		builder.setReducer(MovieAvgRatingReducer.class);
		outputPaths = new ArrayList<>();
		outputPaths.add(new Path("outputAvg"));

		builder.setOutputPaths(outputPaths);

		com.code.stash.hadoop.movies.common.Job avgJob = null;
		try {
			avgJob = builder.build();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		List<Job> jobs = new ArrayList<Job>();
		try {
			jobs.add(maxJob.getMapReduceJobInstance());
			jobs.add(minJob.getMapReduceJobInstance());
			jobs.add(avgJob.getMapReduceJobInstance());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		for (Job job : jobs) {
			try {
				job.submit();
			} catch (ClassNotFoundException | IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
