package com.mycompany.hadoopdemo1;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Text wordKey = new Text("");
	private IntWritable oneValue = new IntWritable(1);
	@Override
	public void map(LongWritable key /*offset*/, 
			Text value /*line string*/, 
			Mapper<LongWritable, Text, Text, IntWritable>.Context context) 
			throws InterruptedException, IOException {
		String line = value.toString().trim();
		System.out.println("LINE : " + line);
		if(line.isEmpty()) { // handle empty lines
			Counter emptyLineCounter = context.getCounter("WordCountCounters", "EmptyLineCounter");
			emptyLineCounter.increment(1);
		} else {
			String	[] words = line.split("\\s+");
			for(String word : words) {
				wordKey.set(word);
				context.write(wordKey, oneValue);
			}
		}
	}
}