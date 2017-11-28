package com.mycompany.hadoopdemo1;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable totalValue = new IntWritable(0);
	@Override
	public void reduce(Text key /*word*/, 
				Iterable<IntWritable> values /*list of count=1*/, 
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) 
				throws InterruptedException, IOException{
		//String word = key.toString();
		int total = 0;
		for(IntWritable countValue : values) {
			int count = countValue.get();
			total = total + count;
		}
		totalValue.set(total);
		context.write(key, totalValue);
	}
}
