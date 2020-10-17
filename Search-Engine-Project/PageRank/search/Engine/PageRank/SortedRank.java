package search.Engine.PageRank;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class SortedRank 
{
	public static class Map extends Mapper<LongWritable ,Text ,DoubleWritable ,Text> 
	{
		public void map( LongWritable offset,  Text lineText,  Context context) throws  IOException,  InterruptedException 
		{
			String title = lineText.toString();     
			String[] s= title.split("\t");			
			String[] s1 = s[1].split(" ");      

			context.write(new DoubleWritable(Double.parseDouble(s1[1])),new Text(s[0]));   
		}
	}
	
	public static class Reduce extends Reducer<DoubleWritable ,Text ,Text ,DoubleWritable> 
	{
		@Override 
		public void reduce( DoubleWritable word,  Iterable<Text> counts,  Context context) throws IOException, InterruptedException 
		{
			for (Text count: counts)
			{
				context.write(count, word);       
			}
		}
	}
}
