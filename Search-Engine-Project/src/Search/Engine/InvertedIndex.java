package Search.Engine;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.HashSet;
import java.util.Set;

import java.util.regex.Pattern;

public class InvertedIndex 
{
	public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text>
	{
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			Pattern linkPat = Pattern.compile("\\[\\[.*?]\\]");
			
			String fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
			String[] indexKeys = value.toString().replaceAll("\\p{P}", "").split("\\s+");
			
			for(String indexKey : indexKeys)
			{ 
				context.write(new Text(indexKey.toLowerCase()), new Text(fileName));
			}
		}
	}
	
	public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text>
	{
		@Override
		public void reduce(Text keyIndexWord, Iterable<Text> valuesDocumentNames, Context context) throws IOException, InterruptedException
		{
			Set<String> documentNames = new HashSet<String>();
			
			for(Text valueDocumentName : valuesDocumentNames)
			{
				documentNames.add(valueDocumentName.toString());
			}
			
			String indexString = new String("");
			
			for(String valueDocumentName : documentNames)
			{
				indexString = new String(indexString.concat(valueDocumentName).concat(" "));
			}
			
			indexString = new String(indexString.trim());
			
			context.write(keyIndexWord, new Text(indexString));
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		System.out.println("*********************************************************");
		
		if(args.length != 2)
		{
			String errorReport = "Usage:: \n" + "hadoop jar InvertedIndex.jar " + "[inputDir] [outputDir]\n";
			System.out.println(errorReport);
			System.exit(-1);
		}
		
		String inputDir = args[0];
		String outputDir = args[1];
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "InvertedIndex");
		job.setJarByClass(InvertedIndex.class);
		
		//deleteFolder(conf, outputDir);
		
		job.setMapperClass(InvertedIndexMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(InvertedIndexReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		 
		int numReduceTasks = 1;
		job.setNumReduceTasks(numReduceTasks);
		
		if(!job.waitForCompletion(true))
			return;
	}
	
	public static void deleteFolder(Configuration conf, String folderPath) throws IOException
	{
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(folderPath);
		if(fs.exists(path))
		{
			fs.delete(path,true); 
		}
	}
}
