package search.Engine.PageRank;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class LinkGraph 
{
	public static class Map extends Mapper<LongWritable, Text ,Text ,Text> 
	{
		private final static IntWritable one  = new IntWritable(1);
		private Text count  = new Text("NumPages");

		Pattern linkPat = Pattern.compile("\\[\\[.*]\\]");

		public void map(LongWritable offset, Text lineText, Context context) throws  IOException,  InterruptedException 
		{

			String title = lineText.toString();	
			String fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
			//Pattern p = Pattern.compile("<title>(.+?)</title>");    
			//Matcher m = p.matcher(title);          
			//List<String> matches = new ArrayList<String>(); 
			String line  = lineText.toString();   
			Matcher m1 = linkPat.matcher(line);  
			
			int i=0;
			List<String> outlinks = new ArrayList<String>();  
			
			while(m1.find()) 
			{ 
				String url = m1.group().replace("[", " ").replace("]", " ");
				if(!url.isEmpty())
				{
					outlinks.add(i, url);     
					i++;             
				}
			}
			
			String out_link="";       
			int flag=0;                
			for (String s: outlinks)
			{    

				if(flag==0)
				{				
					out_link +=s;
				}else
				{
					out_link +="@"+s;       
				}
				flag++;    
				outlinks=new ArrayList<String>();    
			}
			//for(String k:matches){    
			context.write(new Text(fileName), new Text(out_link));
			//}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> 
	{
		@Override 
		public void reduce(Text word, Iterable<Text> counts, Context context) throws IOException,  InterruptedException 
		{
			String s= word.toString();       
			String q="";           			 
			
			Double no_of_links = context.getConfiguration().getDouble("no_of_outlink", 0.0);  
			
			double int_pr;
			for (Text count : counts) 
			{
				q=count.toString();   
			}

			int_pr = 1/no_of_links;    
			context.write(new Text(s), new Text(q+String.valueOf("#####"+int_pr))); 
		}
	}
}








