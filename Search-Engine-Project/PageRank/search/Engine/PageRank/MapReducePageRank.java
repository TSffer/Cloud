package search.Engine.PageRank;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class MapReducePageRank  
{
	public static class Map extends Mapper<LongWritable ,Text ,Text ,Text> 
	{
		public void map( LongWritable offset,  Text lineText,  Context context) throws  IOException,  InterruptedException 
		{

			String title = lineText.toString();

			String[] l = title.split("\t");     
			String[] l2 =l[1].split("#####"); 
			String[] pages={""};
			if(l2[0].contains("@"))
			{       
				pages= l2[0].split(" ");
				context.write(new Text(l[0]), new Text("link"+"\t"+l2[0]));		
				for(String outlink: pages)
				{			
					context.write(new Text(outlink), new Text(l2[1]+"\t"+pages.length));   
				}
			}
			else if(l2[0].isEmpty())
			{     
				context.write(new Text(l[0]), new Text("link"+"\t"+""));     
				context.write(new Text("nill"), new Text(l2[1]+"\t"+"nill"));  
			}
			else if(pages.length==1) 
			{  
				context.write(new Text(l[0]), new Text("link"+"\t"+l2[0]));  
				context.write(new Text(l2[0]), new Text(l2[1]+"\t"+1)); 
			}
		}
	}

	public static class Reduce extends Reducer<Text ,Text ,Text ,Text> 
	{
		@Override 
		public void reduce( Text word, Iterable<Text> counts, Context context) throws IOException, InterruptedException 
		{
			String srt = word.toString();
			int val=0;
			String key="";
			String temp="";
			double sum_ranks=0.0;
			double fpr=0.0;
			
			for ( Text count  : counts) 
			{    
				key = count.toString();
				String[] newKey= key.split("\t");
			
				if(word.toString().equals("nill"))
				{    
					break;	
				}
				else
				{							  
					if(newKey[0].equals("link"))
					{  
						val=1;	
						if(newKey.length==2)
						{ 
							temp = newKey[1];
						}
					}
					else
					{				
						sum_ranks= sum_ranks+ Double.parseDouble(newKey[0])/Double.parseDouble(newKey[1]);  
					}

				}
			}
		}
	}
}
