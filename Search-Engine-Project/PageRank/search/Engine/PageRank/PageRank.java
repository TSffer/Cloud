package search.Engine.PageRank;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

 
public class PageRank extends Configured  implements Tool 
{
	private static final Logger LOG = Logger.getLogger(PageRank.class);

	public static void main( String[] args) throws  Exception 
	{
		int res = ToolRunner.run(new PageRank(), args);
		System.exit(res);
	}

	@SuppressWarnings("deprecation")
	public int run( String[] args) throws  Exception 
	{
		Job job1  = Job .getInstance(getConf(), " TotalNumberofPages ");
		job1.setJarByClass(this.getClass());

		FileInputFormat.addInputPaths(job1, args[0]);
		FileOutputFormat.setOutputPath(job1, new Path(args[ 1]));
		job1.setMapperClass(TotalPages.Map.class);
		job1.setReducerClass(TotalPages.Reduce.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		
		int numReduceT = 1;
		job1.setNumReduceTasks(numReduceT);

		job1.waitForCompletion(true);

		
		Configuration config= new Configuration();
		try
		{
			FileSystem fs = FileSystem.get(config);      
			Path input_path= new Path(args[1]);    

			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(input_path)));
			String line;
			double no_of_outlink=0.0;        

			while((line = br.readLine())!= null)
			{    
				String [] l = line.split("\t");
				no_of_outlink = Double.parseDouble(l[1]);  
				break;
			}
			
			fs.delete(new Path(args[1]),true);          
			br.close();
			config.setDouble("no_of_outlink", no_of_outlink);  
		}
		catch(ArrayIndexOutOfBoundsException e)
		{
			System.out.println(e);
		}

		Job job2  = Job .getInstance(config, " linkgraph ");
		job2.setJarByClass(this.getClass());

		FileInputFormat.addInputPaths(job2, args[0]);
		FileOutputFormat.setOutputPath(job2, new Path(args[1]+"pagerank"));
		job2.setMapperClass(LinkGraph.Map .class);
		job2.setReducerClass(LinkGraph.Reduce.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		numReduceT = 1;
		job2.setNumReduceTasks(numReduceT);

		job2.waitForCompletion(true);

		
		for(int i=0; i<20; i++)
		{      								
			Job job3  = Job.getInstance(getConf(), " PageRank ");
			Configuration conf = new Configuration();
			String input = args[1]+"/pagerank"+i;			
			String output = args[1]+"/pagerank"+(i+1);	
			FileSystem fs = FileSystem.get(conf);

			job3.setJarByClass( this .getClass());

			FileInputFormat.addInputPaths(job3, input);
			FileOutputFormat.setOutputPath(job3, new Path(output));
			job3.setMapperClass(MapReducePageRank.Map.class);
			job3.setReducerClass(MapReducePageRank.Reduce.class);

			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(Text.class);
			
			numReduceT = 1;
			job3.setNumReduceTasks(numReduceT);
			
			job3.waitForCompletion(true);		
		}

		String input_outpath= args[1]+"/pagerank";  
		String outpath= args[1]+"/sorted";							

		Job job4 = Job .getInstance(getConf(), " SortedRank ");
		Configuration config2= new Configuration();
		job4.setJarByClass(this.getClass());
		FileSystem fs1= FileSystem.get(config2);   

		FileInputFormat.addInputPaths(job4, input_outpath);
		FileOutputFormat.setOutputPath(job4, new Path(outpath));
		job4.setMapperClass(SortedRank.Map.class);
		job4.setReducerClass(SortedRank.Reduce.class);
		job4.setMapOutputKeyClass( DoubleWritable.class);
		job4.setMapOutputValueClass(Text.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(DoubleWritable.class);
		Path a= new Path(args[1]+"/pagerank"); 
		job4.setSortComparatorClass(sorting_comp.class); 
		
		numReduceT = 1;
		job4.setNumReduceTasks(numReduceT);

		job4.waitForCompletion( true); 
		fs1.delete(a,true);  
		return 0;
	}
}

class sorting_comp extends WritableComparator 
{ 
	protected sorting_comp()
	{
		super (DoubleWritable.class, true);        
	}

	@Override
	public int compare(@SuppressWarnings("rawtypes") WritableComparable c1, @SuppressWarnings("rawtypes") WritableComparable c2)
	{
		DoubleWritable val1 = (DoubleWritable) c1;     
		DoubleWritable val2 = (DoubleWritable) c2;		

		int sorted_res = val1.compareTo(val2); 
		if(sorted_res == 0)
		{					
			sorted_res = val1.compareTo(val2);
		}
		return sorted_res*-1;  
	}
}
