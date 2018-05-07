
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.MAP;


public class HUIM {
//public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{
        private final static IntWritable one = new IntWritable(1);
        private String T = new String();
        private int Ti = 0;
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
         
       
    StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
 //   System.out.println(profit_table);
    while (itr.hasMoreTokens()) {
    		Ti++;
        T = itr.nextToken().toString();
        String[] T_splite = T.split(",");
        int flag=0;
        
        for(String s : T_splite)
        {
        	 context.write(new Text(Integer.toString(Ti)), new Text(Integer.toString(flag)+":"+Integer.toString(Integer.parseInt(s)*profit_table.get(flag))));
        	 //int num = Integer.parseInt(s)*profit_table.get(flag);
        	 //System.out.println(Integer.toString(Ti)+":"+T+":"+num);
        	 flag++;
        }
        	
       
     

    }
   
        	
  }

}



public static class IntSumReducer

  //   extends Reducer<Text,IntWritable,Text,IntWritable> {
       extends Reducer<Text,Text,Text,Text> {
  private IntWritable result = new IntWritable();
 

  public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

  
   //  int sum=0;
    int i = 0;
    String output = "";
    for (Text val : values) {

    	   String temp = val.toString();
    	   output = temp + " " + output;
    //     if(val.get()>=0)
      //   {
      //  	 max = val.get();
       //  }
       i++;
    }
   String[] orgin_T=new String[i];
   String[] output_splite = output.split(" ");
   int [] num_compare = new int[i];
   for(String s : output_splite) {
	   String[] s_splite = s.split(":");
	   int index = Integer.parseInt(s_splite[0]);
	   orgin_T[index] = s_splite[1];
	   num_compare[index] = Integer.parseInt(s_splite[1]);
   }
  // System.out.println(num_compare);
   /*for(int j=0;j<num_compare.length;j++) {
	   System.out.println(num_compare[j]+" ");
   }*/
  // System.out.println("-------------------");
   int max = num_compare[0];
   for(int j=1;j < num_compare.length;j++){ 
	      if(num_compare[j] > max){ 
	    //	  System.out.println("Now Max is:"+max);
	         max = num_compare[j]; 
	      } 
	    }
  // System.out.println(max);
   String orgin = "";
   for(String o : orgin_T) {
	   if(orgin=="") {
		   orgin = o;
	   }
	   else{
	   orgin = orgin + " " + o;
	   }
   }
   
   
    context.write(new Text(orgin+" "+Integer.toString(max)),null);
   // String[] key_splite = key.toString().split(":");
  //  context.write(key, new IntWritable(max));
   
   // context.write(key, new IntWritable(sum));
    
 
  }
       
 
}
public static class PruneMapper
extends Mapper<Object, Text, Text, IntWritable>{
private String T = new String();
public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
while (itr.hasMoreTokens()) {
 T = itr.nextToken().toString();
 List<Integer> T_value = new ArrayList<Integer>();
  String[] T_splites = T.split(" ");
  for(String s:T_splites) {
	  T_value.add(Integer.parseInt(s));
	  //System.out.println(s);
  }
  
 int max_utility_T = T_value.get(T_value.size()-1);

 for(int i=0;i<T_value.size()-1;i++) {
	 if(T_value.get(i)!=0) {
		 String item_name = Integer.toString(i+1);
		 context.write(new Text(item_name), new IntWritable(max_utility_T));	  
	 }
 }
	 
  
}
}
}
public static class IntSumCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {
private IntWritable result = new IntWritable();

public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
int sum = 0;
int TH=45;
for (IntWritable val : values) {
 sum += val.get();
}

  result.set(sum);
  if(sum>=TH) {
     context.write(key, result);
  }
}
}
public static class LinkMapper 

extends Mapper<Object, Text, Text, Text>{

private String T = new String();

public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
while (itr.hasMoreTokens()) {

T = itr.nextToken().toString();

String[] T_splites = T.split("\t");

if(T_splites[0].length()==1) {
	
	context.write(new Text("1"), new Text(T_splites[0]));
}
else {
	 System.out.println("還沒開發");
	//context.write(new Text(T_splites[0]),new Text("YOO"));
	//String[] items = T_splites[0].split(",");
	 
	
	//context.write(new Text(T_splites[0].substring(0, )), arg1);
}

//context.write(new Text(T_splites[0]),new Text("YOO"));


}	



//context.write(new Text(Prune_T),null);
}
}

public static class LinkReducer

extends Reducer<Text,Text,Text,Text> {


public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

/*for (Text val : values) {
context.write(new Text(val.toString()), null);
}*/
	
	
  if(key.toString().length()==1) {
	 
	
	  List<String> C1 = new ArrayList<String>();
     
    for(Text val : values) {
    
  	  C1.add(val.toString());
    	
    }
    Collections.sort(C1);
    
    for(int i=0;i<C1.size();i++) {
    	    for(int j=i+1;j<C1.size();j++) {
    	       	String C2="";
    	    	    C2 = C1.get(i) + "," + C1.get(j);
    	    	    context.write(new Text(C2), null);
    	    	    
    	    }
    }
   
	  
     
  }
  else {
	  System.out.println("還沒開發");
  }
}
}
static List<Integer> profit_table = new ArrayList<Integer>();
public static void main(String[] args) throws Exception {

  Configuration conf = new Configuration();
  String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
  if (otherArgs.length != 2) {
  //  System.err.println("Usage: <in><out>");
   
  //  System.exit(2);
  }
  Job job = Job.getInstance(conf, "HUIT Mining");
  job.setJarByClass(HUIM.class);
  
 //Path profit = new Path(otherArgs[0]);
 //Path inputPath = new Path(otherArgs[1]);
 //Path outputPath = new Path(otherArgs[2]);
  
  Path profit = new Path("/ethonwu/profit.txt");
  Path inputPath = new Path("/ethonwu/HUIM.txt");
  Path outputPath = new Path("/ethonwu/HUIM_temp/");
  outputPath.getFileSystem(conf).delete(outputPath, true);
  
  FileSystem fs = FileSystem.get(conf);
  BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(profit)));
  String line;
  line = br.readLine();
  while(line!=null)
  {
	  //System.out.println(line);
	  String[] splite_line = line.split(" ");
	  profit_table.add(Integer.parseInt(splite_line[1]));
	  line = br.readLine();
  }
  //System.out.println(profit_table);
  
 
  
  job.setMapperClass(TokenizerMapper.class);
  job.setReducerClass(IntSumReducer.class);

  

  FileInputFormat.addInputPath(job, inputPath);
  //Test
 
  FileOutputFormat.setOutputPath(job,outputPath);
  job.setNumReduceTasks(1);
  job.setOutputKeyClass(Text.class);
 // job.setOutputValueClass(IntWritable.class);
  job.setOutputValueClass(Text.class);

// System.exit(job.waitForCompletion(true) ? 0 : 1);
  while(!job.waitForCompletion(true)) {}
  
  Path prune_output = new Path("hdfs:/ethonwu/HUIM_output/");
  prune_output.getFileSystem(conf).delete(prune_output, true);
  Job job2 = new Job(conf, "Prune part");
  job2.setJarByClass(HUIM.class);
  
  job2.setMapperClass(PruneMapper.class);
  job2.setReducerClass(IntSumCombiner.class);
  
  job2.setOutputKeyClass(Text.class);
  job2.setOutputValueClass(IntWritable.class);
  
  
  FileInputFormat.addInputPath(job2, outputPath);
  FileOutputFormat.setOutputPath(job2,prune_output);
  
  while(!job2.waitForCompletion(true)) {}
  
  //while(!job2.waitForCompletion(true)) {}
  Counters count = job2.getCounters();
  long info = count.getGroup("org.apache.hadoop.mapreduce.TaskCounter").findCounter("REDUCE_OUTPUT_RECORDS").getValue();
  int nums = (int) (long) info;
 // long info = job2.getCounters().findCounter("Map-Reduce Framework","Reduce output records").getValue();
   System.out.println(nums);
   if(nums==0) {
	   System.out.println("Find Finish!!");
	   System.exit(0);
   }
  
  // Job 3 Link F1 part  
   Path generate_temp = new Path("hdfs:/ethonwu/generate_temp/");
   generate_temp.getFileSystem(conf).delete(generate_temp, true);
   Job job3 = new Job(conf, "Generate Candidate");
   job3.setJarByClass(HUIM.class);
   job3.setMapperClass(LinkMapper .class);
   job3.setReducerClass(LinkReducer.class);
   job3.setOutputKeyClass(Text.class);
   job3.setOutputValueClass(Text.class);
   FileInputFormat.addInputPath(job3, prune_output);
   FileOutputFormat.setOutputPath(job3, generate_temp);
   System.exit(job3.waitForCompletion(true) ? 0 : 1);
   
   
   
 System.exit(0);
}


}