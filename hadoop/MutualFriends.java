import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MutualFriends {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text word1= new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
	StringTokenizer itr = new StringTokenizer(value.toString());

	String s="",tmp1="",tmp2;
	if(itr.hasMoreTokens()){
		tmp1=itr.nextToken();
	}
	int val1=Integer.parseInt(tmp1);
	while(itr.hasMoreTokens()){
		s+=itr.nextToken();
	}
	s+=",";
	
	StringTokenizer itr1 = new StringTokenizer(s,",");
	while (itr1.hasMoreTokens()) {
	tmp2=itr1.nextToken();
	int val2=Integer.parseInt(tmp2);
	if(val1<val2){
		String tmp3=tmp1;
		tmp3+="\t";
		tmp3+=tmp2;
		word.set(tmp3);
	}
	else{
		String tmp4=tmp2;
		tmp4+="\t";
		tmp4+=tmp1;
		word.set(tmp4);
	}	
		
	word1.set(s);	
        context.write(word,word1);
      }
    }
  }

/*public static class Combiner1 extends Reducer<Text,Text,Text,Text> 
{
   private IntWritable result = new IntWritable();
   private Text result=new Text();
   public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
   {
      String sum = "";
      for (Text val : values) 
      {
         sum += val.get().toString();
      }
      result.set(sum);
      context.write(key, result);
   }
}
*/


  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      Text result = new Text();
	StringTokenizer itr2 = new StringTokenizer(key.toString(),"\t");
	String endresult="";
	String mut=key.toString();
	
	for (Text val : values) {
        endresult += val.toString();
      }
	StringTokenizer itr3 = new StringTokenizer(endresult,",");
	ArrayList<String> commons=new ArrayList<String>();
	ArrayList <String> com2=new ArrayList<String>();
	boolean mutual=false;
	String h1=itr2.nextToken();
        String h2=itr2.nextToken();
	String h3=h1+","+h2;
	String finalresult="";
	StringTokenizer itr4 = new StringTokenizer(endresult,",");
	while(itr4.hasMoreTokens())
	{
		String com1=itr4.nextToken();
		if(com1.equals(""))
		{
			
		}
		
		else
		{
			commons.add(com1);

		}
	
	}
	int i1=0,i2=0;
	//if(h3.equals("0,4"))
	//{
	for(String k:commons)
	{
		//	if(Collections.frequency(commons,k)>1&&!(com2.contains(k)))
		//	com2.add(k);
		if((Collections.frequency(commons,k)>1)&&(!com2.contains(k)))
		{com2.add(k);}
		

	}
//	}
//	if(h3.equals("0,4"))
//	{
	for(String s:com2)
	{
		
		//if(h3.equals("1,29826"))
                //System.out.println(s);
		//System.out.println("Com2"+s);
		//if(s.equals(h1)||s.equals(h2))
		//{}
		//else{	
		finalresult+=s;
		finalresult+=",";
		//}
		
    	}
//	}
	//if(h3.equals("1,29826"))
	//	System.out.prinln(s);
		
	if(finalresult.length()>=2)
	finalresult=finalresult.substring(0,finalresult.length()-1);
	//if(finalresult!="")
	result.set(finalresult);
	//key.set(h1+"\t"+h2);
      	//if(h3.equals("0,4")||h3.equals("20,22939")||h3.equals("6222,19272")||h3.equals("28041,28056")||h3.equals("1,29826"))
	context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "mutual friends");
    job.setJarByClass(MutualFriends.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
