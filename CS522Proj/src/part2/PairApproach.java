package part2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import part2.Pair;



public class PairApproach {
	public static class PairMapper extends Mapper<LongWritable, Text, Pair, IntWritable> {
	    private Pair wordPair = new Pair();
	    private IntWritable ONE = new IntWritable(1);
	    private IntWritable totalCount = new IntWritable();

	
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        int neighbors = context.getConfiguration().getInt("neighbors", 2);
	        String[] tokens = value.toString().split("\\s+");
	        if (tokens.length > 1) {
	            for (int i = 0; i < tokens.length; i++) {
                    tokens[i] = tokens[i].replaceAll("\\W+","");
                    if(tokens[i].equals("")){
                        continue;
                    }
                    wordPair.setWord(tokens[i]);
                    int start = (i - neighbors < 0) ? 0 : i - neighbors;
                    int end = (i + neighbors >= tokens.length) ? tokens.length - 1 : i + neighbors;
                    for (int j = start; j <= end; j++) {
                        if (j == i) continue;
                        wordPair.setNeighbor(tokens[j].replaceAll("\\W",""));
                        context.write(wordPair, ONE);
                    }
                    wordPair.setNeighbor("*");
                    totalCount.set(end - start);
                    context.write(wordPair, totalCount);
	            }
	        }
	    }
	}
	
	public static class PairReducer extends Reducer<Pair, IntWritable, Pair, Text> {
	    private int totalCount = 0;
//	    private double relativeCount = 0;
	    private Text currentWord = new Text("XXXX");
	    private Text star = new Text("*");

	   
	    public void reduce(Pair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	        if (key.getNeighbor().equals(star)) {
	            if (key.getWord().equals(currentWord)) {
	                totalCount= totalCount + getTotalCount(values);
	            } else {
	                currentWord.set(key.getWord());
	                totalCount=0;
	                totalCount =getTotalCount(values);
	            }
	        } else {
	            int count = getTotalCount(values);
	            String freq = "(" + count +"/" + totalCount +")";
//	            relativeCount =count / totalCount;
	            context.write(key,new Text(freq));
	        }

	    }

	    private int getTotalCount(Iterable<IntWritable> values) {
	        int count = 0;
	        for (IntWritable value : values) {
	            count += value.get();
	        }
	        return count;
	    }
	}
	
	public static void main(String[] args) throws IOException,InterruptedException,ClassNotFoundException {
    	
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(PairApproach.class);
        
        job.setMapperClass(PairMapper.class);
        job.setReducerClass(PairReducer.class);
        

        job.setMapOutputKeyClass(Pair.class);
        job.setMapOutputValueClass(IntWritable.class);

        
        job.setOutputKeyClass(Pair.class);
        job.setOutputValueClass(Text.class);
        //job.setNumReduceTasks(0);
      
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));     
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    	
    }


}
