package part3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class StripeApproach {
	public static class StripeMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
	    private MapWritable occMap = new MapWritable();
	    private Text word = new Text();
	    private IntWritable totalCount = new IntWritable();
	    @Override
	    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        int neighbors = context.getConfiguration().getInt("neighbors", 2);
	        String[] tokens = value.toString().split("\\s+");
	        if (tokens.length > 1) {
	            for (int i = 0; i < tokens.length; i++) {
	                word.set(tokens[i]);
	                occMap.clear();

	                int start = (i - neighbors < 0) ? 0 : i - neighbors;
	                int end = (i + neighbors >= tokens.length) ? tokens.length - 1 : i + neighbors;
	                for (int j = start; j <= end; j++) {
	                    if (j == i) continue;
	                    Text neighbor = new Text(tokens[j]);
	                    if(occMap.containsKey(neighbor)){
	                       IntWritable count = (IntWritable)occMap.get(neighbor);
	                       count.set(count.get()+1);
	                    }else{
	                        occMap.put(neighbor,new IntWritable(1));
	                    }
	                }
	                totalCount.set(end - start);
	                context.write(word,occMap);
	            }
	        }
	    }
	}
	
	public static class StripeReducer extends Reducer<Text, MapWritable, Text, Text> {
	   
		public void reduce(Text word, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
			MapWritable mapNeighbor = new MapWritable();
			Integer count = 0;
			for (MapWritable mwNeighbor : values) {	// for all H in stripes[H1,H2...]
				for (Writable neighbor : mwNeighbor.keySet()) {
					IntWritable frequencies = (IntWritable) mwNeighbor.get(neighbor);
					count += frequencies.get();
					if (mapNeighbor.containsKey(neighbor)) {
						IntWritable tmp = (IntWritable) mapNeighbor.get(neighbor);
						frequencies.set(frequencies.get() + tmp.get());
					}
					mapNeighbor.put(neighbor, frequencies); 	// add to hash result
				}
			}
			//Calculate the frequencies
			MapWritable Hf = new MapWritable();
			String sHf = "";
			for (Writable neighbor : mapNeighbor.keySet()) {
				IntWritable occurrences = (IntWritable) mapNeighbor.get(neighbor);
				DoubleWritable freq = new DoubleWritable(occurrences.get() / count);
				Hf.put(neighbor, freq);
				sHf += "(" + neighbor.toString() + "," + String.valueOf(occurrences.get()) + "/" + String.valueOf(count) + "),";
			}
			context.write(word, new Text(sHf));
		}
	}
	
	public static void main(String[] args) throws IOException,InterruptedException,ClassNotFoundException {
    	
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(StripeApproach.class);
        
        job.setMapperClass(StripeMapper.class);
        job.setReducerClass(StripeReducer.class);
        
        job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);
		
		//job.setMapOutputKeyClass(Text.class);
		//job.setMapOutputValueClass(MapWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		     
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));     
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    	
    }
	
	
	
}
