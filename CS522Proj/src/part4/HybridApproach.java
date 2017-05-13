package part4;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class HybridApproach extends Configured implements Tool {
	public static class PairMapper extends MapReduceBase implements Mapper<LongWritable, Text, Pair, IntWritable> {

		private HashMap<Pair, Integer> outputMap = new HashMap<>();

		@Override
		public void map(LongWritable key, Text values, OutputCollector<Pair, IntWritable> output, Reporter r)
				throws IOException {
			
			String input = values.toString();
			String[] readLines = input.split("//.*\n");
			
			for (String line : readLines) {
				String[] words = line.split("[ ]{1,}");
				for (int i = 0; i < words.length - 1; i++) {
					for (int j = i + 1; j < words.length; j++) {
						if (words[i].equals(words[j]))
							break;

						Pair pair = new Pair(words[i], words[j]);

						if (outputMap.get(pair) != null)
							outputMap.put(pair, outputMap.get(pair) + 1);
						else
							outputMap.put(pair, new Integer(1));
					}
				}
			}

			Iterator<Entry<Pair, Integer>> iterator = outputMap.entrySet().iterator();
			while (iterator.hasNext()) {
				Map.Entry<Pair, Integer> mapEntry = iterator.next();
				output.collect(mapEntry.getKey(), new IntWritable(mapEntry.getValue()));
				iterator.remove();
			}
			Pair end = new Pair("{","}");
			output.collect(end, new IntWritable(0));
		}
	}
	
	public static class StripeReducer extends MapReduceBase implements Reducer<Pair, IntWritable, Text, Text> {
	
		private Text current_term = null ;
		private int maginal = 0;
		private HashMap<String, Double> stripes = new HashMap<String, Double>();
		
		@Override
		public void reduce(Pair key, Iterator<IntWritable> values,OutputCollector<Text,Text> output, Reporter r) 
				throws IOException {
			Text w = key.getElement();
			Text u = key.getNeighbour();
			if(current_term == null){
				current_term = w;
				
			}
			else if(!current_term.equals(w)){
					updateStripes(stripes, maginal);
					System.out.println(current_term + ", " + stripesToText(stripes));
					output.collect(current_term, stripesToText(stripes));
					stripes.clear();
					maginal = 0;
					
			}	
			int sum = sum(values);
			maginal += sum;
			stripes.put(u.toString(),(double)sum);	
			current_term = w;
		}
		
		private int sum(Iterator<IntWritable> values){
			int sum = 0;
			while(values.hasNext()){
				sum += values.next().get();
			}
			return sum;
		}
		private void updateStripes(HashMap<String, Double> stripes, int maginal){
			for(Entry<String, Double> entry : stripes.entrySet()){
				double newValInt = entry.getValue();
				double frequency = newValInt/maginal;
				stripes.put(entry.getKey(), frequency);
			}
		}

		private Text stripesToText(HashMap<String, Double> result){
			String stripes = "[";
			for(Entry<String, Double> entry : result.entrySet()){
				String k = entry.getKey();
				double v = entry.getValue();
				DecimalFormat df = new DecimalFormat("#.##");
				stripes += "(" + k + "," + df.format(v) + "),";
			}
			stripes += "]";
			return new Text(stripes);
		}
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("Please give proper input and output directories");
			return -1;
		}
		JobConf conf = new JobConf(HybridApproach.class);

		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		conf.setMapperClass(HybridApproach.PairMapper.class);
		conf.setReducerClass(HybridApproach.StripeReducer.class);

		conf.setMapOutputKeyClass(Pair.class);
		conf.setMapOutputValueClass(IntWritable.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String args[]) throws Exception {	
		int exitCode = ToolRunner.run(new HybridApproach(), args);
		System.exit(exitCode);
	}
}
