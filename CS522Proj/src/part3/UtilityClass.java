 package part3;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class UtilityClass {

	private static final String [] stopWords = {"i","&","and","!","?","my", "a", "about","an","are","as","at","be","by","for","from","how","in", "is","it",
            "of","on","or","that","the","this","to","was","what","when","where","who","will","with", "rt"};
	
	/**
	 * Stop word analyzer
	 * @param text - text to be analyzed
	 * @return - true if the word is stopword else false
	 */
	public static boolean isStopWord(String text)
	{
		for (String stopword : stopWords) {
			if(text.equalsIgnoreCase(stopword))
				return true;
			
			// remove if its a media link
			if(text.startsWith("http://") || text.contains("http://"))
				return true;
		}
		return false;
	}
	
	/**
	 * Clean the text removing symbols, unwanted characters, etc
	 */
	public static String cleanText(String text)
	{
		text = text.trim()
				.replaceAll(",", "")
				.replaceAll("\\+", "")
				.replaceAll("\\^", "")
				.replaceAll("\\$", "")
				.replaceAll("\\.", "")
				.replaceAll("\\|", "")
				.replaceAll("\\?", "")
				.replaceAll("\\*", "")
				.replaceAll("\\(", "")
				.replaceAll("\\)", "")
				.replaceAll("\\[", "")
				.replaceAll("\\]", "")
				.replaceAll("\\{", "")
				.replaceAll("\\}", "")
				.replaceAll("&", "")
				.replaceAll(";", "")
				.replaceAll("\"", "")				
				.replaceAll("!", "")
				.replaceAll("%", "")
				.replaceAll("=", "")
				.replaceAll("<", "")
				.replaceAll(">", "")
				.replaceAll("\'", "")
				.replaceAll(":", "");			
		return text.toLowerCase();
	}
	
	public static void writeFile(String text, String path, Configuration conf) {
		try {
			System.out.println("*** inside writeFile of reducer, text: " + text
					+ ", path: " + path);
			Path pt = new Path(path);
			FileSystem fs = FileSystem.get(conf);
			BufferedWriter br = new BufferedWriter(new OutputStreamWriter(
					fs.create(pt, true)));

			br.write(text);
			br.close();
		} catch (Exception e) {
			System.out.println("inside reducer some error in write");
			e.printStackTrace();
		}
	}

	public static String readFile(String path, Configuration conf) {
		String line = "";
		System.out.println("********* inside readFile of reducer. Line 78");
		try {
			Path pt = new Path(path);
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(pt)));

			line = br.readLine();

			return line;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return line;
	}
}