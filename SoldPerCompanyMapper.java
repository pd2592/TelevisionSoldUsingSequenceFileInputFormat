package TvSoldPerCompany;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class SoldPerCompanyMapper extends Mapper<Text, Text, Text, IntWritable> {
	
    final static Pattern WORD_PATTERN = Pattern.compile("\\w+\\|\\w+\\|\\d+\\|\\w+\\|\\d+\\|\\d+");
    //Akai|Decent|16|Kerala|922401|12200
	Text outkey = new Text();
	IntWritable outvalue = new IntWritable(1);
	//Text a = new Text("test");
	public void map(Text key, Text value, Context context) 
			throws IOException, InterruptedException {
		/*String state;	
		String company = value.toString().split("\\|")[0];
		outkey.set(company);
			context.write(outkey, outvalue);
			
			*/
			
			
			Matcher matcher = WORD_PATTERN.matcher(key.toString());
            while (matcher.find()) {
                
                String company = matcher.group().toString().split("\\|")[0];
            	outkey.set(company);
            //	outkey.set(a);
                context.write(outkey, outvalue);
			
		}
		
	}
}

