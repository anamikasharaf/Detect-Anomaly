

package Lab1;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import java.io.IOException;
import java.util.StringTokenizer;

public class MRmapper2  extends Mapper <LongWritable,Text,Text,Text> {
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

	        //write (key, value) pair to context
		
		String line = value.toString();
		String[] split_value = line.split("\\s+");
		String a = "a";
		context.write( new Text(a),new Text(split_value[0] + "_" + split_value[1]));
		

}
}
