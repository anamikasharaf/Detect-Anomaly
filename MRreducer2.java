package Lab1;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;

public class MRreducer2  extends Reducer <Text,Text,Text,DoubleWritable> {
   public void reduce(Text key, Iterable<Text> values, Context context) 
		   throws IOException, InterruptedException {
	// parse out (key, values)
		int count =0;
		double sum = 0;
		double mean = 0;
		double xi = 0;
		double xi_sum = 0;
		double xi_square = 0;
		double sigmatemp = 0;
		double sigma = 0;
		List<String> temp_name = new ArrayList<String>();
		List<Double> temp_value = new ArrayList<Double>();
		for( Text value: values) {
			String compositeString = value.toString();
			String[] compositeStringArray = compositeString.split("_");
			temp_name.add(compositeStringArray[0]);
			temp_value.add(Double.parseDouble(compositeStringArray[1]));
			sum += temp_value.get(count);
			count += 1;
		}
		

	// calculate mean_failed_login_attempts and write to context
		
		mean = sum/count;
		
	// calculate sigma_failed_login_attempts and write to context

		for(int i = 0; i< temp_name.size(); i++){
			xi = (temp_value.get(i)) - mean;
			xi_square = xi * xi;
			xi_sum += xi_square;
		}
		sigmatemp = xi_sum/count;
		sigma = Math.sqrt(sigmatemp);
	
		context.write(new Text("mean_failed_login_attempts"),new DoubleWritable(mean));
		context.write(new Text("sigma_failed_login_attempts"), new DoubleWritable(sigma));
		for(int i=0; i<temp_name.size(); i++) {
			String message = "num_sigmas_for:" + temp_name.get(i);
			double number = ((temp_value.get(i)) - mean)/sigma;
			context.write(new Text(message), new DoubleWritable(number));
			
		}
		
   }
}
