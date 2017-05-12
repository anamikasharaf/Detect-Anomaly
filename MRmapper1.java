/**
 * @author Anamika Sharaf
 * @date April 7, 2017
 * 
 */


package Lab1;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;
import java.util.StringTokenizer;

public class MRmapper1  extends Mapper <LongWritable,Text,Text,IntWritable> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	// TODO: filter failed USER_LOGIN records, discard the rest
		
		 		String[] temp = value.toString().split(" ");
				if(temp[0].equals("type=USER_LOGIN") && temp[13].equals("res=failed'"))
				{
					String acctname = temp[8];
					String[] acct = acctname.split("=");
					String acct_name = acct[1];
					String[] acct_name_check = acct_name.split("\"");
					if(acct_name_check.length == 2){
						String acct_final = acct_name_check[1];
						context.write(new Text(acct_name),new IntWritable(1));
					}
					
				}
	}
	}
