import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class movies_Mapper
  extends Mapper<LongWritable, Text, Text, Text> {

	enum Movies {
	deadpool_aquaman
	};
    
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    

    	String line = value.toString().toLowerCase();; //Changes the value to lowercase string
	
	//&& !line.contains(",") || line.contains("Aquaman") && !line.contains(",")

	if(line.contains("deadpool") && !line.contains(",") || line.contains("aquaman") && !line.contains(","))
	{	
		context.getCounter(Movies.deadpool_aquaman).increment(1);
	}


	String[] str = line.split("\\$");
	
	//int length = str.length;
	
	//String v = str[length - 1];

	context.write(new Text(str[0]), new Text(str[1]));
	
  } 
}