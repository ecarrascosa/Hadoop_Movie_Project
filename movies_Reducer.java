import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class movies_Reducer
  extends Reducer<Text, Text, Text, Text> {
  
  @Override
  public void reduce(Text key, Iterable<Text> values,
      Context context)
      throws IOException, InterruptedException {
    
    int count =0;
    String revenue = "null";

    for (Text value : values) 
	{
	
	String word = value.toString();


	if(word.contains(","))
	{
	
	revenue = word;

	}
	else
	{
		
     	 count ++;
	}

  	}

	
	String str = revenue + " " + count;


	context.write(key, new Text(str));
}
}
//Top_grossing_movie reducer

