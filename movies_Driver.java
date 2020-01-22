import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class movies_driver {

  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: ReduceJoin <customer input path> <transaction input path> <output path>");
      System.exit(-1);
    }
    
    Job job = new Job();
    job.setJarByClass(movies_driver.class);
    job.setJobName("movies_driver");

	MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, movies_Mapper.class);
	MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, movies_Mapper.class);
	    
    job.setReducerClass(movies_Reducer.class);
	FileOutputFormat.setOutputPath(job, new Path(args[2]));

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}