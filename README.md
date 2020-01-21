# Hadoop_Movie_Project
This project uses the Apache Hadoop Distributed File System (HDFS) to merge two large datasets containing movie data and gain insights and analytics into the movies of the year for the year 2018

## Java scrapper
### Extracts the day of the year in 2018 and the top grossing movie for that day and stores it in a file called movie.txt

import java.io.IOException;
import java.io.PrintWriter;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class Movie {
	public static void main(String args[]){

		Document document;
		
		try {
			System.out.println("running...");
			PrintWriter writer = new PrintWriter("movie.txt", "UTF-8");
			document = Jsoup.connect("https://www.boxofficemojo.com/daily/2018/?view=year&sort=date&sortDir=asc&ref_=bo_di__resort#table").get();

				Elements links = document.getElementsByClass("a-text-left mojo-field-type-release mojo-cell-wide"); 
				int day = 0;
				for (Element link : links) {
										
					String movie = link.text();
					day++;
					
					writer.println(movie + "\t" + "$" + day);
					//writer.printf("\n");
					
					}
			writer.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		  }
		System.out.println("done!");
	}

}

### Next is a single mapper that reads the output of this file: movie.txt as well as another dataset named revenue.txt ### that contains the name of the movie and its gross revenues for 2018. The name of the movie is the map output join ### key so that the records for each movie in both files are brought together in the reducer producing a file        ### containing the following output: movie name, gross revenues, number of days in the top spot.

#### Mapper

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

#### Mapper Output





