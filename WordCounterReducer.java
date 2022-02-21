import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> { //sumará el numero de veces que aparezca una palabra.

  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {

	  int sum = 0;
	  
	  for (IntWritable count : values){ //for iterable 
		  sum += count.get();
	  }
	  
	  context.write(key, new IntWritable(sum)); //recogemos todas las veces que aparece una palabra y realizar la suma.

  }
}