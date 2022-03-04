import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {

	  public static void main(String[] args) throws Exception {

		  int res = ToolRunner.run(new Driver(), args);
		  System.exit(res);
	  }	  
	  
	  public int run(String[] args) throws Exception {
		  Job job = Job.getInstance(getConf()," Word Counter");//le asignamos un nombre
		  
			job.setJarByClass(this.getClass()); // la clase que contenga el metodo de entrada a la aplicacion
			
			FileInputFormat.addInputPath(job,new Path(args[0])); // path de entrada de proceso, indicamos donde se encuentra el fichero a contar, y el argumento 0 para indicar la entrada
			FileOutputFormat.setOutputPath(job,new Path(args[1])); // path de salida de proceso, fichero de texto donde estaran todas las palabras que hemos contado.
			
			job.setMapperClass(MapMapper.class); //indicar las clase que se encarga de los procesos map
			job.setReducerClass(MapReducer.class); //indicar las clase que se encarga de los procesos reducer
			  
			job.setOutputKeyClass(Text.class); //La salida sera la palabra de tipo text de hadoop que nos dirá la palabra mas repetida.
			
			job.setOutputValueClass(IntWritable.class); // y el valor sera de tipo intwritable que sera el numero de veces que aparece una palabra
			  
			return job.waitForCompletion(true) ? 0 : 1; //Indicamos que el trabajo debe esperar a que se complete, y una vez que se complete diga 0 si ha ido bien y 1 si no.
	  }
	}

