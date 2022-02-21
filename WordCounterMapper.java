import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1); //la clase propia para definir enteros de hadoop
	private final static Pattern SPLIP_PATTERN = Pattern.compile("\\s*\\b\\s*"); //Almacenamos el patron por el que vamos a dividir las lineas de texto, es decir todas las cadenas de caracteres que se encuentren entre espacios o principios de linea.
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

	  String line = value.toString();
	  line = line.replaceAll("[^a-zA-Z0-9 ]","").toLowerCase(); //eliminamos todos los simbolos no deseados y nos quedamos solo con palabras. Y lo pasamos todo a minusculas.
	  Text currentWord = new Text(); //almacenamos cada una de las palabras con las que vamos a crear las variables intermedias.
	  
	  String words[] = SPLIP_PATTERN.split(line); // array de strings donde vamos a meter cada una de las palabras
	  
	  for(int i = 0; i < words.length; i++){
		  if (words[i].isEmpty()){
			  continue; // si es un espacio que continue con el bucle.
		  }
		  currentWord = new Text(words[i]); // la palabra actual es igual a la palabra que este en el arrray de palabras.  
		  context.write(currentWord, one); // escribimos en el contexto de la aplicacion. la tupla es propia palabra y uno.
	  }
	  

  }
}
