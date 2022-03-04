import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;

public class MapMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1); //la clase propia para definir enteros de hadoop
	private Text word = new Text(); //almacenamos cada una de las palabras con las que vamos a crear las variables intermedias.

	String pattern = "(\\W?\\[)(warn|severe|info)(\\]\\W?)"; //Almacenamos el patron por el que vamos a dividir las lineas de texto, es decir todas las cadenas de caracteres que se encuentren entre espacios o principios de linea.

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String txt = value.toString();	
		txt = txt.replaceAll("[^(a-zA-Z\\[\\])]" , " ").toLowerCase(); //eliminamos todos los simbolos no deseados y nos quedamos solo con palabras. Y lo pasamos todo a minusculas.
		
		StringTokenizer tokenizer = new StringTokenizer(txt);	
		
		while (tokenizer.hasMoreTokens()) {		
			word.set(tokenizer.nextToken());
			
			if(word.toString().matches(pattern)){
				context.write(word, one);  // escribimos en el contexto de la aplicacion. la tupla es propia palabra y uno.
			}		
	
		}
	  
	}
}