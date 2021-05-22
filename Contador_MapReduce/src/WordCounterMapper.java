import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1); //Clase para poder definir enteros en Hadoop
	private final static Pattern SPLIT_PATTERN = Pattern.compile("\\s*\\b\\s*"); //Alcenaremos el patron, realizando la division por TOKENS
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  
	  String line = value.toString(); //El valor que toma se convierte en clase Text que es el string manejado por Haddop
	  line = line.replace("[^a-zA-Z0-9 ]", ""); //Eliminamos simbolos no deseados
	  Text currentWord = new Text(); //Almacenando las palabras con la que vamos a crear las nuevas claves intermedias
	  
	  String words[] = SPLIT_PATTERN.split(line); //Arreglo de string donde vamos guardaremos cada palabra
	  for(int i=0;i<words.length;i++){ //Aqui se podra emitir las claves y los valores intermedios
		  if(words[i].isEmpty()){ //Corroborando si existen espacios
			  continue;
		  }
		  currentWord = new Text(words[i]); //Aqui comparamos las palabras que vamos encontrando si algo sucede se cumplira String JAVA -> String HADOOP
		  context.write(currentWord, one); //Ahora relatamos el contexto de la app
		  
	  }
  }
}