import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
	  int sum = 0; //Guardamos el numero de cada una de las palabras correspondientes a la clave intermedia segun las ocurrencias que encontremos
	  for(IntWritable count : values){  
		  sum+=count.get(); //Realizamos la suma al contador
	  }
	  context.write(key,new IntWritable(sum)); //Tomamos todas las veces que se encuentra la palabra y sumar
  }
}