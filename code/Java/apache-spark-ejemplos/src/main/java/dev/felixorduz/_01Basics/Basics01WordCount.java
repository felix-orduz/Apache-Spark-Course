package dev.felixorduz._01Basics;

        import org.apache.spark.SparkConf;
        import org.apache.spark.api.java.JavaPairRDD;
        import org.apache.spark.api.java.JavaRDD;
        import org.apache.spark.api.java.JavaSparkContext;
        import org.apache.spark.rdd.RDD;
        import org.apache.spark.sql.SparkSession;
        import scala.Tuple2;

        import java.util.Arrays;
        import java.util.List;

        import static org.apache.commons.lang3.StringUtils.SPACE;

/**
 * Ejemplo basico de uso de Apache Spark con JAVA.
 *
 * En este ejemplo se lee un dataset que es un archivo plano txt, en el ejemplo se usa la novela de Dracula
 *
 * En el segundo paso se hace un proceso de mapear el dataset completo por palabra y luego se realiza un proceso de
 * reduce para contar cada palabra.
 *
 * En el tercer paso se imprime en consola cada entrada.
 *
 * por ultimo hay una linea comentada con un ejemplo de como exportar el contenido a un archivo en disco.
 */
public class Basics01WordCount
{
    /**
     * Lee un dataset y po
     * @param args
     */
    public static void main( String[] args )
    {

        /**
         * Ejemplo inspirado en:
         * - https://spark.apache.org/docs/latest/rdd-programming-guide.html
         * - https://github.com/inigoserrano/SparkWeather/blob/master/program/src/main/java/com/inigoserrano/sparkWeather/SparkWeather02.java
         */

        /**
         * Configuracion de apache spark
         * Basics01WordCount es el nombre de la aplicación con el cual se indentifica el programa cuando se ejecuta
         * desde un cluster de apache spark
         *
         * local[1] indica que se ejecutara en local y la cantidad de cores que se usaran para el procesamiento.
         * local[2] 2 cores
         * local[*] todos los cores disponibles
         * local es similar a local[1] solo se usa 1 core.
         *
         */
        final SparkConf sparkConf = new SparkConf().setAppName("Basics01WordCount").setMaster("local[1]");

        /**
         * Spark context es el punto de entrada a Spark y representa la conexion a un cluster Spark.
         */
        final JavaSparkContext sc = new JavaSparkContext(sparkConf);

        /**
         * Se lee el dataset a procesar por SPARK RDD, en este caso se usa una ruta absoluta por facilidad.
         * Tener cuidado de cambiar la ruta por la adecuada cuando se ejecute el ejemplo
         */
        JavaRDD<String> textFile = sc.textFile("/Users/felixernestoorduzgrimaldo/Documents/Estudio/Apache Spark/datasets/books/dracula.txt",1);


        /**
         * Procedimiento de MapReduce para contar las palabras del texto.
         *
         * El metodo flatMap recibe un lambda, es este caso cada linea leida se divide por " " espacio para formar una
         * lista.
         *
         * Luego se mapea cada
         *
         */
        JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);

        /**
         * se imprimen cada una de las entradas, en este caso la palabra con la cantidad de veces
         *
         */
        counts.foreach(data -> {
            System.out.println(data._1 + " - " + data._2);
        });

        /**
         * Ejemplo de como exportar a un texto plano el contenido del RDD.
         * Tener cuidado con la ruta a exportar.
         */
        //counts.saveAsTextFile("/Users/felixernestoorduzgrimaldo/Documents/Estudio/Spark/workdir/out/wordcount/");
    }
}

