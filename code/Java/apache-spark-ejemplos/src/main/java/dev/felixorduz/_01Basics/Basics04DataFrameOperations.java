package dev.felixorduz._01Basics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
// $example on:untyped_ops$
// col("...") is preferable to df.col("...")
import static org.apache.spark.sql.functions.col;

/**
 * Ejemplo del lenguaje especifico de manipulacion de Dataset
 */
public class Basics04DataFrameOperations {

    public static void main (String[] args){

        /**
         * Proyeccion y filtradro usando API spark
         * inpirado en:
         *  - https://github.com/inigoserrano/SparkWeather/blob/master/program/src/main/java/com/inigoserrano/sparkWeather/SparkWeather03.java
         *  - https://spark.apache.org/docs/latest/sql-getting-started.html#untyped-dataset-operations-aka-dataframe-operations
         */

        /**
         * Configuracion de apache spark
         * Basics04DataFrameOperations es el nombre de la aplicación con el cual se indentifica el programa cuando se ejecuta
         * desde un cluster de apache spark
         *
         * local[1] indica que se ejecutara en local y la cantidad de cores que se usaran para el procesamiento.
         * local[2] 2 cores
         * local[*] todos los cores disponibles
         * local es similar a local[1] solo se usa 1 core.
         *
         */
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local")
                .appName("Basics04DataFrameOperations")
                .getOrCreate();

        /**
         * Lectura de un archivo csv desde disco usando una ruta estatica que se almacena en un Dataset
         * el metodo read() indica que se va leer un archivo
         * el metodo option() permite ingresar diferentes opciones de lectura
         * la opcion header indica que la primera fila se utiliza como cabecera para asignarle nombres a los campos
         * el metodo csv() recibe como parametro la ruta del archivo csv
         * Se debe tener cuidado con la ruta absoluta del archivo CSV
         */
        Dataset<Row> dfFifa = sparkSession
                .read()
                .option("header","true")
                .csv("/Users/felixernestoorduzgrimaldo/Documents/Estudio/Apache Spark/datasets/fifa19.csv");

        /**
         * El metodo select() permite seleccionar unas columnas del dataset,( proyeccion sql )
         * El metodo show() muestra en consola el dataset
         */
        dfFifa.select("name").show();

        /**
         * El ejemplo usa el metodo col() para realizar la proyeccion de las columnas, en este ejemplo se realiza
         * le proyeccion de dos columnas, name y age.
         * El metodo show() muestra en consola el dataset
         */
        dfFifa.select(col("name"), col("age")).show();

        /**
         * el metodo filter() se utliza para agregar condiciones de filtrado.
         * el metodo lt() agrega una condicion menor que sobre la columna definida en col()
         * el ejemplo realiza una consulta de todos los jugadores menores de 25 años, aca se muetran todos los campos
         * (columnas)
         */
        dfFifa.filter(col("age").lt(25)).show();

        /**
         * Ejemplo similar al anterior con la diferencia que antes de realizar el filtro se realiza la proyeccion
         * de las columnas name y age.
         *
         */
        dfFifa.select(col("name"), col("age")).filter(col("age").lt(25)).show();

        /**
         * El ejemplo es igual al anterior, se proyectan los campos name y age pero el filtro se especifica usando
         * una sintaxis diferentes (se especifica el campo y la condicion en modo similar a sql)
         */
        dfFifa.select(col("name"), col("age")).filter("age<25").show();

    }
}
