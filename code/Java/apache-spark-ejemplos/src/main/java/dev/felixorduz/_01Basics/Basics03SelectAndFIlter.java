package dev.felixorduz._01Basics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Ejemplo de uso de la sintaxis SQL para realizar consultas en un RDD de Apache Spark
 */

public class Basics03SelectAndFIlter {

    public static void main( String[] args){

        /**
         * Ejemplo de proyeccion (SELECT) y filtadaro
         * inpirado en:
         * - https://github.com/inigoserrano/SparkWeather/blob/master/program/src/main/java/com/inigoserrano/sparkWeather/SparkWeather02.java
         * - https://spark.apache.org/docs/latest/sql-getting-started.html#running-sql-queries-programmatically
         *
         */

        /**
         * Configuracion de apache spark
         * Basics03SelectAndFilter es el nombre de la aplicación con el cual se indentifica el programa cuando se ejecuta
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
                .appName("Basics03SelectAndFilter")
                .getOrCreate();

        /**
         * Lectura de un archivo csv desde disco usando una ruta estatica que se almacena en un Dataset
         * el metodo read() indica que se va leer un archivo
         * el metodo option() permite ingresar diferentes opciones de lectura
         * la opcion header indica que la primera fila se utiliza como cabecera para asignarle nombres a los campos
         * el metodo csv() recibe como parametro la ruta del archivo csv
         * Se debe tener cuidado con la ruta absoluta del archivo CSV
         */
        Dataset<Row> df = sparkSession
                .read()
                .option("header","true")
                .csv("/Users/felixernestoorduzgrimaldo/Documents/Estudio/Apache Spark/datasets/fifa19.csv");

        /**
         * Crea una vista temporal a partir del dataset, sobre la vista temporal es la que se ejecutan las consultas
         * SQL.
         * En el ejmplo se usa players como nombre de la vista.
         */
        df.createOrReplaceTempView("players");

        /**
         *  El metodo sql() se usa para realizar una consulta en sintaxis SQL sobrre la vista temporal creada del
         *  DATASET.
         *  El resultado es un dataset en este ejemplo la variable players
         */
        Dataset<Row> players = sparkSession.sql("SELECT name, age FROM players");

        /**
         * El metodo show() imprime las primeras filas del dataset
         */
        players.show();

        /**
         * Se realiza una busqueda con un filtro (where) sobre la vista temporal, en este ejemplo se buscan lo jugadores
         * menores de 25 años.
         */
        Dataset<Row> juniors = sparkSession.sql("SELECT name, age FROM players where age < 25");

        /**
         * El metodo show() imprime las primeras filas del dataset Juniors
         */
        juniors.show();

    }
}
