package dev.felixorduz._01Basics;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


import java.util.Arrays;
import java.util.List;

/**
 * Ejemplo donde se lee un archivo en formato CSV con Apache Spark (DATASET) para luego guardarse en formato JSON
 */
public class Basics02SaveToJson {

    public static void main ( String[] args){


        /**
         * Ejemplo inspirado en:
         * - https://spark.apache.org/docs/latest/sql-getting-started.html
         * - https://github.com/inigoserrano/SparkWeather/blob/master/program/src/main/java/com/inigoserrano/sparkWeather/SparkWeather01.java
         */


        /**
         * Configuracion de apache spark
         * Basics02SaveToJson es el nombre de la aplicación con el cual se indentifica el programa cuando se ejecuta
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
                .appName("Basics02SaveToJson")
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
         * El metodo show() imprime las primeras filas del dataset
         */
        df.show();

        /**
         * El metodo printSchema() muestra en consola el esquema del dataset donde muestra el nombre del campo el tipo
         * de datos y si puede ser nulo o no
         */
        df.printSchema();

        /**
         * Exporta en formato JSON el dataset.
         * Se debe tener cuidado con la ruta absoluta del archivo JSON
         */
        df.write().json("/Users/felixernestoorduzgrimaldo/Documents/Estudio/Spark/workdir/out/fifa19/");


    }
}

