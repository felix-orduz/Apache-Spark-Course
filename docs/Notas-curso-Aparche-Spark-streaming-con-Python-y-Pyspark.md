# Notas curso: Aparche Spark streaming con Python y PySpark - Udemy

Url: [https://www.udemy.com/aparche-spark-con-python-y-pyspark](https://www.udemy.com/aparche-spark-con-python-y-pyspark)

# Resumen General

# Secciones

## Sección 1: Comenzado con Apache Spark

Apache spark esta construido sobre SCALA

Código Fuente de los ejercicios: [https://github.com/jleetutorial/python-spark-tutorial](https://github.com/jleetutorial/python-spark-tutorial)

Apache SPARK tiene una API para Pyhton llamada PYSPARK pero esta igualmente corre sobre JAVA por lo tanto es necesario tener el JDK instalado.

Apache SPARK requiere java 8, las versiones superiores de java no son soportadas, por el cambio del API de acceso a memoria en JAVA 9, se espera que en la version de SPARK 3 ya sea compatible con JAVA 9, 10 y 11.

### Instalación:

Descargar la ultima version de la pagina de apache spark [https://spark.apache.org/downloads.html](https://spark.apache.org/downloads.html)

En este ejemplo se usa la version 2.4.3 con el tipo de paquete: Pre-Built for Apache Hadoop 2.7 and later

Se debe descomprimir el archivo descargado en una ruta del equipo (para equipos Unix como el mac).

La ruta donde se descomprime se debe guardar en la variable de entorno SPARK_HOME y agregar SPARK_HOME/bin al path como se muestra a continuacion:

Configuracion de la variable de entorno en el MAC cambiar ~/.bash_profile y ~/.zshrc

    export SPARK_HOME=/Users/felixernestoorduzgrimaldo/Documents/Estudio/Spark/opt/apache-spark/spark-2.4.3-bin-hadoop2.7
    export PATH=$PATH:$SPARK_HOME/bin

Se debe tener cuidado de la versión de java que se encuentra configurada, si se tienen mas de una version o se usan herramientas como JENV o SDKMAN tener cuidado cual version esta configurada actualmente en la terminal donde se usa APACHE SPARK porque puede generar problemas siempre dejar una version 1.8.

para probar que todo quedo configurado adecuadamente ejecutar en consola: **pyspark**

Pueden salir mensajes WARN pero no ERROR.

Para evitar tener tantos mensajes de log se puede cambiar el nivel de error de SPARK para eso, vamos a la ruta de instalacion y luego al directorio conf.

hacer una copia del archivo [log4j.properties](http://log4j.properties).template y crearlo sin la extension .propierties solamente: log4j.properties y cambiar el valor de log4j.rootCategory a: **ERROR, console**

    # Set everything to be logged to the console
    log4j.rootCategory=ERROR, console

Para que la ejecucion  en MAC use PYTHON3 se debe crara una varible de entorno PYSPARK_PYTON=python3 esto es porque en mac el binario python hace referencia a python 2.

cambiar ~/.bash_profile y ~/.zshrc

    export PYSPARK_PYTHON=python3

En el repositorio del proyecto de ejemplo (code/python-spark-tutorial) ejecutar:

    spark-submit rdd/WordCount.py

Esto ejecutara un ejemplo de contar palabras en Python sobre Apache Spark. Este ejemplo es solamente para demostrar como se ejecuta un codigo en Apache Spark.