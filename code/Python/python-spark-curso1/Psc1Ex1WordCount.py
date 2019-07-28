"""
    Ejemplo basico de conteo de palabras usando apache spark con python
    El ejemplo se ejecutara en una instalacion Standalone de Apache Spark local.
"""

"""
    Se importa SparkContext y SpackConf las cuales son el punto de partida para 
    Apache Spark y la configuracion
"""
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    """
        Configuracion de apache spark
        word count es el nombre de la aplicación con el cual se indentifica el programa
        cuando se ejecuta desde un cluster de apache spark
        local[1] indica que se ejecutara en local y la cantidad de cores que se usaran para el procesamiento.
        local[2] 2 cores
        local[*] todos los cores disponibles
        local es similar a local[1] solo se usa 1 core.
    """
    conf = SparkConf().setAppName("word count").setMaster("local[1]")
    
    """
        Spark context es el punto de entrada a Spark y representa la conexion a un cluster Spark.
    """
    sc = SparkContext(conf = conf)

    """
        Se lee el dataset a procesar, en este caso se usa una ruta absoluta por facilidad.
        Tener cuidado de cambiar la ruta por la adecuada cuando se ejecute el ejemplo
    """
    lines = sc.textFile("/Users/felixernestoorduzgrimaldo/Documents/Estudio/Apache Spark/datasets/books/dracula.txt")

    """
        se utiliza una expresion lambda para dividir cada linea por espacio (por palabra)
        la estructura se almance en la varible words
    """
    words = lines.flatMap(lambda line: line.split(" "))

    """
        cuenta cada palabra
    """
    wordCounts = words.countByValue()

    """
        se reccorre el RDD por cada entrada, imprimiendo la palabra y las veces que se ha contado.
    """
    for word, count in wordCounts.items():
        print("{} : {}".format(word, count))