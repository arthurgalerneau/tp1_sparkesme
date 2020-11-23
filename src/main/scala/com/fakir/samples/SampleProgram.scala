package com.fakir.samples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession



object SampleProgram {

  def majuscule(s: String, filtre: String): String = {
    if(s.contains(filtre)) s
    else s.toUpperCase
  }


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    //Exercice 1 - RDD
    //Question 1
    val rdd = sparkSession.sparkContext.textFile("data")
    //Question 2
    val rddleo = rdd.filter(elem => elem.contains("Di Caprio"))
    println("Combien y a-t-il de films de Leonardo Di Caprio dans ce fichier ?")
    println(rddleo.count())
    //Question 3
    val moyenne = rddleo.map( elem => elem.split(";")(2).toDouble)
    var note = moyenne.collect()
    var counter = 0.0
    for(i<-0 until note.length)
    {
      counter += note(i)
    }
    var moy = counter / note.length
    println("Quelle est la moyenne des notes des films de Di Caprio ?")
    println(moy)
    //Question 4
    val vueleo = rddleo.map( elem => elem.split(";")(1).toDouble)
    var vueleodi = vueleo.collect()
    var vueleosum = 0.0
    for(i<-0 until vueleodi.length)
    {
      vueleosum += vueleodi(i)
    }
    val vueall = rdd.map( elem => elem.split(";")(1).toDouble)
    var vueallmov = vueall.collect()
    var vueallmoviesum = 0.0
    for(i<-0 until vueallmov.length)
    {
      vueallmoviesum += vueallmov(i)
    }
    var pourc = ( vueleosum / vueallmoviesum) * 100
    println("Quel est le pourcentage de vues des films de Di Caprio par rapport à l'échantillon que nous avons ?")
    println(pourc)
    //Question 5
    val counts = rdd.map(item => (item.split(";")(3), (1.0, item.split(";")(2).toDouble)) )
    val countSums = counts.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2 ))
    val keyMeans = countSums.mapValues(avgCount => avgCount._2 / avgCount._1)
    println("Quelle est la moyenne des notes par acteur dans cet échantillon ? ")
    keyMeans.foreach(println)
    //Question 6
    val counts6 = rdd.map(item => (item.split(";")(3), (1.0, item.split(";")(1).toDouble)) )
    val countSums6 = counts6.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2 ))
    val keyMeans6 = countSums6.mapValues(avgCount => avgCount._2 / avgCount._1)
    println("Quelle est la moyenne des vues par acteur dans cet échantillon")
    keyMeans6.foreach(println)

    //Exercice 2 - DataFrames
    import org.apache.spark.sql.functions._
    //Question 1
    val df = sparkSession.read.option("header", false).option("delimiter", ";").option("inferSchema", true).csv("data")
    //Question 2
    val df1 = df.withColumnRenamed("_c0", "nom_film")
    val df2 = df1.withColumnRenamed("_c1", "nombre_vues")
    val df3 = df2.withColumnRenamed("_c2", "note_film")
    val df4 = df3.withColumnRenamed("_c3", "acteur_principal")
    println("Nommez les colonnes comme suit : nom_film, nombre_vues, note_film, acteur_principal")
    df4.show()
    //Question 3
    val count_ = df4.filter(col("acteur_principal") === "Di Caprio").count()
    println("Combien y a-t-il de films de Leonardo Di Caprio dans ce fichier ?")
    println(count_)
    val meannote = df4.filter(col("acteur_principal") === "Di Caprio").groupBy("acteur_principal").mean("note_film")
    println("Quelle est la moyenne des notes des films de Di Caprio ?")
    meannote.show()
    val sumall = df4.agg(sum("nombre_vues").cast("double")).first().getDouble(0)
    val sumleo = df4.filter(col("acteur_principal") === "Di Caprio").agg(sum("nombre_vues").cast("double")).first().getDouble(0)
    var pourc1 = (sumleo / sumall) * 100
    println("Quel est le pourcentage de vues des films de Di Caprio par rapport à l'échantillon que nous avons ?")
    println(pourc1)
    val meannoteall = df4.groupBy("acteur_principal").mean("note_film")
    println("Quelle est la moyenne des notes par acteur dans cet échantillon ?")
    meannoteall.show()
    val meannoteallvue = df4.groupBy("acteur_principal").mean("nombre_vues")
    println("Quelle est la moyenne des vues par acteur dans cet échantillon ?")
    meannoteallvue.show()
    //Questions 4
    val df5 = df4.withColumn("pourcentage_de_vues", (col("nombre_vues") / sumall ) * 100 )
    println("Créer une nouvelle colonne dans ce DataFrame, \"pourcentage de vues\", contenant le pourcentage de vues pour chaque film (combien de fois les films ont-ils été vus par rapport aux vues globales ?)")
    df5.show()
  }
}