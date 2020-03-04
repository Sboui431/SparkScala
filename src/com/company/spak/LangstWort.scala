package com.company.spak

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.parquet.format.StringType
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
 

object LangstWort {
   
 
   def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using the local machine
    val conf=new SparkConf().setAppName("LangstWort")
    val sc = new SparkContext("local", "LangstWort")
    conf.get("spark.executor.cores","1")
    
    val start=System.currentTimeMillis()
    
    
    // Load each line of my File into an RDD
   
    val input = sc.textFile("Text\\Deutsch\\TXT\\*txt")
   
    
    // Split using a regular expression that extracts words
    val words = input.flatMap(x => x.split("\\W+"))
    
    // Normalize everything to lowercase
    val lowercaseWords = words.map(x => x.toLowerCase())
    
    // Count of the occurrences of each word
    val wordCounts = lowercaseWords.map(x => (x, 1)).reduceByKey( _+_ )
    case class Order(LangsteWort:String ,Lange : Int)
    val WordLength=wordCounts.map{case (word,count)=>(word, word.length())}
    
   
    //---------------------------English File--------------------------
    
     val inputEnglish = sc.textFile("Text\\English\\TXT\\*txt")
    
    // Split using a regular expression that extracts words
    val wordsEnglish = inputEnglish.flatMap(x => x.split("\\W+"))
    
    // Normalize everything to lowercase
    val lowercaseWordsEnglish = wordsEnglish.map(x => x.toLowerCase())
    
    // Count of the occurrences of each word
    val wordCountsEnglish = lowercaseWordsEnglish.map(x => (x, 1)).reduceByKey( _+_ )
    case class Order2(LangsteWort2:String ,Lange2 : Int)
    val WordLengthEnglish=wordCountsEnglish.map{case (word2,count2)=>(word2, word2.length())}
    
  
    //---------------------------Espanol File--------------------------
    
     val inputEspanol = sc.textFile("Text\\Espanol\\TXT\\*txt")
    
    // Split using a regular expression that extracts words
    val wordsEspanol = inputEspanol.flatMap(x => x.split("\\W+"))
    
    // Normalize everything to lowercase
    val lowercaseWordsEspanol = wordsEspanol.map(x => x.toLowerCase())
    
    // Count of the occurrences of each word
    val wordCountsEspanol = lowercaseWordsEspanol.map(x => (x, 1)).reduceByKey( _+_ )
    //case class Order2(LangsteWort2:String ,Lange2 : Int)
    val WordLengthEspanol=wordCountsEspanol.map{case (word2,count2)=>(word2, word2.length())}
     
     //---------------------------Francais File------------------------------------------------------
     
      val inputFrancais = sc.textFile("Text\\Francais\\TXT\\*txt")
    
    // Split using a regular expression that extracts words
    val wordsFrancais = inputFrancais.flatMap(x => x.split("\\W+"))
    
    // Normalize everything to lowercase
    val lowercaseWordsFrancais = wordsFrancais.map(x => x.toLowerCase())
    
    // Count of the occurrences of each word
    val wordCountsFrancais = lowercaseWordsFrancais.map(x => (x, 1)).reduceByKey( _+_ )
    //case class Order2(LangsteWort2:String ,Lange2 : Int)
    val WordLengthFrancais=wordCountsFrancais.map{case (word2,count2)=>(word2, word2.length())}
      
       //---------------------------Italiano File------------------------------------------------------
     
      val inputItaliano = sc.textFile("Text\\Italiano\\TXT\\*txt")
    
    // Split using a regular expression that extracts words
    val wordsItaliano = inputItaliano.flatMap(x => x.split("\\W+"))
    
    // Normalize everything to lowercase
    val lowercaseWordsItaliano = wordsItaliano.map(x => x.toLowerCase())
    
    // Count of the occurrences of each word
    val wordCountsItaliano = lowercaseWordsItaliano.map(x => (x, 1)).reduceByKey( _+_ )
    //case class Order2(LangsteWort2:String ,Lange2 : Int)
    val WordLengthItaliano=wordCountsItaliano.map{case (word2,count2)=>(word2, word2.length())}
    
    //---------------------------Nederlands File------------------------------------------------------
     
      val inputNederlands = sc.textFile("Text\\Nederlands\\TXT\\*txt")
    
    // Split using a regular expression that extracts words
    val wordsNederlands = inputNederlands.flatMap(x => x.split("\\W+"))
    
    // Normalize everything to lowercase
    val lowercaseWordsNederlands = wordsNederlands.map(x => x.toLowerCase())
    
    // Count of the occurrences of each word
    val wordCountsNederlands = lowercaseWordsNederlands.map(x => (x, 1)).reduceByKey( _+_ )
    //case class Order2(LangsteWort2:String ,Lange2 : Int)
    val WordLengthNederlands=wordCountsNederlands.map{case (word2,count2)=>(word2, word2.length())}
      
       //---------------------------Русский File------------------------------------------------------
     
      val inputРусский = sc.textFile("Text\\Русский\\TXT\\*txt")
    
    // Split using a regular expression that extracts words
    val wordsРусский = inputРусский.flatMap(x => x.split("\\P{L}+"))
    
    // Normalize everything to lowercase
    val lowercaseWordsРусский = wordsРусский.map(x => x.toLowerCase())
    
    // Count of the occurrences of each word
    val wordCountsРусский = lowercaseWordsРусский.map(x => (x, 1)).reduceByKey( _+_ )
    //case class Order2(LangsteWort2:String ,Lange2 : Int)
    val WordLengthРусский=wordCountsРусский.map{case (word2,count2)=>(word2, word2.length())}
      
       //---------------------------Українська File------------------------------------------------------
     
      val inputУкраїнська = sc.textFile("Text\\Українська\\TXT\\*txt")
    
    // Split using a regular expression that extracts words
    val wordsУкраїнська = inputУкраїнська.flatMap(x => x.split("\\P{L}+"))
    
    // Normalize everything to lowercase
    val lowercaseWordsУкраїнська = wordsУкраїнська.map(x => x.toLowerCase())
    
    // Count of the occurrences of each word
    val wordCountsУкраїнська = lowercaseWordsУкраїнська.map(x => (x, 1)).reduceByKey( _+_ )
    //case class Order2(LangsteWort2:String ,Lange2 : Int)
    val WordLengthУкраїнська=wordCountsУкраїнська.map{case (word2,count2)=>(word2, word2.length())}
      
    
      
     val spark = SparkSession
      .builder
      .appName("LangstWort")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") 
      .getOrCreate()
      
      import spark.implicits._

      val Affichage=WordLength.toDF("LangstesWort","Lange")
      Affichage.printSchema()
      Affichage.createOrReplaceTempView("ord")
      
      
      //DataFrame English
      import spark.implicits._
      val AffichageEnlish=WordLengthEnglish.toDF("Wort2","Lange2")
      AffichageEnlish.printSchema()
      AffichageEnlish.createOrReplaceTempView("ord2")
      
      //DataFrame Espanol
      import spark.implicits._
      val AffichageEspanol=WordLengthEspanol.toDF("Wort3","Lange3")
      AffichageEspanol.printSchema()
      AffichageEspanol.createOrReplaceTempView("ord3")
     
      
      //DataFrame Francais
      import spark.implicits._
      val AffichageFrancais=WordLengthFrancais.toDF("Wort4","Lange4")
      AffichageFrancais.printSchema()
      AffichageFrancais.createOrReplaceTempView("ord4")
      
        //DataFrame Italiano
      import spark.implicits._
      val AffichageItaliano=WordLengthItaliano.toDF("Wort5","Lange5")
      AffichageItaliano.printSchema()
      AffichageItaliano.createOrReplaceTempView("ord5")
      
      //DataFrame Nederlands
      import spark.implicits._
      val AffichageNederlands=WordLengthNederlands.toDF("Wort6","Lange6")
      AffichageNederlands.printSchema()
      AffichageNederlands.createOrReplaceTempView("ord6")
      
      //DataFrame Русский
      import spark.implicits._
      val AffichageРусский=WordLengthРусский.toDF("Wort7","Lange7")
      AffichageРусский.printSchema()
      AffichageРусский.createOrReplaceTempView("ord7")
      
      //DataFrame Українська
      import spark.implicits._
      val AffichageУкраїнська=WordLengthУкраїнська.toDF("Wort8","Lange8")
      AffichageУкраїнська.printSchema()
      AffichageУкраїнська.createOrReplaceTempView("ord8")
      
      //DataFrame für Sprache
      import spark.implicits._
      
      val Sprache=Seq("Deutsh","English","Espanol","Francais","Italiano","Nederlands","Русский","Українська" ).toDF("sprache")
      Sprache.createOrReplaceTempView("Sprache")
      
      val startD=System.currentTimeMillis()
      println("------------------------------------------------------------Langste Wort in Deutsh---------------------------------------------------------------------------------")
      
      val ssh=spark.sql("SELECT sprache ,LangstesWort,Lange FROM Sprache S INNER JOIN ord O WHERE sprache='Deutsh' AND Lange=(SELECT MAX(Lange)FROM ord)  ")
      
      ssh.show()
    val endD=System.currentTimeMillis()
      //---------------------------English File--------------------------
       val startEn=System.currentTimeMillis()
      println("------------------------------------------------------------Langste Wort in English------------------------------------------------------------------------")
      val sshEnglish=spark.sql("SELECT sprache ,Wort2,Lange2 FROM Sprache S INNER JOIN ord2 O  WHERE sprache='English' AND Lange2=(SELECT MAX(Lange2)FROM ord2)").show()
      val endEn=System.currentTimeMillis()
      //---------------------------Espanol File--------------------------
       val startEs=System.currentTimeMillis()
       println("-----------------------------------------------------------Langste Wort in Espanol---------------------------------------------------------")
      val sshEspanol=spark.sql("SELECT sprache,Wort3,Lange3 FROM Sprache S INNER JOIN ord3 O WHERE sprache='Espanol' AND Lange3=(SELECT MAX(Lange3)FROM ord3)").show()
      val endEs=System.currentTimeMillis()
     //---------------------------Francais File--------------------------
       val startF=System.currentTimeMillis()
       println("-------------------------------------------------------------Langste Wort in Francais-----------------------------------------------------------------------------------")
      val sshFrancais=spark.sql("SELECT sprache,Wort4,Lange4 FROM Sprache S INNER JOIN ord4 O WHERE sprache='Francais' AND Lange4=(SELECT MAX(Lange4)FROM ord4)").show()
      val endF=System.currentTimeMillis()
    //---------------------------Italiano File--------------------------
       val startI=System.currentTimeMillis()
       println("-------------------------------------------------------------Langste Wort in Italiano-----------------------------------------------------------------------------------------------")
      val sshItaliano=spark.sql("SELECT sprache, Wort5,Lange5 FROM Sprache S INNER JOIN ord5 O WHERE sprache='Italiano' AND Lange5=(SELECT MAX(Lange5)FROM ord5)").show()
      val endI=System.currentTimeMillis()
       //---------------------------Nederlands File--------------------------
       val startN=System.currentTimeMillis()
       println("-------------------------------------------------------------Langste Wort in Nederlands-----------------------------------------------------------------------------------------------")
      val sshNederlands=spark.sql("SELECT sprache, Wort6,Lange6 FROM Sprache S INNER JOIN ord6 O WHERE sprache='Nederlands' AND Lange6=(SELECT MAX(Lange6)FROM ord6)").show()
     val endN=System.currentTimeMillis()
       //---------------------------Русский File--------------------------
       val startR=System.currentTimeMillis()
      
       println("-------------------------------------------------------------Langste Wort in Русский-----------------------------------------------------------------------------------------------")
      val sshРусский=spark.sql("SELECT sprache,Wort7,Lange7 FROM Sprache S INNER JOIN ord7 O WHERE sprache='Русский' AND Lange7=(SELECT MAX(Lange7)FROM ord7)").show()
      
      val endR=System.currentTimeMillis()
        //---------------------------Українська File--------------------------
       val startU=System.currentTimeMillis()
       println("-------------------------------------------------------------Langste Wort in Українська-----------------------------------------------------------------------------------------------")
      val sshУкраїнська=spark.sql("SELECT sprache, Wort8,Lange8 FROM Sprache S INNER JOIN ord8 O WHERE sprache='Українська' AND Lange8=(SELECT MAX(Lange8)FROM ord8)").show()
     val endU=System.currentTimeMillis()
      
      val end=System.currentTimeMillis()
      
      val actime=(end-start)/1000
      val actimeD=(endD-startD)/1000
      val actimeEn=(endEn-startEn)/1000
      val actimeEs=(endEs-startEs)/1000
      val actimeF=(endF-startF)/1000
      val actimeI=(endI-startI)/1000
      val actimeN=(endN-startN)/1000
      val actimeR=(endR-startR)/1000
      val actimeU=(endU-startU)/1000
      
      
      println("Die Laufzeit ist " +actime + "s " )
     //Die Laufzeit für jeder Sprache
      println("Die Laufzeit für die Deutsh ist " + actimeD + "s " )
      println("Die Laufzeit für die English ist " + actimeEn + "s " )
      println("Die Laufzeit für die Espanol ist " + actimeEs + "s " )
      println("Die Laufzeit für die Franzözisch ist " + actimeF + "s " )
      println("Die Laufzeit für die Italien ist " + actimeI + "s " )
      println("Die Laufzeit für die Nederland ist " + actimeN + "s " )
      println("Die Laufzeit für die Russich ist " + actimeR + "s " )
      println("Die Laufzeit für die Ukrayin ist " + actimeU + "s " )
     // Stop the session
    spark.stop()
    
    
  
   }
}