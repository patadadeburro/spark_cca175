// Databricks notebook source
// MAGIC %md
// MAGIC # CCA175 Practice Exam
// MAGIC * Exam    : CCA175
// MAGIC * Title   : CCA Spark and Hadoop Developer Exam
// MAGIC * Vendor  : Cloudera
// MAGIC * Version : V12.35
// MAGIC 
// MAGIC IT Certification Guaranteed, The Easy Way!

// COMMAND ----------

val a = sc.parallelize( List( "dog", "salmon", "salmon", "rat", "elephant")  )
val b = a.keyBy( _.length ) 

val c = sc.parallelize( List( "dog","cat","gnu","salmon","rabbit","turkey","woif","bear","bee" ) ) 
val d = c.keyBy(_.length) 
//operation1



// COMMAND ----------

//a.take( 10 )
var f_isSalmon = ( animal: String ) => animal == "salmon"

//a.filter( f_isSalmon( _ )  ).collect
a.filter( f_isSalmon  ).collect


// COMMAND ----------

var f_isSalmonorDog = ( animal: String ) => animal == "salmon" || animal == "dog"
a.filter( f_isSalmonorDog  ).collect


// COMMAND ----------

a.filter( ( animal: String ) => animal == "salmon"  ).collect

// COMMAND ----------

d.collect

// COMMAND ----------

//d.filter( { case (n: Int, animal: String) => n == 3 } ).collect
//d.filter{ case (n: Int, animal: String) => n == 3 }.collect
//d.filter{ case (n, animal) => n == 3 }.collect
d.filter{ _._1 == 3 }.collect

// COMMAND ----------

d.filter{ case (n, animal) => n == 3 || n == 6 }.collect


// COMMAND ----------

var fa = ( n: Int ) => n == 3 || n == 6




// COMMAND ----------



// COMMAND ----------

val result = b
          .join( d )
          .filter{ case ( n, animal ) => n == 3 || n == 6 }
          .sortByKey( false )

result.collect

// COMMAND ----------

for ( i <- result.collect)
{
  println( i )
}

// COMMAND ----------


