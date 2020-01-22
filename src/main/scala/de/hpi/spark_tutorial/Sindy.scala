package de.hpi.spark_tutorial

import java.lang

import org.apache.spark.sql.{Dataset, Row, SparkSession, functions}

object Sindy {

  def collectSetLabelToColumnName(label: String) : String = {
    label.replace("collect_set(", "").replace(")", "")
  }

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    val tables = inputs.map(input =>
                            spark.read
                              .option("inferSchema", "true")
                              .option("header", "true")
                              .option("sep", ";")
                              .csv(input)

    )

  //    create sets of attributes
    val attribute_sets =  tables.flatMap(table =>
                                         table.columns.map(attribute =>
                                         table.select(functions.collect_set(table(attribute)))
                                         ))

    println("Got " + attribute_sets.size + " columns to compare to each other.")
  // compare if the count of the attribute set is same as the outcome of a inner join between the other attributes
  // if datatypes of two columns are different, evaluate first to false to avoid exception
    val find_uid = attribute_sets.map(attribute_set
    => (attribute_set.columns(0),attribute_sets.map(other_attribute_sets
      =>(other_attribute_sets.columns(0), (attribute_set.dtypes(0)._2
          ==
          other_attribute_sets.dtypes(0)._2)
          &&
          attribute_set.join(other_attribute_sets,
          other_attribute_sets(other_attribute_sets.columns(0)) === attribute_set(attribute_set.columns(0)))
            .count()
          ==
          attribute_set.count()
      ))))

    find_uid.map(
      attribute_result =>
        (
          attribute_result._1,
          attribute_result._2.filter(
            compared_column => compared_column._2 && compared_column._1 != attribute_result._1
          )
        )
    ).filter(dependencyResult => dependencyResult._2.nonEmpty)
      .map(dependencyResult => collectSetLabelToColumnName(dependencyResult._1)  + " < " + dependencyResult._2.map(t => t._1).map(collectSetLabelToColumnName).mkString(", "))
      .foreach(s => println(s));



  }
}
