/*
This is the main class that the Jar is pointing to

Lifted and shifted from https://github.com/Anant/example-apache-spark-and-datastax-astra/tree/main/Job if you want to make changes

If updating the jar, you will need to update the dependency versions as such in build.sbt:

    ThisBuild / scalaVersion     := "2.12.14"

    and

    libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-core" % "3.1.3" % "provided",
        "org.apache.spark" %% "spark-sql" % "3.1.3" % "provided",
        "com.datastax.spark" %% "spark-cassandra-connector" % "3.1.0",
    )
*/

package sparkCassandra

import org.apache.spark.{SparkContext, SparkConf}
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.SparkSession
import com.datastax.spark.connector.cql.CassandraConnector

object POC {

    def main(args: Array[String]){

        val db_name = args(3)
        val keyspace_name = args(4)
        val table_name = args(5)
        
        val conf = new SparkConf().setAppName("POC")
        val sc = new SparkContext(conf)
        val spark = SparkSession
            .builder()
            .config("spark.cassandra.connection.config.cloud.path", s"secure-connect-$db_name.zip")
            .config("spark.cassandra.auth.username", args(1))
            .config("spark.cassandra.auth.password", args(2))
            .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
            .config("spark.sql.catalog.myCatalog", "com.datastax.spark.connector.datasource.CassandraCatalog")
            .getOrCreate()

        sc.addFile(args(0))
        
        spark.sql(s"SELECT * FROM myCatalog.$keyspace_name.$table_name").show()

        spark.stop()
    }
}
