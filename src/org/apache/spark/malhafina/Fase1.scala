package org.apache.spark.malhafina

import java.util.Properties;

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode

object Fase1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Malha Fina")

    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName("Malha Fina")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "123qwe!@#")

    executarFase1(spark, connectionProperties)
  }

  private def executarFase1(spark: SparkSession, connectionProperties: Properties): Unit = {
    val path = "/home/gustavo/Projetos/MalhaFina/datasets/"

    val esquemaContribuintes = new StructType()
      .add("cpf", LongType)
      .add("nome", StringType)
      .add("sexo", StringType)
      .add("data_nascimento", DateType)

    val esquemaRendimentosTributaveis = new StructType()
      .add("ano_calendario", IntegerType)
      .add("cnpj", LongType)
      .add("cpf", LongType)
      .add("rendimentos", DoubleType)
      .add("decimo_terceiro", DoubleType)

    val beneficiosDF = spark.read.option("header", true).schema(esquemaRendimentosTributaveis).csv(path + "pessoas-juridicas/beneficios.csv")

    val contribuintesDF = spark.read.option("header", true).schema(esquemaContribuintes).csv(path + "contribuintes.csv")
    val beneficiariosDF = spark.read.option("header", true).schema(esquemaRendimentosTributaveis).csv(path + "contribuintes/rendimentos_tribut_receb_pj.csv")

    // Save Data to JDBC
    writeJdbcContribuintes(spark, contribuintesDF, connectionProperties)
    writeJdbcBeneficios(spark, beneficiosDF, connectionProperties)
    writeJdbcRendimentosTributaveisPj(spark, beneficiariosDF, connectionProperties)

    beneficiosDF.createOrReplaceTempView("rendimentos_declarados_por_pjs")
    beneficiariosDF.createOrReplaceTempView("rendimentos_declarados_por_contribuintes")
    contribuintesDF.createOrReplaceTempView("contribuintes")

    val beneficiosAnoCalendarioDF = spark.sql("select cpf, sum(rendimentos) as total_rendimentos from rendimentos_declarados_por_pjs where ano_calendario = 2019 group by cpf")

    val beneficiariosAnoCalendarioDF = spark.sql("select cpf, sum(rendimentos) as total_rendimentos from rendimentos_declarados_por_contribuintes where ano_calendario = 2019 group by cpf")

    beneficiosAnoCalendarioDF.createOrReplaceTempView("total_rendimentos_declarados_por_pjs")
    beneficiariosAnoCalendarioDF.createOrReplaceTempView("total_rendimentos_declarados_por_contribuintes")

    val dadosNaoDeclaradosDF = spark.sql("SELECT pj.cpf, pj.total_rendimentos FROM total_rendimentos_declarados_por_pjs pj"
      + " WHERE pj.cpf NOT IN (SELECT cpf FROM total_rendimentos_declarados_por_contribuintes)")

    val dadosDivergentesDF = spark.sql("SELECT pj.cpf, pj.total_rendimentos total_rendimentos_pj, ct.total_rendimentos total_rendimentos_contrib"
      + " FROM total_rendimentos_declarados_por_pjs pj"
      + " JOIN total_rendimentos_declarados_por_contribuintes ct ON ct.cpf = pj.cpf WHERE pj.total_rendimentos <> ct.total_rendimentos")

    writeJdbcWithoutOptions(spark, dadosNaoDeclaradosDF, SaveMode.Overwrite, "malhafina.rendimentos_nao_declarados", connectionProperties)
    writeJdbcWithoutOptions(spark, dadosDivergentesDF, SaveMode.Overwrite, "malhafina.rendimentos_divergentes", connectionProperties)
  }

  private def executarFase2(spark: SparkSession, connectionProperties: Properties): Unit = {
    //writeJdbcWithoutOptions(spark, dataframe, SaveMode.Overwrite, "schema.tabela", connectionProperties)

  }

  private def writeJdbcContribuintes(spark: SparkSession, dataFrame: DataFrame, connectionProperties: Properties): Unit = {

    // Specifying create table column data types on write
    dataFrame.write
      .option("createTableColumnTypes", "data_nascimento DATE")
      .mode("overwrite")
      .jdbc("jdbc:mysql://localhost:3306", "malhafina.contribuintes", connectionProperties)

    // Specifying the custom data types of the read schema
    // The custom schema to use for READING data from JDBC connectors
    // connectionProperties.put("customSchema", "id BIGINT, cadastrado_em TIMESTAMP")

    // Loading data from a JDBC source
    // val jdbcDF = spark.read.jdbc("jdbc:mysql://localhost:3306", "malhafina.contribuintes", connectionProperties)

    // jdbcDF.show

    // Another way to save data to a JDBC source
    /*dataFrame.write.mode(SaveMode.Overwrite)
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306")
      .option("dbtable", "malhafina.contribuintes")
      .option("user", "root")
      .option("password", "123qwe!@#")
      .save()*/
  }

  /**
   * Dados enviados pelos Contribuintes
   */
  private def writeJdbcRendimentosTributaveisPj(spark: SparkSession, dataFrame: DataFrame, connectionProperties: Properties): Unit = {

    dataFrame.write
      .option("createTableColumnTypes", "cnpj VARCHAR(20), cpf VARCHAR(20), rendimentos DECIMAL(35,2)")
      .mode(SaveMode.Overwrite)
      .jdbc("jdbc:mysql://localhost:3306", "malhafina.rendimentos_tributaveis_recebidos_pj", connectionProperties)
  }

  /**
   * Dados enviados pelas Pessoas Jurídicas
   */
  private def writeJdbcBeneficios(spark: SparkSession, dataFrame: DataFrame, connectionProperties: Properties): Unit = {

    dataFrame.write
      .option("createTableColumnTypes", "cnpj VARCHAR(20), cpf VARCHAR(20), rendimentos DECIMAL(35,2)")
      .mode(SaveMode.Overwrite)
      .jdbc("jdbc:mysql://localhost:3306", "malhafina.beneficios", connectionProperties)
  }

  private def writeJdbc(spark: SparkSession, dataFrame: DataFrame, tableName: String, connectionProperties: Properties): Unit = {

    dataFrame.write
      .option("createTableColumnTypes", "cpf VARCHAR(20), rendimentos DECIMAL(35,2)")
      .mode(SaveMode.Overwrite)
      .jdbc("jdbc:mysql://localhost:3306", tableName, connectionProperties)
  }
  
  private def writeJdbcWithoutOptions(spark: SparkSession, dataFrame: DataFrame, saveMode: SaveMode, tableName: String, connectionProperties: Properties): Unit = {
    dataFrame.write.mode(saveMode).jdbc("jdbc:mysql://localhost:3306", tableName, connectionProperties)
  }

}