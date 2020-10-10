package org.apache.spark.malhafina

import java.util.Properties;

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder

import org.apache.spark.sql.Row

import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode

object Fase2 {
  def main(args: Array[String]): Unit = {
    var appName = "Malha Fina - Fase 2"

    val conf = new SparkConf().setMaster("local").setAppName(appName)

    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName(appName)
      .getOrCreate()

    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "123qwe!@#")

    executar(spark, connectionProperties)
  }

  private def executar(spark: SparkSession, connectionProperties: Properties): Unit = {
    val path = "/home/gustavo/Projetos/MalhaFina/datasets/"

    val esquemaContas = new StructType()
      .add("ano_calendario", IntegerType)
      .add("cpf", StringType)
      .add("agencia_sem_dv", StringType)
      .add("conta_num", StringType)
      .add("conta_dv", StringType)
      .add("situacao_anterior", DoubleType)
      .add("situacao_vigente", DoubleType)

    // RDD a partir do arquivo enviado pelos Bancos
    val rddContas = spark.sparkContext.textFile(path + "instituicoes-financeiras/contas.csv")
    // RDD a partir do arquivo enviado pelos Contribuintes
    val rddBensDireitosContribuintes = spark.sparkContext.textFile(path + "contribuintes/contas.csv")

    // DataFrame a partir do arquivo enviado pelos Bancos
    val contasDF = spark.read.option("header", true).schema(esquemaContas).csv(path + "instituicoes-financeiras/contas.csv")
    // DataFrame a partir do arquivo enviado pelos Contribuintes
    val contasContribuintesDF = spark.read.option("header", true).schema(esquemaContas).csv(path + "contribuintes/contas.csv")

    // Salvar dados usando JDBC
    writeJdbc(spark, contasDF, "contas", connectionProperties)
    writeJdbc(spark, contasContribuintesDF, "contas_contribuintes", connectionProperties)

    import spark.implicits._

    case class Conta(
      ano_calendario:    Int,
      cpf:               String,
      agencia_sem_dv:    String,
      conta_num:         String,
      conta_dv:          String,
      situacao_anterior: Double,
      situacao_vigente:  Double)

    contasDF.filter($"ano_calendario" === 2019).select($"cpf", $"situacao_vigente", $"situacao_anterior").createOrReplaceTempView("saldo_contas")

    val saldos = spark.sql("select cpf, situacao_vigente - situacao_anterior as saldo from saldo_contas")
      .createOrReplaceTempView("total_saldo_contas")
    val totalSaldos = spark.sql("select cpf, sum(saldo) total_saldo from total_saldo_contas group by cpf")

    //totalSaldos.show

    contasContribuintesDF.filter($"ano_calendario" === 2019).select($"cpf", $"situacao_vigente", $"situacao_anterior").createOrReplaceTempView("saldo_contas_contribuintes")

    val saldosContribuintes = spark.sql("select cpf, situacao_vigente - situacao_anterior as saldo from saldo_contas_contribuintes")
      .createOrReplaceTempView("total_saldo_contas_contribuintes")
    val totalSaldosContribuintes = spark.sql("select cpf, sum(saldo) total_saldo from total_saldo_contas_contribuintes group by cpf")

    //totalSaldosContribuintes.show

    totalSaldos.createOrReplaceTempView("total_saldos")
    totalSaldosContribuintes.createOrReplaceTempView("total_saldos_contribuintes")

    val saldosNaoDeclaradosDF = spark.sql("SELECT if.cpf, if.total_saldo FROM total_saldos if"
      + " WHERE if.cpf NOT IN (SELECT cpf FROM total_saldos_contribuintes)")

    val saldosDivergentesDF = spark.sql("SELECT if.cpf, if.total_saldo total_saldo_contrib, ct.total_saldo total_saldo_inst_financ"
      + " FROM total_saldos if"
      + " JOIN total_saldos_contribuintes ct ON ct.cpf = if.cpf WHERE if.total_saldo <> ct.total_saldo")

    writeJdbcWithoutOptions(spark, saldosNaoDeclaradosDF, SaveMode.Overwrite, "saldos_nao_declarados", connectionProperties)
    writeJdbcWithoutOptions(spark, saldosDivergentesDF, SaveMode.Overwrite, "saldos_divergentes", connectionProperties)
  }

  private def writeJdbc(spark: SparkSession, dataFrame: DataFrame, tableName: String, connectionProperties: Properties): Unit = {

    val columnTypes = "cpf VARCHAR(20), situacao_anterior DECIMAL(35,2), situacao_vigente DECIMAL(35,2)"

    dataFrame.write
      .option("createTableColumnTypes", columnTypes)
      .mode(SaveMode.Overwrite)
      .jdbc("jdbc:mysql://localhost:3306/malhafina", tableName, connectionProperties)
  }

  private def writeJdbcWithoutOptions(spark: SparkSession, dataFrame: DataFrame, saveMode: SaveMode, tableName: String, connectionProperties: Properties): Unit = {
    dataFrame.write.mode(saveMode).jdbc("jdbc:mysql://localhost:3306/malhafina", tableName, connectionProperties)
  }

}