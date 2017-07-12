package org.dianahep.sparkrootapplications.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD

import scala.math._

//import org.apache.spark.implicits._

import org.dianahep.sparkroot._
import org.dianahep.sparkrootapplications.defs.aodpublic._

import org.dianahep.histogrammar._
import org.dianahep.histogrammar.ascii._

// for casting
case class Event(muons: Muons);
case class Muons(seq: Seq[RecoLeafCandidate], pt: Float)


object ReductionExampleApp {
  def main(args: Array[String]) {
    val inputPath = args(0)
    val muonMass = 0.105658369
    val spark = SparkSession.builder()
      .appName("AOD Public DS Example")
      .getOrCreate()

    // get the Dataset[Row] = Dataframe (from 2.0)
    val df = spark.sqlContext.read.option("tree", "Events").root(inputPath)

    // at this point at least...
    import spark.implicits._

    // select only AOD Collection of Muons. Now you have Dataset[Event].
    val dsMuons = df.select("recoMuons_muons__RECO_.recoMuons_muons__RECO_obj.reco::RecoCandidate.reco::LeafCandidate").toDF("muons").as[Event]
    // define the empty[zero] container of Histos


    val emptyDiCandidate = Label(
      "pt" -> Bin(200, 0, 200, {m: DiCandidate => m.pt}),
      "eta" -> Bin(48, -2.4, 2.4, {m: DiCandidate => m.eta}),
      "phi" -> Bin(63, -3.15, 3.15, {m: DiCandidate => m.phi}),
      "mass" -> Bin(200, 0, 200, {m: DiCandidate => m.mass})
    )
    def filterMuons(e: Event): Event = {
      var r = e.muons.seq(0).pt
      for( i <- 0 until e.muons.seq.length ) {
          var muon_p = e.muons.seq(i).pt
          if (r <= muon_p) {
            r = muon_p
          }
      }
      val mu = Muons(e.muons.seq , r)
      val ef = Event(mu)

      return ef
     }

      val dsfMuons = dsMuons.filter(filterMuons(_).muons.pt >= 10).flatMap({e: Event => for (i <- 0 until e.muons.seq.length) yield buildCandidate(e.muons.seq(i))})

    dsfMuons.show
    dsfMuons.write.format("parquet").save("file:/tmp/testReduced.parquet")
    val filled = dsfMuons.rdd.aggregate(emptyDiCandidate)(new Increment, new Combine);
    filled("pt").println;
    filled.toJsonFile("/tmp/testBundle.json")


    // stop the session/context
    spark.stop

  }
}
