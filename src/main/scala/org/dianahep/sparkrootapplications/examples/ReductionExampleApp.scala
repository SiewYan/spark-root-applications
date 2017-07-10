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
case class Event(muons: Seq[RecoLeafCandidate]);

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
/*
    case class MuonVars(var pt: Double, var eta: Double, var phi: Double, var m: Double)
    case class AllVars(var muonvars: MuonVars = null)
    val allvars = AllVars(null)
*/
    val emptyDiCandidate = Label(
      "pt" -> Bin(200, 0, 200, {m: DiCandidate => m.pt}),
      "eta" -> Bin(48, -2.4, 2.4, {m: DiCandidate => m.eta}),
      "phi" -> Bin(63, -3.15, 3.15, {m: DiCandidate => m.phi}),
      "mass" -> Bin(200, 0, 200, {m: DiCandidate => m.mass})
    )
    val dsfMuons = dsMuons.filter(_.muons.pt >= 10)
      .map({e: Event => for (i <- 0 until e.muons.length) yield buildDiCandidate(e.muons(i),0)})
/*	
//    var vmuon = LorentzVector(0,0,0,0)
      if (!dsfMuons.isEmpty) {
        allvars.muonvars = MuonVars(0.0, 0.0, 0.0, 0.0)
        val m = dsfMuons.maxBy(_.pt)
        allvars.muonvars.pt = m.pt
        allvars.muonvars.eta = m.eta
        allvars.muonvars.phi = m.phi
        allvars.muonvars.m = muonMass
//        vmuon = LorentzVector(m.pt, m.eta, m.phi, muonMass)
      }
*/
    dsfMuons.show
    dsfMuons.write.format("parquet").save("file:/tmp/testReduced.parquet")
    val filled = dsfMuons.rdd.aggregate(emptyDiCandidate)(new Increment, new Combine);
    filled("pt").println;
    filled.toJsonFile("/tmp/testBundle.json")

    // stop the session/context
    spark.stop
  }
/*
  def filterMuon(m: baconhep.TMuon) = {
      m.pt >= 10 && Math.abs(m.eta) < 2.4 && passMuonLooseSel(m)
  }
  def passMuonLooseSel(muon: baconhep.TMuon) = {
      // PF-isolation with Delta-beta correction                                                                                                                                                     
      val iso = muon.chHadIso + Math.max(muon.neuHadIso + muon.gammaIso - 0.5*(muon.puIso), 0)
      ((muon.pogIDBits & kPOGLooseMuon) != 0) && (iso < 0.12*(muon.pt))
    }

    // TLorentzVector
  trait LorentzVectorMethods {
    def pt: Double
    def phi: Double
    def eta: Double
    def m: Double

    def px = Math.abs(pt)*Math.cos(phi)
    def py = Math.abs(pt)*Math.sin(phi)
    def pz = Math.abs(pt)*Math.sinh(eta)
    def e = {
      if(m >= 0) Math.sqrt(px*px+py*py+pz*pz+m*m)
      else Math.sqrt(Math.max(px*px+py*py+pz*pz-m*m,0))
    }

    def +(that: LorentzVectorMethods) = {
      val px = this.px + that.px
      val py = this.py + that.py
      val pz = this.pz + that.pz
      val e = this.e + that.e
      val (pt, phi, pta, m) = LorentzVectorMethods.setptphietam(px, py, pz, e)
      LorentzVector(pt, phi, eta, m)
    }

    def DeltaR(that: LorentzVectorMethods) = {
      val deta = this.eta - that.eta
      val dphi = if(this.phi - that.phi >= Math.PI) this.phi - that.phi - Math.PI
                 else if(this.phi - that.phi < -Math.PI) this.phi - that.phi + Math.PI
                 else this.phi - that.phi
      Math.sqrt(deta*deta + dphi*dphi)
    }

  }
  object LorentzVectorMethods {
    def setptphietam(px: Double, py: Double, pz: Double, e: Double) = {
      val pt = Math.sqrt(px*px + py*py)
      val p = Math.sqrt(px*px + py*py + pz*pz)
      val m = Math.sqrt(e*e - px*px - py*py - pz*pz)
      val eta = 0.5*Math.log((p + pz)/(p - pz))
      val phi = Math.atan2(py, px)
      (pt, phi, eta, m)
  	  }
  }

  case class LorentzVector(pt: Double, phi: Double, eta: Double, m: Double) extends LorentzVectorMethods with Ordered[LorentzVector] {
    // return 0 if the same, negative if this < that, positive if this > that
    def compare (that: LorentzVector) = {
      if (this.pt == that.pt)
        0
      else if (this.pt > that.pt)
        1
      else
       -1
    }
  }

*/

}
