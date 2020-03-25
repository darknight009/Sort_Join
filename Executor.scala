import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.io.Source

class Executor{
  var set = 0
  var batchR =  new mutable.HashMap[String, mutable.ListBuffer[String]]()
  var antiR =  new mutable.HashMap[String, mutable.ListBuffer[String]]()
  var keyL = ""
  var maxKeyR = ""
  var rfin = 0
  var lfin = 0
  var rSchemcnt = 0
  var lSchemcnt = 0
  var i = 0
  var cnt = 0
  def execute(): Unit = {
    val key = "Name,Prefix"
    val key_s = key.split(",") //key on which join has to be performed
    val bw_inner = new BufferedWriter(new FileWriter(new File("C:/Workspace/office/Right/innerL.csv")))
    var fileL = "C:/Workspace/office/dummy1_sorted_9683.csv"
    var fileR = "C:/Workspace/office/dummyBIG_sorted_10000001.csv"
    if(fileL.split("_").last.split("\\.")(0).toInt < fileR.split("_").last.split("\\.")(0).toInt){
      val t = fileL
      fileL = fileR
      fileR = t
    }
    val srcL = Source.fromFile(fileL).getLines()
    Future{
      searchR(fileR, bw_inner)
    } //spawn right thread
    for(l<-srcL) {
      cnt+=1
      while (set == 1) this.synchronized(wait())
      val lSplit = l.split(",")
      lSchemcnt = lSplit.size
      keyL = lSplit(2)+","+lSplit(1)
      if (batchR.contains(keyL)) {
        utilMerge(l, batchR(keyL), bw_inner)
        antiR.remove(keyL)
      }
      else {
        if(keyCompare(keyL, maxKeyR)== 1 && rfin ==0){
          antiR.keys.foreach{ key=> utilMerge(("Null,"*lSchemcnt).dropRight(1), antiR(key), bw_inner) }
          batchR = new mutable.HashMap[String, mutable.ListBuffer[String]]()
          antiR = new mutable.HashMap[String, mutable.ListBuffer[String]]()
          set = 1
          this.synchronized(notifyAll())
          while (set == 1) this.synchronized(wait())
          if(batchR.contains(keyL)) {
            utilMerge(l, batchR(keyL), bw_inner)
            antiR.remove(keyL)
          }
          else bw_inner.write(l+(",Null"*rSchemcnt)+"\n")
        }
        else bw_inner.write(l+(",Null"*rSchemcnt)+"\n")
      }
    }
    antiR.keys.foreach{ key=> utilMerge(("Null,"*lSchemcnt).dropRight(1), antiR(key), bw_inner) }
    lfin = 1
    if(rfin ==0){
      set = 1
      this.synchronized(notifyAll())
      while (set == 1) this.synchronized(wait())
    }
    bw_inner.close()
    "Merge Success"
  }
  def searchR(fileR: String, bw_rightAnti: BufferedWriter): Unit ={
    val srcR = Source.fromFile(fileR).getLines()
    val quant = 100
    var temp = 0
    var prev = ""
    var keyR = ""
    for(r<-srcR) {
      while(set == 0) this.synchronized(wait())
      val rSplit = r.split(",")
      rSchemcnt = rSplit.size
      keyR = rSplit(2)+","+rSplit(1)
      i+=1
      if(temp >= quant && keyCompare(keyR, prev)!=0){
        maxKeyR = prev
        temp = 0
        set = 0
        this.synchronized(notifyAll())
      }
      if(keyCompare(keyL,keyR) != 1 && lfin ==0){
        while(set == 0) this.synchronized(wait())
        addKey(keyR, r, batchR);addKey(keyR, r, antiR)
        temp += 1
        prev = keyR
      }
      else bw_rightAnti.write(("Null,"*lSchemcnt) + r + "\n" )
    }
    maxKeyR = keyR
    rfin = 1
    set = 0
    this.synchronized(notifyAll())
  }
  def addKey(keyR: String, valR: String, hmap: mutable.HashMap[String, mutable.ListBuffer[String]]): Unit ={
    if(hmap.contains(keyR)) hmap(keyR) += valR
    else hmap(keyR) = new mutable.ListBuffer[String]()+=valR
  }
  def utilMerge(str: String, strings: mutable.ListBuffer[String], writer: BufferedWriter): Unit ={
    for(line<-strings){
      writer.write(str+","+line+"\n")
    }
  }
  def keyCompare(key1: String, key2: String): Int ={
    val k1 = key1.split(",")
    val k2 = key2.split(",")
    for(k <- 0 to k1.size-1){
      if(k1(k) > k2(k)) return 1
      else if(k1(k) < k2(k)) return -1
    }
    return 0
  }
}