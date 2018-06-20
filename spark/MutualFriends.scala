//generate ((user1, user2), friendList) for pair counts
def pairs(str: Array[String]) = {
 val users = str(1).split(",")
val user=str(0)

val n = users.length
 
 for(i <- 0 until n) yield {
  val pair = if(user < users(i)) {
    (user,users(i))
  } else {
   (users(i),user)
  }
(pair, users)
 } 
 
   
}


val data = sc.textFile("soc-LiveJournal1Adj.txt")
val data1=data.map(x=>x.split("\t")).filter(li => (li.size == 2))
val pairCounts  = data1.flatMap(pairs).reduceByKey({ case (param1,param2) => (param1.intersect(param2)) })
val p1=pairCounts.map({case ((param1, param2),param3) => (param1+"\t"+param2+"\t"+param3.mkString(","))})
p1.saveAsTextFile("output")

var ans=""
val p2=p1.map(x=>x.split("\t")).filter(x => (x.size == 3)).filter(x=>(x(0)=="0"&&x(1)=="4")).flatMap(x=>x(2).split(",")).collect()
ans=ans+"0"+"\t"+"4"+"\t"+p2.mkString(",")+"\n"
val p3=p1.map(x=>x.split("\t")).filter(x => (x.size == 3)).filter(x=>x(0)=="20"&&x(1)=="22939").flatMap(x=>x(2).split(",")).collect()
ans=ans+"20"+"\t"+"22939"+"\t"+p3.mkString(",")+"\n"
val p4=p1.map(x=>x.split("\t")).filter(x => (x.size == 3)).filter(x=>x(0)=="1"&&x(1)=="29826").flatMap(x=>x(2).split(",")).collect()
ans=ans+"1"+"\t"+"29826"+"\t"+p4.mkString(",")+"\n"
val p5=p1.map(x=>x.split("\t")).filter(x => (x.size == 3)).filter(x=>x(0)=="19272"&&x(1)=="6222").flatMap(x=>x(2).split(",")).collect()
ans=ans+"6222"+"\t"+"19272"+"\t"+p5.mkString(",")+"\n"
val p6=p1.map(x=>x.split("\t")).filter(x => (x.size == 3)).filter(x=>x(0)=="28041"&&x(1)=="28056").flatMap(x=>x(2).split(",")).collect()
ans=ans+"28041"+"\t"+"28056"+"\t"+p6.mkString(",")+"\n"

val answer=sc.parallelize(Seq(ans))
answer.saveAsTextFile("output1")





