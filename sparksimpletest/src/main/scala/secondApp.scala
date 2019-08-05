import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}

object secondApp {

  def main(args: Array[String]): Unit = {
    // SparkSession은 스팍 버전 2 부터 sparkContext와 sqlContext 합친거로 사용할 수 있음.
    val spark = SparkSession.builder().appName("githubPushApp")
      .master("spark://master1.0phsxb3scd5ezokhxq2qtfyv5e.syx.internal.cloudapp.net:7077")
      .getOrCreate()

    val sc = spark.sparkContext

    val homedir = "/home/minji/github/"
    val inputPath = homedir + "*.json"

    // dir 해당 json 파일 읽어오기
    val ghLog = spark.read.json(inputPath)

    // filter로 PushEvent 열을 매핑하여 가져오기
    val pushes = ghLog.filter("type = 'PushEvent'")

//    pushes.printSchema()
//    println()
//    println("all events: " + ghLog.count()) // 전체 카운드
//    println("only pushes: " + pushes.count()) // push 로그 카운트
//    pushes.show()

    val actorLogin = pushes.groupBy("actor.login").count() //json column에 {}.{} 식으로 해야 함.

    //actorLogin.show()

    val actorLoginorder = actorLogin.orderBy(actorLogin("count").desc)
    actorLoginorder.show()

    //분석 제외 대상이 되 employee
    import scala.io.Source.fromFile
    val emppath = "/home/minji/inaction/first-edition/ch03/ghEmployees.txt"

    // employees Set() 객체 생성. ++ 메서드는 Set에 요소를 추가한다.
    // 각 라인마다 line 객체안에다가 넣어준다. - Scala.Collection.Immutable.Set()
    val employees = Set() ++ (
      for {
        line <- fromFile(emppath).getLines
      } yield line.trim //trim - 공백 문자 제거.
    )

    import spark.implicits._

    //broadcast - 공유변수
    //공유변수는 각 노드에 정확히 한 번만 전송 하도록 하고 메모리에 자동 캐시되므로 바로 접근할 수 있다. P2P 프로토콜을 사용한다.
    val bcEmployees = sc.broadcast(employees)

    // String을 받고 Boolean으로 retrun한당.
    val isEmp = user => bcEmployees.value.contains(user)
    val sqFunc = spark.udf.register("SetContainUdf", isEmp)
    // isEmp에 Login 컬럼이 들어가 있는지 filter.
    val filtered = actorLogin.filter(sqFunc($"login"))
    filtered.show()
    filtered.write.format(args(3)).save(args(2))

  }
}
