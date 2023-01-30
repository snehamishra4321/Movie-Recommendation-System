import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import breeze.linalg._
import breeze.numerics._
//import org.apache.spark.ml.feature.{ StringIndexer, StringIndexerModel}
//import org.apache.spark.ml.feature.VectorAssembler
//import org.apache.spark.ml.linalg.DenseVector
//import org.apache.spark.mllib.linalg
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import SparkContext._
import org.apache.spark.SparkConf
import org.rogach.scallop._
//import scala.io.Source
//import java.nio.charset.CodingErrorAction
//import scala.io.Codec
import scala.math.sqrt
//import java.io.Serializable

class MovieRecomm10MConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  //val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))

  // To run on DataSci Cluster:Added --num-executors and --executor-cores options
 // val numExecutors = opt[Int](descr = "number of executors", required = false, default = Some(1))
 // val executorCores = opt[Int](descr = "Executors cores", required = false, default = Some(1))
  verify()
}

object MovieRecommendation10M {
	val log = Logger.getLogger(getClass().getName())
  def main(argv: Array[String]) {
    val args = new MovieRecomm10MConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())

    val conf = new SparkConf().setAppName("MovieRecommendation10M")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    //load movielens data
    //val MOVIELENS_DIR = "/path/to/MovieLens-100k"

    //change the code here and try to calculate the size of each
    val NUM_USERS = 65133
    val NUM_MOVIES = 71567
    val NUM_RATINGS = 1000000

    val ratingsRDD = sc.textFile(args.input() + "/ratings.dat")
    .map(line => {
      val cols = line.split("::")
      val userId = cols(0).toLong
      val movieId = cols(1).toLong + 50000 // to avoid mixups with userId
      //val movieId = cols(1)
      val rating = cols(2).toDouble
      (userId, movieId, rating)
      })

    val movieId2Title = sc.textFile(args.input() + "/movies.dat")
    .map(line => {
      val cols = line.split("::")
     // print(cols)
      val movieId = cols(0).toLong + 50000
     // print(movieId)
      //val movieId = cols(0)
      val title = cols(1)
     // print(title)
      //(movieId, title)
      movieId -> title
      })
      //val movieNames = movieId2Title.collectAsMap() 
      .collectAsMap
    val movieId2Title_b = sc.broadcast(movieId2Title)

    //Creating a graph
    //Define the vertices
    val users: RDD[(VertexId, String)] = ratingsRDD.map(l =>  (l._1, "NA"))

    val movies: RDD[(VertexId, String)] = ratingsRDD.map(l => (l._2, movieId2Title_b.value(l._2)))

    val vertices = users.union(movies)

    // Create an RDD for edges
    val relationships: RDD[Edge[Double]] = ratingsRDD.map(l => Edge(l._1, l._2, l._3))

    // Build the initial Graph
    val graph = Graph(vertices, relationships)

    print("%d vertices, %d edges\n".format(graph.vertices.count, graph.edges.count))
   // assert(graph.edges.count == NUM_RATINGS)

    val source = 21L
    val p = 100 // number of users to look at
    val q = 10  // number of movies to recommend

    //Finding the movies M rated by User N:
    val moviesRatedbyUser = graph.edges.filter(e => e.srcId ==  source)
		.map(e => (e.dstId, e.attr))
		.collect
    .toMap

    val moviesUser_N = sc.broadcast(moviesRatedbyUser)

    println("No. of movies rated by user: %d".format(moviesRatedbyUser.size))

    //Finding all users K who rated movies M
    val usersRatedMovieM = graph.aggregateMessages[List[Long]](
    triplet => { // Map Function
    if (moviesUser_N.value.contains(triplet.dstId)) {
      // Send message to destination vertex containing counter and age
      triplet.sendToDst(List(triplet.srcId))
    }
	},
	(a, b)  => (a++b)
)
	.flatMap(p => {
    val movieId = p._1
    val userIds = p._2
    userIds.map(userId => (userId, 1))              // (userId, 1)
  })
  .reduceByKey((a, b) => a + b)                     // (userId, n)
  .map(p => p._1)                               // unique List(userId)
  .collect
  .toSet

  val usersRatedMovies_k = sc.broadcast(usersRatedMovieM)
  println("No. of unique users: %d".format(usersRatedMovieM.size))
	

  //Finding p users with most similar taste as k
  def buildVector(elements: List[(Long, Double)]): DenseVector[Double] = {
    val v = DenseVector.zeros[Double](NUM_MOVIES)
    elements.foreach(e => {
      val vecIdx = (e._1 - 50001).toInt
      val vecVal = e._2
      v(vecIdx) = vecVal
      })
      v
     }
  
  def cosineSimilarity(v1: DenseVector[Double], v2: DenseVector[Double]): Double = {
    (v1 dot v2) / (norm(v1) * norm(v2))
  }  


  val userVectorsRDD: RDD[(VertexId, DenseVector[Double])] = graph
  .aggregateMessages[List[(Long, Double)]](
  triplet => { // map function
  // consider only users that rated movies M
    if (usersRatedMovies_k.value.contains(triplet.srcId)) {
  // send to each user the target movieId and rating
        triplet.sendToSrc(List((triplet.dstId, triplet.attr)))
      }
    },
    // reduce to a single list
    (a, b) => (a ++ b)
  )                                       
  .mapValues(elements => buildVector(elements))


  val sourceVec = userVectorsRDD.filter(rec => rec._1 == source)
  .map(_._2)
  .collect
  .toList(0)
  val sourceVec_b = sc.broadcast(sourceVec)

  val similarUsersRDD = userVectorsRDD.filter(rec => rec._1 != source)
  .map(rec => {
    val targetUserId = rec._1
    val targetVec = rec._2
    val cosim = cosineSimilarity(targetVec, sourceVec_b.value)
    (targetUserId, cosim)
  })

  val similarUserSet = similarUsersRDD.takeOrdered(p)(Ordering[Double].reverse.on(rec => rec._2))
  .map(rec => rec._1)
  .toSet
  val similarUserSet_b = sc.broadcast(similarUserSet)
  println("# of similar users: %d".format(similarUserSet.size))


  val candidateMovies = graph.aggregateMessages[List[Long]](
    triplet => { // map function
      // only consider users in the set p of similar users,
      // exclude movies rated by user u
      if (similarUserSet_b.value.contains(triplet.srcId) &&
         !moviesUser_N.value.contains(triplet.dstId)) {
        // send message [movieId] back to user
        triplet.sendToSrc(List(triplet.dstId))
      }
    },
    // reduce function
    (a, b) => a ++ b
  )                                             // (userId, List(movieId))
  .flatMap(rec => {
    val userId = rec._1
    val movieIds = rec._2
    movieIds.map(movieId => (movieId, 1))       // (movieId, 1)
  })
  .reduceByKey((a, b) => a + b)                 // (movieId, count)
  .map(_._1)                                    // (movieId)
  .collect
  .toSet

  val candidateMovies_b = sc.broadcast(candidateMovies)
  println("# of candidate movies for recommendation: %d".format(candidateMovies.size))

  //Recommend top q movies with highest average rating

  val recommendedMoviesRDD: RDD[(VertexId, Double)] = graph
  .aggregateMessages[List[Double]](
    triplet => { // map function
      // limit search to movies rated by top p similar users
      if (candidateMovies_b.value.contains(triplet.dstId)) {
        // send ratings to movie nodes
        triplet.sendToDst(List(triplet.attr))
      }
    },
    // reduce ratings to single list per movie
    (a, b) => (a ++ b)
  )
  .mapValues(ratings => ratings.foldLeft(0D)(_ + _) / ratings.size)

  val recommendedMovies = recommendedMoviesRDD.takeOrdered(q)(Ordering[Double].reverse.on(rec => rec._2))
  println("#-recommended: %d".format(recommendedMovies.size))

  print("---- recommended movies ----\n")
  recommendedMovies.foreach(rec => {
    val movieId = rec._1.toLong
    val score = rec._2
    val title = movieId2Title(movieId)
    print("(%.3f) [%d] %s\n".format(score, movieId - 50000, title))
})

  }
  }

  
  
