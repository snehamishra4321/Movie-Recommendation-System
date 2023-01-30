import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
//package linalg contains everything relating to Vectors, Matrices, Tensors, etc
import breeze.linalg._
//package numerics contains several standard numerical functions as UFunc with MappingUFuncs
import breeze.numerics._
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import SparkContext._
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.math.sqrt


class MovieRecomm100kConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  //val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))

  //To run on DataSci Cluster: Added --num-executors and --executor-cores options
  //val numExecutors = opt[Int](descr = "number of executors", required = false, default = Some(1))
  //val executorCores = opt[Int](descr = "Executors cores", required = false, default = Some(1))
  verify()
}

object MovieRecommendation100k {
	val log = Logger.getLogger(getClass().getName())
  def main(argv: Array[String]) {
    val args = new MovieRecomm100kConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())

    val conf = new SparkConf().setAppName("MovieRecommendation100k")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    //Initialise with the unique movies
    val MOVIES = 1683

    //Map the Movies with the respective genres
    val movieId_to_Genre = sc.textFile(args.input() + "/u.item")
    .map(line => {
      val c = line.split('|')
      val movieId = c(0).toLong + 50000
      val title = c(1)
      val genres = c(2).split("\\|")
      movieId -> genres
      })
      .collectAsMap
    val broadcast_movieId2Genre = sc.broadcast(movieId_to_Genre)
    
    //Get UserID, MovieID, Ratings from ratings file
    val rateRDD = sc.textFile(args.input() + "/u.data")
    .map(line => {
      val c = line.split('\t')
      val userId = c(0).toLong
      val movieId = c(1).toLong + 10000 // to avoid mixups with userIds
      val rating = c(2).toDouble
      (userId, movieId, rating)
      })

  //  val movieId2Genre = sc.textFile(args.input() + "/u.item")
   // .map(line => {
    //  val cols = line.split('|')
     // print(cols)
    //  val movieId = cols(0).toLong + 50000
     // print(movieId)
      //val movieId = cols(0)
    //  val title = cols(1)
     // print(title)
      //(movieId, title)
     // val genres = cols(2).split("\\|")
     // movieId -> genres
     // })
      //val movieNames = movieId2Title.collectAsMap() 
    //  .collectAsMap
   // val movieId2Genre_b = sc.broadcast(movieId2Genre)
    //Map MovieID  with the Movie title
    val movieId_to_Title = sc.textFile(args.input() + "/u.item")
  .map(line => {
    val c = line.split('|')
    val movieId = c(0).toLong + 50000
    val movietitle = c(1)
    movieId -> movietitle
  })
  .collectAsMap
val broadcast_movieId2title = sc.broadcast(movieId_to_Title)


    //Creating a graph
    //Define the vertices
    val users: RDD[(VertexId, String)] = rateRDD.map(l =>  (l._1, "NA"))

    val movies: RDD[(VertexId, String)] = rateRDD.map(l => (l._2, broadcast_movieId2title.value(l._2)))

    val vertices = users.union(movies)

    // Create an RDD for edges
    val edgeRDD: RDD[Edge[Double]] = rateRDD.map(l => Edge(l._1, l._2, l._3))

    //Build the initial Graph
    val graph = Graph(vertices, edgeRDD)

    print("%d vertices of the graph, %d edges of the graph\n".format(graph.vertices.count, graph.edges.count))

    //Choose an initial source to start with
    val source = 21L
  
    //val p = 100
    //val q = 10  // number of movies to recommend

    //Finding the movies M rated by User N:
    val moviesRated_User = graph.edges.filter(e => e.srcId ==  source)
		.map(e => (e.dstId, e.attr))
		.collect
    .toMap
    
    val genresByUser = moviesRated_User
      .flatMap({case (id, rating) => movieId_to_Genre(id)})

    val preferredGenreByUser = genresByUser.groupBy(identity).mapValues(_.size).maxBy(_._2)._1
    print(preferredGenreByUser)

    println("Genres are %s, Preferred genre is %s".format(genresByUser.mkString("|"), preferredGenreByUser))

    val broadcastmovies_N = sc.broadcast(moviesRated_User)

    println("No. of movies rated by user: %d".format(moviesRated_User.size))

    //Finding all users K who rated movies M
    val ratedMoviesM = graph.aggregateMessages[List[Long]](
    l => {
    if (broadcastmovies_N.value.contains(l.dstId)) {
      // Send message to destination vertex 
      l.sendToDst(List(l.srcId))
    }
	},
	(a, b)  => (a++b)//reduce to a single list
)
	.flatMap(p => {
    val movieId = p._1
    val userId = p._2
    userId.map(u => (u, 1))        
  })
  .reduceByKey((a, b) => a + b)               
  .map(p => p._1)                             
  .collect
  .toSet

  val broadcastRatedMovies_k = sc.broadcast(ratedMoviesM)
  println("No. of unique users: %d".format(ratedMoviesM.size))
	

  //Finding p users with most similar taste as k
  def buildVector(elements: List[(Long, Double)]): DenseVector[Double] = {
    val v = DenseVector.zeros[Double](MOVIES)
    elements.foreach(e => {
      val d1 = (e._1 - 10001).toInt
      val d2 = e._2
      v(d1) = d2
      })
      v
     }
  //Using the cosine Similarity measure
  def cosineSimilarity(v1: DenseVector[Double], v2: DenseVector[Double]): Double = {
    (v1 dot v2) / (norm(v1) * norm(v2))
  }  

  //Build a user vector
  val userVRDD: RDD[(VertexId, DenseVector[Double])] = graph
  .aggregateMessages[List[(Long, Double)]](
  l => { // map function
  //users who rated movies M
    if (broadcastRatedMovies_k.value.contains(l.srcId)) {
  //send to users movieId and rating
        l.sendToSrc(List((l.dstId, l.attr)))
      }
    },
    (a, b) => (a ++ b)//reduce to a single list
  )                                       
  .mapValues(p => buildVector(p))


  val src = userVRDD.filter(l => l._1 == source)
  .map(_._2)
  .collect
  .toList(0)
  val broadcastSrc = sc.broadcast(src)

  val similarUser = userVRDD.filter(l => l._1 != source)
  .map(l => {
    val tUserId = l._1
    val tVec = l._2
    val cosim = cosineSimilarity(tVec, broadcastSrc.value)
    (tUserId, cosim)
  })
  val p = 100
  val similarSet = similarUser.takeOrdered(p)(Ordering[Double].reverse.on(r => r._2))
  .map(r => r._1)
  .toSet

  val broadcastSimilarUser = sc.broadcast(similarSet)
  println("Number of similar users: %d".format(similarSet.size))

//Step4: 
  val moviesUser= graph.aggregateMessages[List[Long]](
    l => {
      if (broadcastSimilarUser.value.contains(l.srcId) &&
         !broadcastmovies_N.value.contains(l.dstId)) {
        //send movieId to user
        l.sendToSrc(List(l.dstId))
      }
    },
    // reduce to a single set
    (a, b) => a ++ b
  )                                           
  .flatMap(r => {
    val userId = r._1
    val movieIds = r._2
    movieIds.map(movieId => (movieId, 1))     
  })
  .reduceByKey((a, b) => a + b)               
  .map(_._1)                              
  .collect
  .toSet

  val broadcastMovies = sc.broadcast(moviesUser)
  println("Number of candidate movies for recommendation: %d".format(moviesUser.size))

  //Recommend top 10 movies with the highest average rating

  val recommendedMovies: RDD[(VertexId, Double)] = graph
  .aggregateMessages[List[Double]](
    l => {
      //Movies rated by top n similar users
      if (broadcastMovies.value.contains(l.dstId)) {
        //send ratings to movies
        l.sendToDst(List(l.attr))
      }
    },
    //reduce ratings to single list
    (a, b) => (a ++ b)
  )
  .mapValues(ratings => ratings.foldLeft(0D)(_ + _) / ratings.size)
  
  //Recommend top 10 movies(t = 10)
  val t = 10
  val recommended = recommendedMovies.takeOrdered(t)(Ordering[Double].reverse.on(r => r._2))
  println("Number of recommended: %d".format(recommended.size))

  print("Top 10 recommended movies\n")
  recommended.foreach(l => {
    val movieId = l._1.toLong
    val score = l._2
    val title = movieId_to_Title(movieId)
    val genres = movieId_to_Genre(movieId).mkString(" ")

    print("(%.3f) [%d] %s\n".format(score, movieId - 10000, title, genres))
})

  val relevantMoviesCount = recommended
    .map(rec => broadcast_movieId2Genre.value(rec._1).contains("Action").compare(false))
    .sum
    .toDouble

  print(relevantMoviesCount)
  val percentage = (relevantMoviesCount/recommended.size)*100
  print("percentage relevance of user is (%0.3f)\n".format(percentage))

  }
  }

  
  
