import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    // Init Spark
    val spark = SparkSession.builder()
      .appName("RedditNER")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    // Parse command-line arguments
    val cmdArgs = CommandLineArgs.parse(args) match {
      case Some(parsed) => parsed
      case None => return // scopt prints error messages
    }

    // Load subscriptions
    val subscriptionOpts = FileIO.readSubscriptions(cmdArgs.subscriptionFile)

    // Filter out malformed subscriptions (None values)
    val subscriptions = subscriptionOpts.flatten

    // Check if subscriptions loaded correctly
    if (subscriptions.isEmpty) {
      println("Error: No valid subscriptions found")
      return
    }

    // Convert subscriptions to RDD
    val subscriptionsRDD = sc.parallelize(subscriptions)

    // Statistics accumulators
    val feedsSuccess = sc.longAccumulator("feedsSuccess")
    val feedsFailed = sc.longAccumulator("feedsFailed")
    val postsFailed = sc.longAccumulator("postsFailed")
    val postsFiltered = sc.longAccumulator("postsFiltered")

    // Download + parse + filter in parallel
    val postsRDD = subscriptionsRDD.flatMap {
      subscription =>
        FileIO.downloadFeed(subscription.url) match {
          case Some(json) =>
            try {
              feedsSuccess.add(1)

              JsonParser
                .parsePosts(json, subscription.name)
                .filter { post =>
                  val valid = post.title.trim.nonEmpty && post.selftext.trim.nonEmpty
                  if (!valid) postsFiltered.add(1)

                  valid
                }
            } catch {
              case _: Exception =>
                postsFailed.add(1)
                println(s"Warning: Failed to parse posts from '${subscription.name}' (${subscription.url})")

                List.empty[Post]
            }
          case None =>
            feedsFailed.add(1)
            println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")

            List.empty[Post]
        }
    }

    // Force Spark execution
    val filteredPosts = postsRDD.collect().toList
    val postsSuccess = filteredPosts.length

    // Calculate average characters in filtered posts
    val totalChars = filteredPosts.map(post => post.title.length + post.selftext.length).sum
    val avgChars = if (filteredPosts.nonEmpty) totalChars / filteredPosts.length else 0

    // Set statistics
    val stats = Map(
      "feedsSuccess" -> feedsSuccess.value.toInt,
      "feedsFailed" -> feedsFailed.value.toInt,
      "postsSuccess" -> postsSuccess,
      "postsFailed" -> postsFailed.value.toInt,
      "postsFiltered" -> postsFiltered.value.toInt,
      "avgChars" -> avgChars
    )

    // Print output
    println(Formatters.formatProcessingStats(stats))
    println()

    // Check if we have any posts to process
    if (filteredPosts.isEmpty) {
      println("Error: No valid posts downloaded after filtering")
      return
    }

    // Load dictionaries
    /*val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)

    // Detect entities in all posts (combine title and selftext)
    val allEntities = filteredPosts.flatMap {
      post =>
        val combinedText = post.title + " " + post.selftext
        Analyzer.detectEntities(combinedText, dictionary)
    }

    // Count entities
    val entityCounts = Analyzer.countEntities(allEntities)
    val typeStats = Analyzer.countByType(allEntities)

    println(Formatters.formatTypeStats(typeStats))
    println()
    println(Formatters.formatEntityStats(entityCounts, cmdArgs.topK))*/
  }
}
