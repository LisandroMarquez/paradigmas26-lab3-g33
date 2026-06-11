import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    // Just to see better things in the terminal
    println("<======================= START =======================>")

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

    // Check if subscriptions were loaded correctly
    if (subscriptions.isEmpty) {
      println("Error: No valid subscriptions found")
      return
    }

    // Lazy evaluation starts here!
    // Convert subscriptions to RDD
    val subscriptionsRDD = sc.parallelize(subscriptions)

    // Statistics accumulators
    val feedsSuccess = sc.longAccumulator("feedsSuccess")
    val feedsFailed = sc.longAccumulator("feedsFailed")
    val postsDownloaded = sc.longAccumulator("postsDownloaded") 
    val postsFailed = sc.longAccumulator("postsFailed")
    val postsFiltered = sc.longAccumulator("postsFiltered")

    // Download + parse + filter in parallel
    val postsRDD = subscriptionsRDD.flatMap {
      subscription =>
        FileIO.downloadFeed(subscription.url) match {
          case Some(json) =>
            try {
              // Feed accessible
              feedsSuccess.add(1)

              JsonParser
                .parsePosts(json, subscription.name)
                .filter {
                  // Is this post valid?
                  post =>
                                    // Contar cada post antes de aplicar el filtro
                    postsDownloaded.add(1)
                    val valid = post.title.trim.nonEmpty && post.selftext.trim.nonEmpty
                    if (!valid) postsFiltered.add(1)

                    valid
                }
            } catch {
              case _: Exception =>
                // Post failed to parse
                postsFailed.add(1)
                println(s"Warning: Failed to parse posts from '${subscription.name}' (${subscription.url})")

                List.empty[Post]
            }
          case None =>
            // Feeds failed to download
            feedsFailed.add(1)
            println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")

            List.empty[Post]
        }
    }.cache()

    // Load dictionaries
    val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)

    // Share an immutable copy of dictionary to every worker (recommended by Spark documentation)
    val dictionaryBroadcast = sc.broadcast(dictionary)

    // Pipepline encadenado??? sobre el RDD[Post] 
    // Buscamos las entidades 
    val entitiesRDD = postsRDD.flatMap{ post => 
      val combinedText = post.title + " " + post.selftext

      Analyzer.detectEntities(combinedText, dictionaryBroadcast.value)
    }.cache()

    // Armamos las claves con los valores tanto para tipos como para entidades 
    val typePairsRDD = entitiesRDD.map(
      entity => (entity.entityType, 1)
    )
    val entityPairsRDD = entitiesRDD.map(
      entity => ((entity.entityType, entity.text), 1)
    )  
    
    // Sumamos los valores para agruparlos
    val typeCountsRDD = typePairsRDD.reduceByKey((value1, value2) => value1 + value2)
    val entityCountsRDD = entityPairsRDD.reduceByKey((value1, value2) => value1 + value2)

    // Lazy evaluation finish here!
    // Now we collect all data
    val t0 = System.currentTimeMillis()
    val totalEntities = entitiesRDD.count().toInt
    val t1 = System.currentTimeMillis()

    val filteredPosts = postsRDD.collect().toList
    val t2 = System.currentTimeMillis()

    val entityCounts = entityCountsRDD.collect().toMap
    val typeStats = typeCountsRDD.collect().toMap + ("total" -> totalEntities)
    val t3 = System.currentTimeMillis()

    // Release cached RDDs 
    entitiesRDD.unpersist()
    postsRDD.unpersist()

    // Steps Duration
    val timeEntities = (t1 - t0) / 1000.0
    val timePosts    = (t2 - t1) / 1000.0
    val timeCounts   = (t3 - t2) / 1000.0
    val timeTotal    = (t3 - t0) / 1000.0

    // Get posts stats
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
    println(Formatters.formatTimingStats(timeEntities, timePosts, timeCounts, timeTotal))
    println()

    // Check if we have any posts to process
    if (filteredPosts.isEmpty) {
      println("Error: No valid posts downloaded after filtering")
      return
    }

    println(Formatters.formatTypeStats(typeStats))
    println() 
    println(Formatters.formatEntityStats(entityCounts, cmdArgs.topK))
  }
}
