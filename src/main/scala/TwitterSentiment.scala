import java.util.Properties
import java.util.HashMap
import org.apache.kafka.clients.producer._
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import scala.collection.convert.wrapAll._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

object TwitterSentiment {
  def main(args: Array[String]): Unit = {

    // Check if argument (topic name) passed to program.
    if (args.length < 1) {
      println("Correct usage: Program_Name inputTopic")
      System.exit(1)
    }
    val topic = args(0)

    // Set up logger for error messages.
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    // Function to get sentiment value from string of text.
    def getSentiment(text: String): Int = {
      // Set up sentiment extraction pipeline.
      val sentiment_props = new Properties()
      sentiment_props.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
      val pipeline: StanfordCoreNLP = new StanfordCoreNLP(sentiment_props)

      // Process text and get sentiments.
      val annotation: Annotation = pipeline.process(text)
      val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
      val sentiments = sentences.map(x => (x, x.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])))
        .map{ case(sentence, tree) => RNNCoreAnnotations.getPredictedClass(tree)}

      // If more than 0 sentiments returned, take the average.
      if(sentiments.length > 0) {
        sentiments.sum / sentiments.length
      }

      // Otherwise, the text was empty, could not be tokenized, or processed by the pipeline.
      // StanfordCoreNLP uses 5 values for sentiment: 0, 1, 2, 3, 4.
      // Return -1 as error.
      else {
        -1
      }
    }

    // Set up authorization for twitter stream.
    System.setProperty("twitter4j.oauth.consumerKey", "g3VELXWnRhfn2wgSOcUUySwih")
    System.setProperty("twitter4j.oauth.consumerSecret", "P0lnVRYyIdijW8xymfW7qbeSsk8Dwen0wix3iY5UjdEPW1hch5")
    System.setProperty("twitter4j.oauth.accessToken", "1284637642940911621-6pL3C3gBT4FQjWRoGiAtce99ndTS0Q")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "cyfl1R2HxfYzbag6RldSlhD6FF5jurkVrYFyoZ17nwQfO")
    val auth = Some(new OAuthAuthorization(new ConfigurationBuilder().build()))

    // Set topic for twitter stream and setup/create stream.
    val filters = Array("summer")
    val sparkConf = new SparkConf().setAppName("TwitterSearch")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, auth, filters)

    // For each RDD in the stream, extract sentiment from each tweet.
    try{
      stream.foreachRDD((rdd, time) => {

        // Check if RDD is empty.
        val count = rdd.count()
        if(count > 0) {

          // Set up Kafka producer.
          val props:Properties = new Properties()
          props.put("bootstrap.servers","localhost:9092")
          props.put("key.serializer",
            "org.apache.kafka.common.serialization.StringSerializer")
          props.put("value.serializer",
            "org.apache.kafka.common.serialization.StringSerializer")
          props.put("acks","all")
          val producer = new KafkaProducer[String, String](props)

          // Get each tweet.
          val topList = rdd.collect()
          topList.foreach(a => {
            // Get text from each tweet.
            val tweet = a.getText()

            // Check if tweet is empty.
            if (tweet.length > 0) {
              // Get sentiment from tweet.
              val sentiment = getSentiment(tweet)

              // Check if sentiment value is greater than -1.
              // If the getSentiment(tweet) returns -1, then the tweet could not be processed.
              if (sentiment > -1) {
                // Send sentiment to topic using Kafka producer.
                val record = new ProducerRecord[String, String](topic, sentiment.toString)
                producer.send(record)
              }
            }
          })
        }
      })
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    // Start stream and wait for termination signal from user.
    ssc.start()
    ssc.awaitTermination()
  }
}
