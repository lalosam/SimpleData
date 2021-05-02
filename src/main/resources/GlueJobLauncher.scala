import com.amazonaws.services.glue.util.GlueArgParser
import org.apache.spark.SparkContext
import org.slf4j.{Logger, LoggerFactory}

object GlueJobLauncher {

  val log: Logger = LoggerFactory.getLogger("GlueJobLauncher")

  def main(args: Array[String]): Unit = {
    log.info(s"Launching a glue job with the follow parameters[${args.mkString("[", ", ", "]")}")
    val jobArgs = GlueArgParser.getResolvedOptions(args, Array("JOB_NAME", "JOB_TYPE"))
    val job = org.rojosam.etl.jobs.JobFactory(jobArgs("JOB_TYPE"), jobArgs("JOB_NAME"), args)
    job.execute()
    log.info(s"Job execution succeeded!!")
  }

}