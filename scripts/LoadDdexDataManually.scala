//> using scala "2.13"
//> using lib "com.google.cloud:google-cloud-bigquery:2.34.2"

import com.google.cloud.{NoCredentials, bigquery => bq}
import java.io.File

object LoadDdexDataManually {
  def main(args: Array[String]): Unit = {
    println("\n🎵 Loading DDEX XML into BigQuery Emulator")
    println("=" * 60)
    
    // Connect to emulator at localhost:9050
    val options = bq.BigQueryOptions.newBuilder()
      .setHost("http://localhost:9050")
      .setProjectId("test-project")
      .setCredentials(NoCredentials.getInstance())
      .build()
    
    val client = options.getService
    
    println("✅ Connected to BigQuery emulator")
    
    // List datasets
    val datasets = client.listDatasets("test-project")
    println("\n📊 Available datasets:")
    datasets.iterateAll().forEach { dataset =>
      println(s"   - ${dataset.getDatasetId.getDataset}")
    }
    
    // Check if music_metadata dataset exists
    val datasetId = client.getDataset("test-project", "music_metadata")
    if (datasetId != null) {
      println(s"\n✅ Dataset music_metadata exists")
      
      // List tables
      val tables = client.listTables("test-project", "music_metadata")
      println("\n📋 Tables in music_metadata:")
      tables.iterateAll().forEach { table =>
        println(s"   - ${table.getTableId.getTable}")
      }
    } else {
      println("\n❌ Dataset music_metadata does not exist")
    }
    
    println("\n" + "=" * 60)
  }
}
