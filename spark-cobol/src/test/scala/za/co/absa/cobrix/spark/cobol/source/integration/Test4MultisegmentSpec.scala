/*
 * Copyright 2018 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.cobrix.spark.cobol.source.integration

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.scalatest.FunSuite
import za.co.absa.cobrix.spark.cobol.source.base.SparkTestBase
import za.co.absa.cobrix.spark.cobol.utils.FileUtils

import scala.collection.JavaConverters._

//noinspection NameBooleanParameters
class Test4MultisegmentSpec extends FunSuite with SparkTestBase {

  private val exampleName = "Test4(multisegment,ascii)"
  private val inputCopybookPath = "file://../data/test4_copybook.cob"
  private val inputDataPath = "../data/test4_data"

  test(s"Integration test on $exampleName - ascii segment ids, ascii") {

    val expectedSchemaPath = "../data/test4_expected/test4_schema.json"
    val actualSchemaPath = "../data/test4_expected/test4_schema_actual.json"
    val expectedResultsPath = "../data/test4_expected/test4.txt"
    val actualResultsPath = "../data/test4_expected/test4_actual.txt"

    val df = spark
      .read
      .format("cobol")
      .option("copybook", inputCopybookPath)
      .option("encoding", "ascii")
      .option("record_format", "V")
      .option("segment_field", "SEGMENT_ID")
      .option("segment_id_level0", "C")
      .option("segment_id_level1", "P")
      .option("generate_record_id", "true")
      .option("schema_retention_policy", "collapse_root")
      .option("segment_id_prefix", "A")
      .load(inputDataPath)

    val expectedSchema = Files.readAllLines(Paths.get(expectedSchemaPath), StandardCharsets.ISO_8859_1).toArray.mkString("\n")
    val actualSchema = df.schema.json

    if (actualSchema != expectedSchema) {
      FileUtils.writeStringToFile(actualSchema, actualSchemaPath)
      assert(false, s"The actual schema doesn't match what is expected for $exampleName example. Please compare contents of $expectedSchemaPath to " +
        s"$actualSchemaPath for details.")
    }

    val actualDf = df
      .orderBy("File_Id", "Record_Id")
      .toJSON
      .take(60)

    FileUtils.writeStringsToFile(actualDf, actualResultsPath)

    // toList is used to convert the Java list to Scala list. If it is skipped the resulting type will be Array[AnyRef] instead of Array[String]
    val expected = Files.readAllLines(Paths.get(expectedResultsPath), StandardCharsets.ISO_8859_1).asScala.toArray
    val actual = Files.readAllLines(Paths.get(actualResultsPath), StandardCharsets.ISO_8859_1).asScala.toArray

    if (!actual.sameElements(expected)) {
      assert(false, s"The actual data doesn't match what is expected for $exampleName example. Please compare contents of $expectedResultsPath to " +
        s"$actualResultsPath for details.")
    }
    Files.delete(Paths.get(actualResultsPath))

  }

  test(s"Integration test on $exampleName - ascii segment ids, ascii with ISO-8859-1 charset") {

    val expectedResultsPath = "../data/test4_expected/test4a.txt"
    val actualResultsPath = "../data/test4_expected/test4a_actual.txt"

    val df = spark
      .read
      .format("cobol")
      .option("copybook", inputCopybookPath)
      .option("encoding", "ascii")
      .option("ascii_charset", "ISO-8859-1")
      .option("record_format", "V")
      .option("improved_null_detection", "false")
      .option("segment_field", "SEGMENT_ID")
      .option("segment_id_level0", "C")
      .option("segment_id_level1", "P")
      .option("generate_record_id", "true")
      .option("schema_retention_policy", "collapse_root")
      .option("segment_id_prefix", "A")
      .load("../data/test4a_data")

    val actualDf = df
      .orderBy("File_Id", "Record_Id")
      .toJSON
      .take(5)

    FileUtils.writeStringsToFile(actualDf, actualResultsPath)

    // toList is used to convert the Java list to Scala list. If it is skipped the resulting type will be Array[AnyRef] instead of Array[String]
    val expected = Files.readAllLines(Paths.get(expectedResultsPath), StandardCharsets.ISO_8859_1).asScala.toArray
    val actual = Files.readAllLines(Paths.get(actualResultsPath), StandardCharsets.ISO_8859_1).asScala.toArray

    if (!actual.sameElements(expected)) {
      assert(false, s"The actual data doesn't match what is expected for $exampleName example. Please compare contents of $expectedResultsPath to " +
        s"$actualResultsPath for details.")
    }
    Files.delete(Paths.get(actualResultsPath))

  }

}
