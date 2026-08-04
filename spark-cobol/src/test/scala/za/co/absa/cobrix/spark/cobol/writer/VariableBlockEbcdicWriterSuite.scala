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

package za.co.absa.cobrix.spark.cobol.writer

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SaveMode
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.cobrix.spark.cobol.source.base.SparkTestBase
import za.co.absa.cobrix.spark.cobol.source.fixtures.{BinaryFileFixture, TextComparisonFixture}

/**
  * Tests for writing VB (Variable Block) COBOL files.
  *
  * A VB file groups one or more variable-length records into "blocks". On disk the layout is:
  *
  *   BDW (4 bytes) | RDW (4 bytes) + payload | RDW (4 bytes) + payload | ...   <- block 1
  *   BDW (4 bytes) | RDW (4 bytes) + payload | ...                            <- block 2
  *
  * where:
  *   - RDW (Record Descriptor Word) prefixes each record and encodes the payload length.
  *   - BDW (Block Descriptor Word) prefixes each block and encodes the total length of everything
  *     that follows it in the block (the sum of all RDW+payload bytes), EXCLUDING the BDW's own 4 bytes.
  */
class VariableBlockEbcdicWriterSuite extends AnyWordSpec with SparkTestBase with BinaryFileFixture with TextComparisonFixture {

  import spark.implicits._

  // A = PIC X(1), B = PIC X(5) => payload is 6 bytes per record.
  // With a 4-byte RDW prefix, each record occupies 10 bytes on disk.
  private val copybookContents =
    """       01  RECORD.
           05  A       PIC X(1).
           05  B       PIC X(5).
    """

  // Reusable per-record byte fragments (RDW + EBCDIC payload) for the sample rows below.
  // RDW little-endian, payload length 6 => 0x06 0x00 0x00 0x00
  private val rdwLe = Array[Byte](0x06, 0x00, 0x00, 0x00)
  // RDW big-endian, payload length 6 => 0x00 0x06 0x00 0x00
  private val rdwBe = Array[Byte](0x00, 0x06, 0x00, 0x00)

  private val payloadA = Array[Byte](0xC1.toByte, 0xC6.toByte, 0x89.toByte, 0x99.toByte, 0xA2.toByte, 0xA3.toByte) // "A","First"
  private val payloadB = Array[Byte](0xC2.toByte, 0xE2.toByte, 0x83.toByte, 0x95.toByte, 0x84.toByte, 0x40.toByte) // "B","Scnd_"
  private val payloadC = Array[Byte](0xC3.toByte, 0xD3.toByte, 0x81.toByte, 0xA2.toByte, 0xA3.toByte, 0x40.toByte) // "C","Last_"
  private val payloadD = Array[Byte](0xC4.toByte, 0xC6.toByte, 0x96.toByte, 0x99.toByte, 0xA3.toByte, 0x88.toByte) // "D","Forth"

  private val fourRows = List(("A", "First"), ("B", "Scnd"), ("C", "Last"), ("D", "Forth"))
  private val threeRows = List(("A", "First"), ("B", "Scnd"), ("C", "Last"))
  private val twoRows = List(("A", "First"), ("B", "Scnd"))

  // BDW encoders. blockLength is the sum of the (RDW + payload) bytes of every record in the block.
  private def bdwLe(blockLength: Int): Array[Byte] =
    Array[Byte](0x00, 0x00, (blockLength & 0xFF).toByte, ((blockLength >> 8) & 0xFF).toByte)

  private def bdwBe(blockLength: Int): Array[Byte] =
    Array[Byte](((blockLength >> 8) & 0xFF).toByte, (blockLength & 0xFF).toByte, 0x00, 0x00)

  "cobol VB writer" should {
    "write a VB file with records_per_block = 2, little-endian BDW and RDW" in {
      withTempDirectory("cobol_vb_writer") { tempDir =>
        val df = fourRows.toDF("A", "B")
        val path = new Path(tempDir, "vb1")

        df.coalesce(1)
          .orderBy("A")
          .write
          .format("cobol")
          .mode(SaveMode.Overwrite)
          .option("copybook_contents", copybookContents)
          .option("record_format", "VB")
          .option("records_per_block", "2")
          .save(path.toString)

        // Two blocks of two records each. Each block payload = 2 * (4 + 6) = 20 bytes.
        val expected =
          bdwLe(20) ++ rdwLe ++ payloadA ++ rdwLe ++ payloadB ++
          bdwLe(20) ++ rdwLe ++ payloadC ++ rdwLe ++ payloadD

        assertArraysEqual(readSinglePartFile(path), expected)
      }
    }

    "write a VB file with records_per_block = 2, big-endian BDW and RDW" in {
      withTempDirectory("cobol_vb_writer") { tempDir =>
        val df = fourRows.toDF("A", "B")
        val path = new Path(tempDir, "vb2")

        df.coalesce(1)
          .orderBy("A")
          .write
          .format("cobol")
          .mode(SaveMode.Overwrite)
          .option("copybook_contents", copybookContents)
          .option("record_format", "VB")
          .option("records_per_block", "2")
          .option("is_bdw_big_endian", "true")
          .option("is_rdw_big_endian", "true")
          .save(path.toString)

        val expected =
          bdwBe(20) ++ rdwBe ++ payloadA ++ rdwBe ++ payloadB ++
          bdwBe(20) ++ rdwBe ++ payloadC ++ rdwBe ++ payloadD

        assertArraysEqual(readSinglePartFile(path), expected)
      }
    }

    "write a VB file with records_per_block = 2 and an uneven number of records" in {
      withTempDirectory("cobol_vb_writer") { tempDir =>
        val df = threeRows.toDF("A", "B")
        val path = new Path(tempDir, "vb3")

        df.coalesce(1)
          .orderBy("A")
          .write
          .format("cobol")
          .mode(SaveMode.Overwrite)
          .option("copybook_contents", copybookContents)
          .option("record_format", "VB")
          .option("records_per_block", "2")
          .save(path.toString)

        // First block has 2 records (blockLength 20), last block has the leftover single record (blockLength 10).
        val expected =
          bdwLe(20) ++ rdwLe ++ payloadA ++ rdwLe ++ payloadB ++
          bdwLe(10) ++ rdwLe ++ payloadC

        assertArraysEqual(readSinglePartFile(path), expected)
      }
    }

    "write a VB file with block_length cap producing an uneven split" in {
      withTempDirectory("cobol_vb_writer") { tempDir =>
        val df = threeRows.toDF("A", "B")
        val path = new Path(tempDir, "vb4")

        // Each record is 10 bytes. With a 25-byte cap, 2 records (20 bytes) fit, a 3rd (30) would overflow.
        df.coalesce(1)
          .orderBy("A")
          .write
          .format("cobol")
          .mode(SaveMode.Overwrite)
          .option("copybook_contents", copybookContents)
          .option("record_format", "VB")
          .option("block_length", "25")
          .save(path.toString)

        val expected =
          bdwLe(20) ++ rdwLe ++ payloadA ++ rdwLe ++ payloadB ++
          bdwLe(10) ++ rdwLe ++ payloadC

        assertArraysEqual(readSinglePartFile(path), expected)
      }
    }

    "write a VB file where a single record exceeds block_length (record is never split)" in {
      withTempDirectory("cobol_vb_writer") { tempDir =>
        val df = twoRows.toDF("A", "B")
        val path = new Path(tempDir, "vb5")

        // block_length smaller than a single 10-byte record => each record gets its own block.
        df.coalesce(1)
          .orderBy("A")
          .write
          .format("cobol")
          .mode(SaveMode.Overwrite)
          .option("copybook_contents", copybookContents)
          .option("record_format", "VB")
          .option("block_length", "5")
          .save(path.toString)

        val expected =
          bdwLe(10) ++ rdwLe ++ payloadA ++
          bdwLe(10) ++ rdwLe ++ payloadB

        assertArraysEqual(readSinglePartFile(path), expected)
      }
    }

    "fail fast when writing VB without a blocking option" in {
      withTempDirectory("cobol_vb_writer") { tempDir =>
        val df = twoRows.toDF("A", "B")
        val path = new Path(tempDir, "vb6")

        val exception = intercept[IllegalArgumentException] {
          df.coalesce(1)
            .write
            .format("cobol")
            .mode(SaveMode.Overwrite)
            .option("copybook_contents", copybookContents)
            .option("record_format", "VB")
            .save(path.toString)
        }

        assert(exception.getMessage.contains("records_per_block"))
        assert(exception.getMessage.contains("block_length"))
      }
    }

    "fail when writing VB with both blocking options" in {
      withTempDirectory("cobol_vb_writer") { tempDir =>
        val df = twoRows.toDF("A", "B")
        val path = new Path(tempDir, "vb7")

        val exception = intercept[IllegalArgumentException] {
          df.coalesce(1)
            .write
            .format("cobol")
            .mode(SaveMode.Overwrite)
            .option("copybook_contents", copybookContents)
            .option("record_format", "VB")
            .option("records_per_block", "2")
            .option("block_length", "25")
            .save(path.toString)
        }

        assert(exception.getMessage.contains("cannot be used together"))
      }
    }

    "round-trip: write a VB file and read it back" in {
      withTempDirectory("cobol_vb_writer") { tempDir =>
        val df = fourRows.toDF("A", "B")
        val path = new Path(tempDir, "vb8")

        df.coalesce(1)
          .orderBy("A")
          .write
          .format("cobol")
          .mode(SaveMode.Overwrite)
          .option("copybook_contents", copybookContents)
          .option("record_format", "VB")
          .option("records_per_block", "2")
          .save(path.toString)

        val readBack = spark.read
          .format("cobol")
          .option("copybook_contents", copybookContents)
          .option("record_format", "VB")
          .load(path.toString)
          .orderBy("A")
          .collect()
          .map(r => (r.getString(0), r.getString(1)))
          .toList

        assert(readBack == List(("A", "First"), ("B", "Scnd"), ("C", "Last"), ("D", "Forth")))
      }
    }
  }

  private def readSinglePartFile(path: Path): Array[Byte] = {
    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    assert(fs.exists(path), "Output directory should exist")
    val files = fs.listStatus(path).filter(_.getPath.getName.startsWith("part-"))
    assert(files.nonEmpty, "Output directory should contain part files")

    val partFile = files.head.getPath
    val data = fs.open(partFile)
    val bytes = new Array[Byte](files.head.getLen.toInt)
    data.readFully(bytes)
    data.close()
    bytes
  }

  private def assertArraysEqual(actual: Array[Byte], expected: Array[Byte]): Assertion = {
    if (!actual.sameElements(expected)) {
      val actualHex = actual.map(b => f"0x$b%02X").mkString(", ")
      val expectedHex = expected.map(b => f"0x$b%02X").mkString(", ")
      fail(s"Actual:   $actualHex\nExpected: $expectedHex")
    } else {
      succeed
    }
  }
}
