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

package za.co.absa.cobrix.cobol.reader.iterator

import za.co.absa.cobrix.cobol.internal.Logging
import za.co.absa.cobrix.cobol.parser.Copybook
import za.co.absa.cobrix.cobol.parser.ast.Primitive
import za.co.absa.cobrix.cobol.parser.headerparsers.RecordHeaderParser
import za.co.absa.cobrix.cobol.reader.parameters.ReaderParameters
import za.co.absa.cobrix.cobol.reader.extractors.raw.RawRecordExtractor
import za.co.absa.cobrix.cobol.reader.stream.SimpleStream
import za.co.absa.cobrix.cobol.reader.validator.ReaderParametersValidator

/**
  * This iterator is used to read fixed length and variable length records from a binary COBOL-generated file.
  * It returns a pair of (segment_id, raw_data_byte_array) without actually decoding the data.
  *
  * @param cobolSchema        A parsed copybook.
  * @param dataStream         A source of bytes for sequential reading and parsing. It should implement [[SimpleStream]] interface.
  * @param readerProperties   Additional properties for customizing the reader.
  * @param recordHeaderParser A record parser for multisegment files
  * @param recordExtractor    A record extractor that can be used instead of the record header parser.
  * @param startRecordId      A starting record id value for this particular file/stream `dataStream`
  * @param startingFileOffset An offset of the file where parsing should be started
  */
class VRLRecordReader(cobolSchema: Copybook,
                      dataStream: SimpleStream,
                      readerProperties: ReaderParameters,
                      recordHeaderParser: RecordHeaderParser,
                      recordExtractor: Option[RawRecordExtractor],
                      startRecordId: Long,
                      startingFileOffset: Long) extends Iterator[(String, Array[Byte])] with Logging {

  type RawRecord = (String, Array[Byte])

  private var cachedValue: Option[RawRecord] = _
  private var byteIndex = startingFileOffset
  private var recordIndex = startRecordId - 1

  final private val copyBookRecordSize = cobolSchema.getRecordSize
  final private val (recordLengthField, lengthFieldExpr) = ReaderParametersValidator.getEitherFieldAndExpression(readerProperties.lengthFieldExpression, readerProperties.lengthFieldMap, cobolSchema)
  final private val lengthField = recordLengthField.map(_.field)
  final private val lengthMap = recordLengthField.map(_.valueMap).getOrElse(Map.empty)
  final private val isLengthMapEmpty = lengthMap.isEmpty
  final private val segmentIdField = ReaderParametersValidator.getSegmentIdField(readerProperties.multisegment, cobolSchema)
  final private val recordLengthAdjustment = readerProperties.rdwAdjustment
  final private val useRdw = lengthField.isEmpty && lengthFieldExpr.isEmpty
  final private val minimumRecordLength = readerProperties.minimumRecordLength
  final private val maximumRecordLength = readerProperties.maximumRecordLength

  fetchNext()

  override def hasNext: Boolean = cachedValue.nonEmpty

  @throws(classOf[IllegalStateException])
  @throws(classOf[NoSuchElementException])
  override def next(): RawRecord = {
    cachedValue match {
      case None => throw new NoSuchElementException
      case Some(value) =>
        fetchNext()
        recordIndex = recordIndex + 1
        value
    }
  }

  @throws(classOf[IllegalStateException])
  private def fetchNext(): Unit = {
    var recordFetched = false
    while (!recordFetched) {
      val binaryData = recordExtractor match {
        case Some(extractor) =>
          if (extractor.hasNext) {
            Option(extractor.next())
          } else {
            None
          }
        case None =>
          if (useRdw) {
            fetchRecordUsingRdwHeaders()
          } else if (lengthField.nonEmpty) {
            fetchRecordUsingRecordLengthField()
          } else {
            fetchRecordUsingRecordLengthFieldExpression(lengthFieldExpr.get)
          }
      }

      binaryData match {
        case None =>
          cachedValue = None
          recordFetched = true
        case Some(data) if data.length < minimumRecordLength || data.length > maximumRecordLength =>
          recordFetched = false
        case Some(data) =>
          val segmentId = getSegmentId(data)
          val segmentIdStr = segmentId.getOrElse("")

          cachedValue = Some(segmentIdStr, data)
          recordFetched = true
      }
    }
  }

  def getRecordIndex: Long = recordIndex

  private def fetchRecordUsingRecordLengthField(): Option[Array[Byte]] = {
    if (lengthField.isEmpty) {
      throw new IllegalStateException(s"For variable length reader either RDW record headers or record length field should be provided.")
    }

    val lengthFieldBlock = lengthField.get.binaryProperties.offset + lengthField.get.binaryProperties.actualSize

    val binaryDataStart = dataStream.next(readerProperties.startOffset + lengthFieldBlock)

    byteIndex += readerProperties.startOffset + lengthFieldBlock

    if (binaryDataStart.length < readerProperties.startOffset + lengthFieldBlock) {
      return None
    }

    val recordLength = lengthField match {
      case Some(lengthAST) => getRecordLengthFromField(lengthAST, binaryDataStart)
      case None            => copyBookRecordSize
    }

    val restOfDataLength = recordLength - lengthFieldBlock + readerProperties.endOffset

    byteIndex += restOfDataLength

    if (restOfDataLength > 0) {
      Some(binaryDataStart ++ dataStream.next(restOfDataLength))
    } else {
      Some(binaryDataStart)
    }
  }

  final private def getRecordLengthFromField(lengthAST: Primitive, binaryDataStart: Array[Byte]): Int = {
    val length = if (isLengthMapEmpty) {
      cobolSchema.extractPrimitiveField(lengthAST, binaryDataStart, readerProperties.startOffset) match {
        case i: Int    => i
        case l: Long   => l.toInt
        case s: String => s.toInt
        case null      => throw new IllegalStateException(s"Null encountered as a record length field (offset: $byteIndex, raw value: ${getBytesAsHexString(binaryDataStart)}).")
        case _         => throw new IllegalStateException(s"Record length value of the field ${lengthAST.name} must be an integral type.")
      }
    } else {
      cobolSchema.extractPrimitiveField(lengthAST, binaryDataStart, readerProperties.startOffset) match {
        case i: Int    => getRecordLengthFromMapping(i.toString)
        case l: Long   => getRecordLengthFromMapping(l.toString)
        case s: String => getRecordLengthFromMapping(s)
        case null      => throw new IllegalStateException(s"Null encountered as a record length field (offset: $byteIndex, raw value: ${getBytesAsHexString(binaryDataStart)}).")
        case _         =>    throw new IllegalStateException(s"Record length value of the field ${lengthAST.name} must be an integral type.")
      }
    }
    length + recordLengthAdjustment
  }

  final private def getRecordLengthFromMapping(v: String): Int = {
    lengthMap.get(v) match {
      case Some(len) => len
      case None => throw new IllegalStateException(s"Record length value '$v' is not mapped to a record length.")
    }
  }

  final private def getBytesAsHexString(bytes: Array[Byte]): String = {
    bytes.map("%02X" format _).mkString
  }

  private def fetchRecordUsingRecordLengthFieldExpression(expr: RecordLengthExpression): Option[Array[Byte]] = {
    val lengthFieldBlock = expr.requiredBytesToread
    val evaluator = expr.evaluator

    val binaryDataStart = dataStream.next(readerProperties.startOffset + lengthFieldBlock)

    byteIndex += readerProperties.startOffset + lengthFieldBlock

    if (binaryDataStart.length < readerProperties.startOffset + lengthFieldBlock) {
      return None
    }

    expr.fields.foreach{
      case (name, field) =>
        val obj = cobolSchema.extractPrimitiveField(field, binaryDataStart, readerProperties.startOffset)
        try {
           obj match {
            case i: Int    => evaluator.setValue(name, i)
            case l: Long   => evaluator.setValue(name, l.toInt)
            case s: String => evaluator.setValue(name, s.toInt)
            case _         => throw new IllegalStateException(s"Record length value of the field ${field.name} must be an integral type.")
          }
        } catch {
          case ex: NumberFormatException =>
            throw new IllegalStateException(s"Encountered an invalid value of the record length field. Cannot parse '$obj' as an integer in: ${field.name} = '$obj'.", ex)
        }
    }

    val recordLength = evaluator.eval()

    val restOfDataLength = recordLength - lengthFieldBlock + readerProperties.endOffset

    byteIndex += restOfDataLength

    if (restOfDataLength > 0) {
      Some(binaryDataStart ++ dataStream.next(restOfDataLength))
    } else {
      Some(binaryDataStart)
    }
  }

  private def fetchRecordUsingRdwHeaders(): Option[Array[Byte]] = {
    val rdwHeaderBlock = recordHeaderParser.getHeaderLength

    var isValidRecord = false
    var isEndOfFile = false
    var headerBytes = Array[Byte]()
    var recordBytes = Array[Byte]()

    while (!isValidRecord && !isEndOfFile) {
      headerBytes = dataStream.next(rdwHeaderBlock)

      val recordMetadata = recordHeaderParser.getRecordMetadata(headerBytes, dataStream.offset, dataStream.size, dataStream.totalSize, recordIndex)
      val recordLength = recordMetadata.recordLength

      byteIndex += headerBytes.length

      isValidRecord = recordMetadata.isValid && recordLength >= minimumRecordLength && recordLength <= maximumRecordLength

      if (recordLength > 0) {
        recordBytes = dataStream.next(recordLength)
        byteIndex += recordBytes.length
      } else {
        isEndOfFile = true
      }
    }

    if (!isEndOfFile) {
      if (recordHeaderParser.isHeaderDefinedInCopybook) {
        Some(headerBytes ++ recordBytes)
      } else {
        Some(recordBytes)
      }
    } else {
      None
    }
  }

  private def getSegmentId(data: Array[Byte]): Option[String] = {
    segmentIdField.map(field => {
      val fieldValue = cobolSchema.extractPrimitiveField(field, data, readerProperties.startOffset)
      if (fieldValue == null) {
        logger.error(s"An unexpected null encountered for segment id at $byteIndex")
        ""
      } else {
        fieldValue.toString.trim
      }
    })
  }

}
