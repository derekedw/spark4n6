/*
 * The software provided here is released by the Naval Postgraduate
 * School, an agency of the U.S. Department of Navy.  The software
 * bears no warranty, either expressed or implied. NPS does not assume
 * legal liability nor responsibility for a User's use of the software
 * or the results of such use.
 *
 * Please note that within the United States, copyright protection,
 * under Section 105 of the United States Code, Title 17, is not
 * available for any work of the United States Government and/or for
 * any works created by United States Government employees. User
 * acknowledges that this software contains work which was created by
 * NPS government employees and is therefore in the public domain and
 * not subject to copyright.
 *
 * Released into the public domain on December 17, 2010 by Bruce Allen.
 */

package edu.nps.jlibewf;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.Adler32;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;
//import org.apache.log4j.Logger;

/**
 * The <code>EWFSegmentFileReader</code> class provides accessors for reading EWF files formatted
 * in the .E01 format.
 */
public class EWFSegmentFileReader {

  /**
   * A generic format for formatting long to String.
   */
  protected static String longFormat = "%1$d (0x%1$08x)";
    private FileSystem fs = null;

    public EWFSegmentFileReader(FileSystem fs) {
        this.fs = fs;
    }

    /**
   * Sets the format for formatting long to string.
   * This format is used for preparing log reports contining long numbers.
   * The default value is <code>"%1$d (0x%1$08x)"</code>
   */
  public static void setLongFormat(String longFormat) {
    EWFSegmentFileReader.longFormat = longFormat;
  }

  /**
   * The start address of the first section in an EWF file
   */
  public static final int FILE_FIRST_SECTION_START_ADDRESS = 13;

  /**
   * The default chunk size of media chunks
   */
  public final int DEFAULT_CHUNK_SIZE = 64 * 512;

  // EWF file signature magic number and file offset
  private static final byte[] EWF_SIGNATURE
                       = {0x45, 0x56, 0x46, 0x09, 0x0d, 0x0a, (byte)0xff, 0x00};
  private static final long SIGNATURE_OFFSET = 0;

    // ************************************************************
    // segment file reading
    // ************************************************************

    private Path currentOpenedFile = null;
    private FSDataInputStream currentOpenedFileInputStream = null;
    /**
     * Opens a <code>FileChannel</code> for a file and verifies that it is of the correct file type.
     * If the previous open was the same file, then the already opened file channel is returned.
     * @param file The file to open the file channel for
     * @throws java.io.IOException If an I/O error occurs, which is possible if the file is invalid
     * or if the file's EWF signature is invalid
     * @return The file channel associated with the file
     */
    private FSDataInputStream openFileChannel(Path file) throws IOException {

        if (file == currentOpenedFile) {
            // the file channel is already active
            return currentOpenedFileInputStream;

        } else {
            // close any currently opened file
            closeFileChannel();

            // open file
            currentOpenedFile = file;
            currentOpenedFileInputStream = fs.open(file);

            // since the file is being opened, validate its signature
            if (!isValidE01Signature(currentOpenedFileInputStream)) {
                throw new IOException("Invalid E01 file signature");
            }

            return currentOpenedFileInputStream;
        }
    }

    /**
     * Closes the currently open file channel, if open, releasing resources.
     */
    protected void closeFileChannel() throws IOException {

        // close the file channel and the file input stream, and clear the current file references
        if (currentOpenedFileInputStream != null) {
            currentOpenedFileInputStream.close();
            currentOpenedFileInputStream = null;
        }
        currentOpenedFile = null;
    }

  /**
   * Indicates whether the bytes match the E01 file signature
   * @return true if the signature matches
   * @throws java.io.IOException if the signature cannot be read from the file channel
   */
  private boolean isValidE01Signature(FSDataInputStream fileChannel) throws IOException {
    // map and read the file's EWF signature into ewfSignatue
    byte[] ewfSignature = new byte[EWF_SIGNATURE.length];
    int status = fileChannel.read(SIGNATURE_OFFSET, ewfSignature, 0, ewfSignature.length);

    // validate the file's EWF signature
    for (int i=0; i < EWF_SIGNATURE.length; i++) {
      if (ewfSignature[i] != EWF_SIGNATURE[i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns the bytes from the specified EWF file and offset.
   * An IOException is thrown if the byte buffer cannot be read or completely filled.
   * @param file the file to read from
   * @param fileOffset the byte offset address in the file to read from
   * @param numBytes the number of bytes to read
   * @throws java.io.IOException If an I/O error occurs, which is possible if the requested read fails
   */
  public byte[] readRaw(Path file, long fileOffset, int numBytes) throws EWFIOException {

    try {
        FSDataInputStream fileChannel = openFileChannel(file);

      byte[] bytes = new byte[numBytes];
      int status = fileChannel.read(fileOffset, bytes, 0, numBytes);
      return bytes;

    } catch (IOException e) {
      // the read failed
      throw new EWFIOException("Unable to read from file", file, fileOffset);
    }
  }

  // object for calculating an Adler32 checksum
  private Adler32 adler32 = new Adler32();

  /**
   * Returns the bytes from the specified EWF file and offset.
   * The last four bytes are the Adler32 checksum, which is checked.
   * An IOException is thrown if the bytes cannot be read or if the Adler32 checksum fails.
   * @param file the file to read from
   * @param fileOffset the byte offset address in the file to read from
   * @param numBytes the number of bytes to read
   * @throws java.io.IOException If the bytes cannot be read or if the Adler32 checksum fails
   */
  public byte[] readAdler32(Path file, long fileOffset, int numBytes) throws EWFIOException {
    // verify proper input
    if (numBytes <= 4) {
      throw new EWFIOException("Invalid Adler32 read too short: " + numBytes + " bytes",
               file, fileOffset);
    }

    // read the raw bytes
    byte[] bytes = readRaw(file, fileOffset, numBytes);

    // calculate the Adler32 checksum
    adler32.reset();
    adler32.update(bytes, 0, numBytes - 4);

    // check the Adler32 checksum
    long expectedValue = bytesToUInt(bytes, bytes.length - 4);
    if (adler32.getValue() != expectedValue) {
      EWFFileReader.logger.error("Invalid Adler32 checksum: Calculated value "
            + String.format(longFormat, adler32.getValue())
            + " is not equal to expected value "
            + String.format(longFormat, expectedValue)
            + "\n" + makeByteLog("Bytes failing Adler32 checksum", bytes));
      throw new EWFIOException("Invalid Adler32 checksum on " + numBytes + " bytes",
               file, fileOffset);
    }

    // return the requested bytes
    return bytes;
  }

  // inflater for decompressing chunks
  private Inflater inflater = new Inflater();

  /**
   * Returns the decompessed bytes from the specified EWF file and offset.
   * The bytes must properly decompress.
   * An IOException is thrown if the bytes cannot be read or if the decompression fails.
   * @param file the file to read from
   * @param fileOffset the byte offset address in the file to read from
   * @param numBytes the number of bytes to read
   * @throws java.io.IOException If the bytes cannot be read or if the decompression fails
   */
  public byte[] readZlib(Path file, long fileOffset, int numBytes, int chunkSize)
                            throws EWFIOException {

    // read the raw bytes
    byte[] inBytes = readRaw(file, fileOffset, numBytes);

    // allocate temp space for the deflated bytes
    byte[] outBytes = new byte[chunkSize];

    // reset the inflater
    inflater.reset();

    // run the inflater
    inflater.setInput(inBytes, 0, inBytes.length);

    // get the output in outBytes
    int decompressedLength;
    try {
      decompressedLength = inflater.inflate(outBytes);
    } catch (DataFormatException e) {
      // the compressed data format is invalid
      throw new EWFIOException (e.getMessage(), file, fileOffset);
    }

    if (!inflater.finished()) {
      // fail on error
      throw new EWFIOException("Inflater not finished: "
        + inflater.getTotalIn() + " in, " + inflater.getTotalOut() + " out, "
        + inflater.getRemaining() + " remaining."
        + "  Needs input = " + inflater.needsInput(),
        file, fileOffset);
    }

    // return the deflated bytes
    if (decompressedLength == chunkSize) {
      // return the array
      return outBytes;
    } else {
      // pack outBytes from chunk size to actual decompressed size using ByteArrayOutputStream 
      ByteArrayOutputStream outStream = new ByteArrayOutputStream(decompressedLength);
      outStream.write(outBytes, 0, decompressedLength);
      return outStream.toByteArray();
    }
  }

  // ************************************************************
  // file data type parsers
  // ************************************************************
  // get long from 8-byte array sequence
  /**
   * Returns a <code>long</code> in little-endian form from the specified bytes.
   * @param bytes the bytes from which the <code>long</code> value is to be extracted
   * @param arrayOffset the offset in the byte array from which the value is to be extracted
   * @return the <code>long</code> value
   * @throws IndexOutOfBoundsException of there are insufficient bytes to perform the conversion
   */
  public static long bytesToLong(byte[] bytes, int arrayOffset) {
    // validate that the bytes are available
    if (bytes.length < arrayOffset + 8) {
      throw new IndexOutOfBoundsException();
    }

    // read the long
    long longValue =
           ((bytes[arrayOffset + 0] & 0xFFL) << 0)
         + ((bytes[arrayOffset + 1] & 0xFFL) << 8)
         + ((bytes[arrayOffset + 2] & 0xFFL) << 16)
         + ((bytes[arrayOffset + 3] & 0xFFL) << 24)
         + ((bytes[arrayOffset + 4] & 0xFFL) << 32)
         + ((bytes[arrayOffset + 5] & 0xFFL) << 40)
         + ((bytes[arrayOffset + 6] & 0xFFL) << 48)
         + (((long)bytes[arrayOffset + 7]) << 56);
    return longValue;
  }

  // get int from 4-byte array sequence
  /**
   * Reads four bytes in little-endian form from the array and returns it as an unsigned int
   * inside a <code>long</code>.
   * @param bytes the bytes from which the unsigned integer value is to be extracted
   * @param arrayOffset the offset in the byte array from which the value is to be extracted
   * @return the unsigned integer value in a <code>long</code>.
   * @throws IndexOutOfBoundsException of there are insufficient bytes to perform the conversion
   */
  public static long bytesToUInt(byte[] bytes, int arrayOffset) {
    // validate that the bytes are available
    if (bytes.length < arrayOffset + 4) {
      throw new IndexOutOfBoundsException();
    }

    // read the unsigned Integer
    long uintValue =
           ((bytes[arrayOffset + 0] & 0xFFL) << 0)
         + ((bytes[arrayOffset + 1] & 0xFFL) << 8)
         + ((bytes[arrayOffset + 2] & 0xFFL) << 16)
         + ((bytes[arrayOffset + 3] & 0xFFL) << 24);
    return uintValue;
  }

  // get String from byte range or /0 terminated portion
  /**
   * Returns a <code>String</code> from the specified bytes.
   * @param bytes the bytes from which the <code>int</code> value is to be extracted
   * @param arrayOffset the offset in the byte array from which the <code>String</code>
   * is to be extracted
   * @param length the maximum length to parse if the /0 terminator is not present
   * @return the <code>String</code> value residing at the requested bytes
   * @throws IndexOutOfBoundsException of there are insufficient bytes to perform the conversion
   */
  public static String bytesToString(byte[] bytes, int arrayOffset, int length) {
    // validate that the bytes are available
    if (bytes.length < arrayOffset + length) {
      throw new IndexOutOfBoundsException();
    }

    // read the bytes, stopping at end or at /0 within length
    int i;
    for (i=0; i<length; i++) {
      if (bytes[arrayOffset + i] == 0) {
        break;
      }
    }

    // return String from range or from /0 terminated portion
    return new String(bytes, 0, i);
  }

  /**
   * Returns a printable report of the specified bytes.
   * @param bytes the bytes to format
   * @return the formatted byte log
   */
  protected static final String makeByteLog(String text, byte[] bytes) {
    StringBuffer buffer = new StringBuffer();

    // generate text
    buffer.append("\n");
    buffer.append(text);
    buffer.append(", size: " + bytes.length);
    buffer.append("\n");

    int i;

    // add bytes as text
    for (i=0; i<bytes.length; i++) {
      byte b = bytes[i];
      if (b > 31 && b < 127) {
        buffer.append((char)b);	// printable
      } else {
        buffer.append(".");	// not printable
      }
    }
    buffer.append("\n");

    // add bytes as hex
    for (i=0; i<bytes.length; i++) {
      buffer.append(String.format("%1$02x", bytes[i]));
      buffer.append(" ");
    }
    buffer.append("\n");

    // return the byte log as a string
    return buffer.toString();
  }
}

