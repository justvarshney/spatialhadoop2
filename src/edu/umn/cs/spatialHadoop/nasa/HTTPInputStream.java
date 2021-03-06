/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.nasa;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

/**
 * A wrapper around a stream obtained from {@link URL#openStream()} that
 * makes it {@link Seekable} and {@link PositionedReadable} to be used
 * with {@link FSDataInputStream}, hence {@link HTTPFileSystem}.
 * 
 * All methods are delegated to the underlying (wrapped) input stream.
 * Non-supported methods raise an exception if called.
 * 
 * 
 * @author Ahmed Eldawy
 *
 */
public class HTTPInputStream extends InputStream implements Seekable, PositionedReadable {
  /**The underlying stream obtained from {@link URL#openStream()}*/
  private InputStream in;
  
  /**Current position in the file*/
  private long pos;
  
  public HTTPInputStream(InputStream in) {
    this.in = in;
    this.pos = 0;
  }

  public int read() throws IOException {
    pos++;
    return in.read();
  }

  public int hashCode() {
    return in.hashCode();
  }

  public int read(byte[] b) throws IOException {
    int diff = in.read(b);
    pos += diff;
    return diff;
  }

  public boolean equals(Object obj) {
    return in.equals(obj);
  }

  public int read(byte[] b, int off, int len) throws IOException {
    int diff = in.read(b, off, len);
    pos += diff;
    return diff;
  }

  public long skip(long n) throws IOException {
    long skipped = in.skip(n);
    pos += skipped;
    return skipped;
  }

  public String toString() {
    return in.toString();
  }

  public int available() throws IOException {
    return in.available();
  }

  public void close() throws IOException {
    in.close();
  }

  public void mark(int readlimit) {
    in.mark(readlimit);
  }

  public void reset() throws IOException {
    in.reset();
  }

  public boolean markSupported() {
    return in.markSupported();
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
    seek(position);
    return read(buffer, offset, length);
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length)
      throws IOException {
    seek(position);
    read(buffer, offset, length);
  }

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    seek(position);
    read(buffer);
  }

  @Override
  public void seek(long newPos) throws IOException {
    if (newPos < pos)
      throw new RuntimeException("Unsupported feature");
    skip(newPos - pos);
  }

  @Override
  public long getPos() throws IOException {
    return pos;
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }
  
  

}
