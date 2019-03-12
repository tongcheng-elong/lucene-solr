/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.store;

import java.io.IOException;
import java.nio.file.Path;

public class TestMockDirectoryWrapper extends BaseDirectoryTestCase {
  
  @Override
  protected Directory getDirectory(Path path) throws IOException {
    final MockDirectoryWrapper dir;
    if (random().nextBoolean()) {
      dir = newMockDirectory();
    } else {
      dir = newMockFSDirectory(path);
    }
    dir.setEnableVirusScanner(false); // test manipulates filesystem directly
    return dir;
  }
  
  // we wrap the directory in slow stuff, so only run nightly
  @Override @Nightly
  public void testThreadSafety() throws Exception {
    super.testThreadSafety();
  }
  
  public void testDiskFull() throws IOException {
    // test writeBytes
    MockDirectoryWrapper dir = newMockDirectory();
    dir.setMaxSizeInBytes(3);
    final byte[] bytes = new byte[] { 1, 2};
    IndexOutput out = dir.createOutput("foo", IOContext.DEFAULT);
    out.writeBytes(bytes, bytes.length); // first write should succeed
    // close() to ensure the written bytes are not buffered and counted
    // against the directory size
    out.close();
    out = dir.createOutput("bar", IOContext.DEFAULT);
    try {
      out.writeBytes(bytes, bytes.length);
      fail("should have failed on disk full");
    } catch (IOException e) {
      // expected
    }
    out.close();
    dir.close();
    
    // test copyBytes
    dir = newMockDirectory();
    dir.setMaxSizeInBytes(3);
    out = dir.createOutput("foo", IOContext.DEFAULT);
    out.copyBytes(new ByteArrayDataInput(bytes), bytes.length); // first copy should succeed
    // close() to ensure the written bytes are not buffered and counted
    // against the directory size
    out.close();
    out = dir.createOutput("bar", IOContext.DEFAULT);
    try {
      out.copyBytes(new ByteArrayDataInput(bytes), bytes.length);
      fail("should have failed on disk full");
    } catch (IOException e) {
      // expected
    }
    out.close();
    dir.close();
  }

  public void testAbuseClosedIndexInput() throws Exception {
    MockDirectoryWrapper dir = newMockDirectory();
    IndexOutput out = dir.createOutput("foo", IOContext.DEFAULT);
    out.writeByte((byte) 42);
    out.close();
    final IndexInput in = dir.openInput("foo", IOContext.DEFAULT);
    in.close();
    expectThrows(RuntimeException.class, new ThrowingRunnable() {
      @Override
      public void run() throws Throwable {
        in.readByte();
      }
    });
    dir.close();
  }

  public void testAbuseCloneAfterParentClosed() throws Exception {
    MockDirectoryWrapper dir = newMockDirectory();
    IndexOutput out = dir.createOutput("foo", IOContext.DEFAULT);
    out.writeByte((byte) 42);
    out.close();
    IndexInput in = dir.openInput("foo", IOContext.DEFAULT);
    final IndexInput clone = in.clone();
    in.close();
    expectThrows(RuntimeException.class, new ThrowingRunnable() {
      @Override
      public void run() throws Throwable {
        clone.readByte();
      }
    });

    dir.close();
  }

  public void testAbuseCloneOfCloneAfterParentClosed() throws Exception {
    MockDirectoryWrapper dir = newMockDirectory();
    IndexOutput out = dir.createOutput("foo", IOContext.DEFAULT);
    out.writeByte((byte) 42);
    out.close();
    IndexInput in = dir.openInput("foo", IOContext.DEFAULT);
    IndexInput clone1 = in.clone();
    final IndexInput clone2 = clone1.clone();
    in.close();
    expectThrows(RuntimeException.class, new ThrowingRunnable() {
      @Override
      public void run() throws Throwable {
        clone2.readByte();
      }
    });
    dir.close();
  }
}
