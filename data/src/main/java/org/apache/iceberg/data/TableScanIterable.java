/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.data;

import java.io.IOException;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;

class TableScanIterable extends CloseableGroup implements CloseableIterable<Record> {
  private final GenericReader reader;
  private final CloseableIterable<FileScanTask> tasks;

  TableScanIterable(TableScan scan, boolean reuseContainers) {
    this.reader = new GenericReader(scan, reuseContainers);
    // start planning tasks in the background
    this.tasks = scan.planFiles();
  }

  @Override
  public CloseableIterator<Record> iterator() {
    CloseableIterator<Record> iter = reader.open(tasks);
    addCloseable(iter);
    return iter;
  }

  CloseableIterable<CloseableIterable<Record>> fileIterable() {
    return new TasksIterable(tasks);
  }

  /**
   * An iterable that iterates over tasks and opens each one as an independent CloseableIterable.
   * <p>
   * Each task produced by this should be independently closed.
   */
  private class TasksIterable extends CloseableGroup implements CloseableIterable<CloseableIterable<Record>> {
    private final CloseableIterable<FileScanTask> tasks;

    private TasksIterable(CloseableIterable<FileScanTask> tasks) {
      this.tasks = tasks;
    }

    @Override
    public CloseableIterator<CloseableIterable<Record>> iterator() {
      CloseableIterator<FileScanTask> files = tasks.iterator();
      TasksIterable.this.addCloseable(files);
      return CloseableIterator.transform(files, ScanIterable::new);
    }
  }

  /**
   * An iterable that opens a task and produces that task's records.
   */
  private class ScanIterable extends CloseableGroup implements CloseableIterable<Record> {
    private final FileScanTask task;

    private ScanIterable(FileScanTask task) {
      this.task = task;
    }

    @Override
    public CloseableIterator<Record> iterator() {
      CloseableIterator<Record> iter = reader.open(CloseableIterable.withNoopClose(task));
      addCloseable(iter);
      return iter;
    }
  }

  @Override
  public void close() throws IOException {
    tasks.close(); // close manifests from scan planning
    super.close(); // close data files
  }
}
