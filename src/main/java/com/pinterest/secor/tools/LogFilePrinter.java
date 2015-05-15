/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.secor.tools;

import com.pinterest.secor.util.FileUtil;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;

import java.io.FileNotFoundException;

/**
 * Log file printer displays the content of a log file.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class LogFilePrinter {
    private boolean mPrintOffsetsOnly;
    private boolean mPrintMessagesOnly;
    private boolean mRecursive;

    public LogFilePrinter(boolean printOffsetsOnly, boolean printMessagesOnly, boolean recursive) {
        mPrintOffsetsOnly = printOffsetsOnly;
        mPrintMessagesOnly = printMessagesOnly;
        mRecursive = recursive;
    }

    public void print(String path) throws Exception {
        // change s3: uris to s3n: so hadoop loads the right file system.
        path = path.replace("^s3://", "s3n://");
        final FileSystem fileSystem = FileUtil.getFileSystem(path);
        final Path fsPath = new Path(path);
        final FileStatus[] fileList = fileSystem.globStatus(fsPath);
        if (fileList == null || fileList.length == 0) {
            throw new FileNotFoundException("unable to find file '" + path + "'");
        }
        for (FileStatus fileStatus : fileList) {
            if (fileStatus.isDir()) {
                if (!mRecursive) {
                    System.err.println("set -recursive to read directory '" + fsPath + "'");
                    continue;
                }
                printDirectory(fileSystem, fileStatus.getPath());
            } else {
                printFile(fileSystem, fileStatus.getPath());
            }
        }
    }

    private void printDirectory(FileSystem fileSystem, Path dirPath) throws Exception {
        final FileStatus[] statusList = fileSystem.listStatus(dirPath);
        if (statusList == null) {
            System.err.println("No status found for " + dirPath);
        } else {
            for (FileStatus fileStatus : fileSystem.listStatus(dirPath)) {
                if (fileStatus.isDir()) {
                    printDirectory(fileSystem, fileStatus.getPath());
                } else {
                    printFile(fileSystem, fileStatus.getPath());
                }
            }
        }
    }

    private void printFile(FileSystem fileSystem, Path fsPath) throws Exception {
        SequenceFile.Reader reader = new SequenceFile.Reader(fileSystem, fsPath, fileSystem.getConf());
        try {
            LongWritable key = (LongWritable) reader.getKeyClass().newInstance();
            BytesWritable value = (BytesWritable) reader.getValueClass().newInstance();
            System.err.println("reading file " + fsPath);
            while (reader.next(key, value)) {
                if (mPrintOffsetsOnly) {
                    System.out.println(Long.toString(key.get()));
                } else {
                    byte[] nonPaddedBytes = new byte[value.getLength()];
                    System.arraycopy(value.getBytes(), 0, nonPaddedBytes, 0, value.getLength());
                    if (!mPrintMessagesOnly) {
                        System.out.print(Long.toString(key.get()) + ": ");
                    }
                    System.out.println(new String(nonPaddedBytes));
                }
            }
        } finally {
            reader.close();
        }
    }
}
