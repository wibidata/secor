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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;

import java.io.FileNotFoundException;
import java.util.Vector;

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
        final FileSystem fileSystem = FileUtil.getFileSystem(path);
        Path fsPath = new Path(path);
        FileStatus[] fileList = fileSystem.globStatus(fsPath);
        if (fileList.length == 0) {
            throw new FileNotFoundException("unable to find file '" + path + "'");
        }
        for (FileStatus fileStatus : fileList) {
            if (fileStatus.isDirectory()) {
                if (!mRecursive) {
                    System.err.println("set -recursive to read directory '" + fsPath + "'");
                    continue;
                }
                // read the remote iterator completely before reading files to prevent it
                // from going stale.
                RemoteIterator<LocatedFileStatus> recursiveFilesIterator =
                    fileSystem.listFiles(fileStatus.getPath(), true);
                Vector<FileStatus> recursiveFileList = new Vector<FileStatus>();
                while (recursiveFilesIterator.hasNext()) {
                    FileStatus recursiveFileStatus = recursiveFilesIterator.next();
                    if (recursiveFileStatus.isFile()) {
                        recursiveFileList.add(recursiveFileStatus);
                    }
                }
                for (FileStatus recursiveFileStatus : recursiveFileList) {
                    printFile(recursiveFileStatus.getPath());
                }
            } else if (fileStatus.isFile()) {
                printFile(fileStatus.getPath());
            } else {
                System.err.println
                    ("Non-file and non-directory found at '" + fileStatus.getPath() + "'?");
            }
        }
    }

    private void printFile(Path fsPath) throws Exception {
        SequenceFile.Reader reader = new SequenceFile.Reader(new Configuration(), SequenceFile.Reader.file(fsPath));
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
