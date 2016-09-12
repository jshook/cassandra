/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package iodoc;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;

import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * This is the set of instrumentation methods to be used by the byteman
 * injections for narrating the IO and formats used in sstables.
 */
@SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" })
public class IODoc
{
    private MappedByteBuffer mbb = getMappedBuffer();
    private Charset charset = Charset.forName("UTF-8");

    public void logOpen(File file) {
        mbb.put(bytes("open:" + file.getAbsolutePath()));
        mbb.force();
    }

    public void logWrite(DataOutputPlus dop, String string)
    {
        mbb.put(bytes(string));
        mbb.force();
    }

    private File getTmpFile()
    {
        File file = null;
        try
        {
            file = File.createTempFile("iolog", "");
        }
        catch (IOException ioe)
        {
            throw new RuntimeException(ioe);
        }
        return file;
    }

    public FileChannel getChannel()
    {
        try
        {
            FileOutputStream fos = new FileOutputStream(getTmpFile());
            return fos.getChannel();
        }
        catch (IOException ioe)
        {
            throw new RuntimeException(ioe);
        }
    }

    private MappedByteBuffer getMappedBuffer()
    {
        FileChannel channel = getChannel();
        MappedByteBuffer mbb = null;
        try
        {
            mbb = channel.map(FileChannel.MapMode.READ_WRITE, 0, 10000000);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        mbb.position(0);
        return mbb;
    }

    private byte[] bytes(String string) {
        return string.getBytes(charset);
    }


}
