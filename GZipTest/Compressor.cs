using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;

namespace GZipTest
{
    class Compressor : Worker
    {
        private static readonly object Locker = new object();

        public Compressor(string inFileName, string outFileName, bool verbose) : base(inFileName, outFileName, verbose)
        {
        }

        //read uncompressed data chunks from disk
        public override void Start()
        {
            StartTime = DateTime.Now;
            Log("Available cores: " + CoreCount);
            Log("Available memory (MB): " + CompInfo.AvailablePhysicalMemory / (1024 * 1024));
            Log("Compression started...");
            try
            {
                using (var inFileStream = new FileStream(InFileName, FileMode.Open))
                {
                    InFileStream = inFileStream;
                    for (ChunkCounter = 0; inFileStream.Length - inFileStream.Position > 0; ChunkCounter++)
                    {
                        if (StopRequested)
                            return;
                        //ram overload - wait
                        while ((float)CompInfo.AvailablePhysicalMemory / InitialFreeRam < 0.2)
                        {
                            Log("Memory overload. Data reading paused. Free RAM rate: " + (float)CompInfo.AvailablePhysicalMemory / InitialFreeRam);
                            //force gc cleanup
                            GC.Collect(2, GCCollectionMode.Forced);
                            Thread.Sleep(1000);
                        }
                        //optimal data read chunk size based on ram and cpu cores
                        var dataChunkSize = (long)CompInfo.AvailablePhysicalMemory / CoreCount / 4;
                        if (inFileStream.Length - inFileStream.Position <= dataChunkSize)
                            //this is the last file part
                            dataChunkSize = inFileStream.Length - inFileStream.Position;
                        if (inFileStream.Length < dataChunkSize)
                            dataChunkSize = inFileStream.Length / CoreCount + 1;
                        if (dataChunkSize > 256*1024*1024)
                            //we don't need too huge chunks
                            dataChunkSize = 256*1024*1024;
                        var dataChunk = new byte[dataChunkSize];
                        //read file
                        inFileStream.Read(dataChunk, 0, (int)dataChunkSize);
                        lock(Locker)
                            IncomingChunks.Add(ChunkCounter, dataChunk);
                        //start new thread with chunk number as argument
                        var chunkNumber = ChunkCounter;
                        var compressThread = new Thread(() => ProcessChunk(chunkNumber));
                        compressThread.Start();
                        Log("Chunk #" + ChunkCounter + " (size = " +
                            dataChunkSize / (1024 * 1024) + " MB) started. Available memory (MB): " +
                            CompInfo.AvailablePhysicalMemory / (1024 * 1024));
                    }
                    IncomingFinished = true;
                }
            }
            catch (Exception ex)
            {
                Log("ERROR!\n" + ex);
                StopRequested = true;
                Console.Write("1");
            }
        }

        //compress data chunk
        protected override void ProcessChunk(int chunkNumber)
        {
            if (StopRequested)
                return;
            ProcessSemaphore.WaitOne();
            try
            {
                KeyValuePair<int, byte[]> chunk;
                lock (Locker)
                    chunk = IncomingChunks.FirstOrDefault(b => b.Key == chunkNumber);
                //stream for processed data
                using (var outMemStream = new MemoryStream())
                {
                    using (var zipStream = new GZipStream(outMemStream, CompressionMode.Compress))
                    {
                        using (var inMemStream = new MemoryStream(chunk.Value, 0, chunk.Value.Length))
                        {
                            inMemStream.CopyTo(zipStream);
                        }
                    }
                    //compressed bytes
                    var bytes = outMemStream.ToArray();
                    lock (Locker)
                    {
                        //add new data chunk to collection of compressed data
                        OutgoingChunks.Add(chunkNumber, bytes);
                        //remove chunk from collection of uncompressed data
                        IncomingChunks.Remove(chunkNumber);
                    }
                    Log("#" + chunkNumber + " compressed! Size(MB) = " + bytes.Length / (1024 * 1024));
                }
            }
            catch (Exception ex)
            {
                Log("ERROR!\n" + ex);
                StopRequested = true;
                Console.Write("1");
            }
            ProcessSemaphore.Release();
            //if this is the first chunk - start flush to disk in a separate thread
            if (chunkNumber == 0 && FlushThread.ThreadState != ThreadState.Running && !StopRequested)
                FlushThread.Start();
        }

        protected override void FlushToDisk()
        {
            Log("Starting to write compressed data to disk.");
            try
            {
                using (var outFileStream = new FileStream(OutFileName, FileMode.Create))
                {
                    OutFileStream = outFileStream;
                    //flush all zipped data chunks to disk
                    for (var chunkNumber = 0; !(IncomingFinished && chunkNumber == ChunkCounter); chunkNumber++)
                    {
                        if (StopRequested)
                            return;
                        //waiting for the next chunk to flush to disk
                        for (;;)
                        {
                            KeyValuePair<int, byte[]> chunk;
                            lock (Locker)
                                chunk = OutgoingChunks.FirstOrDefault(b => b.Key == chunkNumber);
                            if (chunk.Value != null)
                            {
                                if (StopRequested)
                                    return;
                                //append compressed data chunk size to gzip header
                                BitConverter.GetBytes(chunk.Value.Length + 1).CopyTo(chunk.Value, 4);
                                outFileStream.Write(chunk.Value, 0, chunk.Value.Length);
                                Log("#" + chunk.Key + " flushed to disk!");
                                lock (Locker)
                                    OutgoingChunks.Remove(chunk.Key);
                                break;
                            }
                            Thread.Sleep(100);
                        }
                    }
                    var elapsed = DateTime.Now - StartTime;
                    Log("Time elapsed: " + elapsed.TotalSeconds + " seconds.");
                    Console.Write("0");
                }
            }
            catch (Exception ex)
            {
                Log("ERROR!\n" + ex);
                StopRequested = true;
                Console.Write("1");
            }
        }
    }
}
