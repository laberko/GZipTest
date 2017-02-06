using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;

namespace GZipTest
{
    class Decompressor : Worker
    {
        private static readonly object Locker = new object();

        public Decompressor(string inFileName, string outFileName, bool verbose) : base(inFileName, outFileName, verbose)
        {
        }

        //read compressed data chunks from disk
        public override void Start()
        {
            StartTime = DateTime.Now;
            Log("Available cores: " + CoreCount);
            Log("Available memory (MB): " + CompInfo.AvailablePhysicalMemory / (1024 * 1024));
            Log("Decompression started...");
            try
            {
                using (var inFileStream = new FileStream(InFileName, FileMode.Open))
                {
                    InFileStream = inFileStream;
                    var header = new byte[8];
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
                        //extract compressed data chunk size from gzip header
                        inFileStream.Read(header, 0, 8);
                        var dataChunkSize = BitConverter.ToInt32(header, 4);
                        var compressedChunk = new byte[dataChunkSize];
                        header.CopyTo(compressedChunk, 0);
                        //get the rest bytes of data block
                        inFileStream.Read(compressedChunk, 8, dataChunkSize - 9);
                        lock (Locker)
                            IncomingChunks.Add(ChunkCounter, compressedChunk);
                        var chunkNumber = ChunkCounter;
                        var decompressThread = new Thread(() => ProcessChunk(chunkNumber));
                        decompressThread.Start();
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

        //decompress data chunk
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
                using (var inMemStream = new MemoryStream(chunk.Value, 0, chunk.Value.Length))
                {
                    //stream for processed data
                    using (var outMemStream = new MemoryStream())
                    {
                        using (var zipStream = new GZipStream(inMemStream, CompressionMode.Decompress))
                        {
                            zipStream.CopyTo(outMemStream);
                        }
                        var bytes = outMemStream.ToArray();
                        lock (Locker)
                        {
                            OutgoingChunks.Add(chunkNumber, bytes);
                            IncomingChunks.Remove(chunkNumber);
                        }
                        Log("#" + chunkNumber + " uncompressed! Size(MB) = " + bytes.Length / (1024 * 1024));
                    }
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
            Log("Starting to write uncompressed data to disk.");
            try
            {
                using (var outFileStream = new FileStream(OutFileName, FileMode.Create))
                {
                    OutFileStream = outFileStream;
                    //flush all decompressed data chunks to disk
                    for (var chunkNumber = 0; !(IncomingFinished && chunkNumber == ChunkCounter); chunkNumber++)
                    {
                        if (StopRequested)
                            return;
                        //waiting for the next chunk to flush to disk
                        for (;;)
                        {
                            if (StopRequested)
                                return;
                            KeyValuePair<int, byte[]> chunk;
                            lock (Locker)
                                chunk = OutgoingChunks.FirstOrDefault(b => b.Key == chunkNumber);
                            if (chunk.Value != null)
                            {
                                outFileStream.Write(chunk.Value, 0, chunk.Value.Length);
                                Log("#" + chunk.Key + " flushed to disk!");
                                lock (Locker)
                                    OutgoingChunks.Remove(chunk.Key);
                                //force gc cleanup of processed bytes
                                GC.Collect(2, GCCollectionMode.Forced);
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
