using Microsoft.VisualBasic.Devices;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading;

namespace GZipTest
{
    public abstract class Worker
    {
        //number of available processor cores - is the number of threads we will use
        protected static readonly int CoreCount = Environment.ProcessorCount;
        protected readonly string InFileName;
        protected readonly string OutFileName;
        //ComputerInfo will be used to get free RAM amount
        protected readonly ComputerInfo CompInfo;
        protected int ChunkCounter;
        protected DateTime StartTime;
        private readonly StreamWriter _logFile;
        private readonly bool _verbose;
        private static readonly object Locker = new object();
        protected readonly Dictionary<int, byte[]> OutgoingChunks;
        protected readonly Dictionary<int, byte[]> IncomingChunks;
        protected bool IncomingFinished;
        //separate thread for writing compressed data chunks to disk
        protected readonly Thread FlushThread;
        //semaphore to control number of simultaneous compress threads
        protected static Semaphore ProcessSemaphore;
        protected bool StopRequested;
        protected FileStream InFileStream;
        protected FileStream OutFileStream;


        protected Worker(string inFileName, string outFileName, bool verbose)
        {
            _verbose = verbose;
            InFileName = inFileName;
            OutFileName = outFileName;
            CompInfo = new ComputerInfo();
            OutgoingChunks = new Dictionary<int, byte[]>();
            IncomingChunks = new Dictionary<int, byte[]>();
            ProcessSemaphore = new Semaphore(CoreCount, CoreCount);
            FlushThread = new Thread(FlushToDisk);
            _logFile = new StreamWriter(inFileName + ".log")
            {
                AutoFlush = true
            };
        }

        //non-threaded zip - for test
        public void CompressNonThread()
        {
            Console.WriteLine("Available cores: " + CoreCount);
            Console.WriteLine("Available memory (MB): " + CompInfo.AvailablePhysicalMemory / (1024 * 1024));
            Console.WriteLine("Non-threaded compression started...");

            StartTime = DateTime.Now;

            using (var inFileStream = new FileStream(InFileName, FileMode.Open))
            {
                using (var outFileStream = new FileStream(InFileName + ".gz", FileMode.Create))
                {
                    for (var chunk = 0; ; chunk++)
                    {
                        var dataChunkSize = (long) CompInfo.AvailablePhysicalMemory/CoreCount/4;
                        if (inFileStream.Length - inFileStream.Position <= dataChunkSize)
                        {
                            //this is the last file part
                            dataChunkSize = inFileStream.Length - inFileStream.Position;
                            if (dataChunkSize == 0)
                                //no more data to read
                                break;
                        }
                        var bytes = new byte[dataChunkSize];
                        inFileStream.Read(bytes, 0, (int) dataChunkSize);
                        using (var zipStream = new GZipStream(outFileStream, CompressionMode.Compress, true))
                        {
                            zipStream.Write(bytes, 0, bytes.Length);
                            Console.WriteLine("Zipped chunk #" + chunk);
                        }
                    }
                }
            }
            var elapsed = DateTime.Now - StartTime;
            Console.WriteLine("Time elapsed: " + elapsed.TotalSeconds + " sec.");
            Console.WriteLine("Press a key to exit.");
            Console.ReadKey();
        }

        public abstract void Start();

        protected abstract void ProcessChunk(int chunkNumber);

        protected abstract void FlushToDisk();

        protected void Log(string text)
        {
            lock (Locker)
            {
                _logFile.WriteLine(text);
            }
            if (_verbose)
                Console.WriteLine(text);
        }
    }
}
