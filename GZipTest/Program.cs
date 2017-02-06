using System;

namespace GZipTest
{
    internal class Program
    {
        public static void Main(string[] args)
        {
            if (args.Length < 3 || args.Length > 4)
            {
                Console.WriteLine("Please provide arguments: compress/decompress, input file name, output file name, verbose (optional).");
                return;
            }

            switch (args[0])
            {
                case "compress":
                    var compressor = new Compressor(args[1], args[2], args.Length == 4 && args[3] == "verbose");
                    compressor.Start();
                    break;
                case "decompress":
                    var decompressor = new Decompressor(args[1], args[2], args.Length == 4 && args[3] == "verbose");
                    decompressor.Start();
                    break;
                default:
                    Console.WriteLine("The first argument must be compress or decompress!");
                    break;
            }

            //var compressor = new Compressor(@"D:\test\test.vhd", @"D:\test\test.vhd.gz", true);
            //compressor.Start();
        }
    }
}