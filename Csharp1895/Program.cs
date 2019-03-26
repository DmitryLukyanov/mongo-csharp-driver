using System;
using System.IO;
using System.Threading;
using MongoDB.Bson;
using MongoDB.Driver;

namespace tour

{
    class Program
    {
        // the application to reproduce exception by steps from: a comment - Mar 06 2019 03:00:23 PM GMT+0000 
        static void Main(string[] args)
        {
            string logFile = @"c:\Users\Dmitry_Lukyanov\Documents\backups\1895\log.txt";
            File.AppendAllText(logFile, "######_________________start" + Environment.NewLine);

            var uri = "url";
            Console.WriteLine("Start");

            var client = new MongoClient(uri);
            var database = client.GetDatabase("test");
            var collection = database.GetCollection<BsonDocument>("zipdata");

            long count = 0;
            Console.WriteLine("Start Querying");

            // Endless loop to query database and to catch the Exception 
            while (true)
            {
                try
                {
                    count = collection.CountDocuments(new BsonDocument());
                    Console.WriteLine("Doc counts: " + count);
                    Thread.Sleep(50);

                    try
                    {
                        File.AppendAllText(logFile, "count:" + count + Environment.NewLine);
                    }
                    catch (Exception )
                    {
                        // just breakpoint
                    }
                }
                catch (Exception e)
                {
                    try
                    {
                        File.AppendAllText(logFile, DateTime.Now + " _________________" + e + Environment.NewLine);
                        File.AppendAllText(logFile, "######_________________end_exception" + Environment.NewLine);
                    }
                    catch
                    {
                        // just breakpoint
                    }

                    Console.WriteLine(e);
                }
            }
        }
    }
}