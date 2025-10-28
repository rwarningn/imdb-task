using IMDbApplication.Models;
using IMDbApplication.Utilities;
using System.Collections.Concurrent;
using System.Globalization;
using System.Diagnostics;

namespace IMDbApplication.Readers;

public class RatingsReader
{
    public static void LoadRatings(Dictionary<string, Movie> movies, string ratingsPath)
    {
        var stopwatch = Stopwatch.StartNew();
        int ratingsLoaded = 0;
        int totalLines = 0;

        var processorCount = Environment.ProcessorCount;

        var linesQueue = new BlockingCollection<string>(boundedCapacity: 10000);

        var readerTask = Task.Run(() =>
        {
            try
            {
                using (var reader = new StreamReader(ratingsPath))
                {
                    reader.ReadLine(); // skip header
                    string? line;

                    while ((line = reader.ReadLine()) != null)
                    {
                        Interlocked.Increment(ref totalLines);
                        linesQueue.Add(line);
                    }
                }
            }
            finally
            {
                linesQueue.CompleteAdding();
            }
        });

        var parserTasks = new Task[processorCount];
        int errorCount = 0;

        for (int i = 0; i < processorCount; i++)
        {
            parserTasks[i] = Task.Run(() =>
            {
                foreach (var line in linesQueue.GetConsumingEnumerable())
                {
                    try
                    {
                        // only tconst, averageRating
                        string tconst = StringParser.ExtractTSVField(line, 0);
                        string ratingStr = StringParser.ExtractTSVField(line, 1);

                        if (float.TryParse(ratingStr, NumberStyles.Any, CultureInfo.InvariantCulture,
                             out float rating) &&
                            movies.TryGetValue(tconst, out var movie))
                        {
                            lock (movie)
                            {
                                movie.Rating = rating;

                            }
                            Interlocked.Increment(ref ratingsLoaded);
                        }
                    }
                    catch (Exception ex)
                    {
                        Interlocked.Increment(ref errorCount);
                        if (errorCount <= 10)
                        {
                            Console.WriteLine($"[Rating Parser] Error parsing line: {ex.Message}");
                        }
                    }
                }
            });
        }

        readerTask.Wait();
        Task.WaitAll(parserTasks);

        stopwatch.Stop();
        Console.WriteLine($"Loaded {ratingsLoaded} ratings from {totalLines} records in {stopwatch.ElapsedMilliseconds} ms");
    }
}