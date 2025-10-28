using IMDbApplication.Models;
using IMDbApplication.Utilities;
using System.Collections.Concurrent;
using System.Diagnostics;

namespace IMDbApplication.Readers;

public class MoviesReader
{
    public static Dictionary<string, Movie> LoadMovies(string path)
    {
        var stopwatch = Stopwatch.StartNew();
        var movies = new ConcurrentDictionary<string, Movie>();
        int totalLines = 0;
        int processedLines = 0;

        var processorCount = Environment.ProcessorCount;

        var linesQueue = new BlockingCollection<string>(boundedCapacity: 10000);
        var moviesQueue = new BlockingCollection<Movie>(boundedCapacity: 1000);

        // Reader task
        var readerTask = Task.Run(() =>
        {
            try
            {
                using (var reader = new StreamReader(path)) 
                {
                    reader.ReadLine();
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
        int movieIdCounter = 0;
        int errorCount = 0;

        for (int i = 0; i < processorCount; i++)
        {
            parserTasks[i] = Task.Run(() =>
            {
                foreach (var line in linesQueue.GetConsumingEnumerable())
                {
                    try
                    {
                        if (HardcodedParsers.TryParseMovieLine(line, out var parsed))
                        {
                            var movie = new Movie
                            {
                                ID = Interlocked.Increment(ref movieIdCounter),
                                ImdbID = parsed.ImdbID,
                                Title = parsed.Title
                            };

                            moviesQueue.Add(movie);
                            Interlocked.Increment(ref processedLines);
                        }
                    }
                    catch (Exception ex)
                    {
                        Interlocked.Increment(ref errorCount);

                        if (errorCount <= 10)
                            Console.WriteLine($"[Movies Parser] Error parsing line: {ex.Message}");
                    }
                }
            });
        }


        var aggregatorTask = Task.Run(() =>
        {
            foreach (var movie in moviesQueue.GetConsumingEnumerable())
            {
                movies.TryAdd(movie.ImdbID, movie);
            }
        });

        readerTask.Wait();

        Task.WaitAll(parserTasks);
        moviesQueue.CompleteAdding();

        aggregatorTask.Wait();

        if (errorCount > 0)
            Console.WriteLine($"Warning: {errorCount} lines were skipped");

        stopwatch.Stop();
        Console.WriteLine($"Loaded {movies.Count}/{totalLines} records in {stopwatch.ElapsedMilliseconds} ms");
        Console.WriteLine($"Processed {processedLines} suitable records");

        return new Dictionary<string, Movie>(movies);

    }
}