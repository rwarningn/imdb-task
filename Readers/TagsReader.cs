using IMDbApplication.Models;
using IMDbApplication.Utilities;
using System.Collections.Concurrent;
using System.Diagnostics;

namespace IMDbApplication.Readers;

public class TagsReader
{
    public static Dictionary<string, string> LoadLinks(string linksPath)
    {
        var stopwatch = Stopwatch.StartNew();
        var links = new ConcurrentDictionary<string, string>();
        int totalLines = 0;

        var processorCount = Environment.ProcessorCount;

        var linesQueue = new BlockingCollection<string>(boundedCapacity: 10000);
        var linksQueue = new BlockingCollection<KeyValuePair<string, string>>(boundedCapacity: 1000);

        var readerTask = Task.Run(() =>
        {
            try
            {
                using (var reader = new StreamReader(linksPath))
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

        for (int i = 0; i < processorCount; i++)
        {
            parserTasks[i] = Task.Run(() =>
            {
                foreach (var line in linesQueue.GetConsumingEnumerable())
                {
                    if (HardcodedParsers.TryParseMovieLensLinkLine(line, out var parsed))
                    {
                        linksQueue.Add(new KeyValuePair<string, string>(parsed.movieLensId, parsed.imdbId));
                    }
                }
            });
        }

        var aggregatorTask = Task.Run(() =>
        {
            foreach (var kvp in linksQueue.GetConsumingEnumerable())
            {
                links.TryAdd(kvp.Key, kvp.Value);
            }
        });

        readerTask.Wait();
        Task.WaitAll(parserTasks);
        linksQueue.CompleteAdding();
        aggregatorTask.Wait();

        stopwatch.Stop();
        Console.WriteLine($"Loaded {links.Count} IMDB-MovieLens links from {totalLines} records in {stopwatch.ElapsedMilliseconds} ms");

        return new Dictionary<string, string>(links);
    }

    public static Dictionary<string, string> LoadTagNames(string tagCodesPath)
    {
        var stopwatch = Stopwatch.StartNew();
        var tags = new ConcurrentDictionary<string, string>();
        int totalLines = 0;

        var processorCount = Environment.ProcessorCount;

        var linesQueue = new BlockingCollection<string>(boundedCapacity: 10000);
        var tagsQueue = new BlockingCollection<KeyValuePair<string, string>>(boundedCapacity: 1000);

        var readerTask = Task.Run(() =>
        {
            try
            {
                using (var reader = new StreamReader(tagCodesPath))
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

        for (int i = 0; i < processorCount; i++)
        {
            parserTasks[i] = Task.Run(() =>
            {
                foreach (var line in linesQueue.GetConsumingEnumerable())
                {
                    if (HardcodedParsers.TryParseTagCodeLine(line, out var parsed))
                    {
                        tagsQueue.Add(new KeyValuePair<string, string>(parsed.tagId, parsed.tagName));
                    }
                }
            });
        }

        var aggregatorTask = Task.Run(() =>
        {
            foreach (var kvp in tagsQueue.GetConsumingEnumerable())
            {
                tags.TryAdd(kvp.Key, kvp.Value);
            }
        });

        readerTask.Wait();
        Task.WaitAll(parserTasks);
        tagsQueue.CompleteAdding();
        aggregatorTask.Wait();

        stopwatch.Stop();
        Console.WriteLine($"Loaded {tags.Count} tags from {totalLines} records in {stopwatch.ElapsedMilliseconds} ms");

        return new Dictionary<string, string>(tags);
    }

    public static void ProcessTagScores(Dictionary<string, Movie> movies,
                                      Dictionary<string, string> movieLensToImdb,
                                      Dictionary<string, string> tagsIndex,
                                      string tagScoresPath)
    {
        var stopwatch = Stopwatch.StartNew();
        int totalLines = 0;
        int relevantTags = 0;

        var processorCount = Environment.ProcessorCount;

        var linesQueue = new BlockingCollection<string>(boundedCapacity: 10000);

        var readerTask = Task.Run(() =>
        {
            try
            {
                using (var reader = new StreamReader(tagScoresPath))
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

        var localTagLinks = new ConcurrentBag<List<(Movie movie, string tagName)>>();
        var parserTasks = new Task[processorCount];

        for (int i = 0; i < processorCount; i++)
        {
            parserTasks[i] = Task.Run(() =>
            {
                var localResult = new List<(Movie movie, string tagName)>();
                foreach (var line in linesQueue.GetConsumingEnumerable())
                {

                    if (HardcodedParsers.TryParseTagScoreLine(line, out var result, out var relevance))
                    {
                        string movieLensId = result.movieId;
                        string tagId = result.tagId;

                        Interlocked.Increment(ref relevantTags);

                        if (movieLensToImdb.TryGetValue(movieLensId, out string? imdbId) &&
                            movies.TryGetValue(imdbId, out var movie) &&
                            tagsIndex.TryGetValue(tagId, out string? tagName))
                        {

                            localResult.Add((movie, tagName));
                        }
                    }
                }
                localTagLinks.Add(localResult);
            });
        }

        readerTask.Wait();
        Task.WaitAll(parserTasks);

        var allTagLinks = new List<(Movie movie, string tagName)>();
        foreach (var ls in localTagLinks)
        {
            allTagLinks.AddRange(ls);
        }

        var groupedByMovie = allTagLinks.GroupBy(link => link.movie);

        int tagsProcessed = 0;

        Parallel.ForEach(groupedByMovie, movieGroup =>
        {
            var movie = movieGroup.Key;

            lock (movie.Tags)
            {
                foreach (var (_, tagName) in movieGroup)
                {
                    movie.Tags.Add(tagName);
                }
            }

            Interlocked.Add(ref tagsProcessed, movieGroup.Count());
        });

        stopwatch.Stop();
        Console.WriteLine($"Processed {tagsProcessed} tags for movies");
        Console.WriteLine($"Relevant tags (>0.5): {relevantTags} from {totalLines} records");
        Console.WriteLine($"Processing time: {stopwatch.ElapsedMilliseconds} ms");
    }
}