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
        int errorCount = 0;

        for (int i = 0; i < processorCount; i++)
        {
            parserTasks[i] = Task.Run(() =>
            {
                foreach (var line in linesQueue.GetConsumingEnumerable())
                {
                    try
                    {
                        // only movieId, imdbId
                        string movieLensId = StringParser.ExtractCSVField(line, 0);
                        string imdbIdRaw = StringParser.ExtractCSVField(line, 1);

                        string imdbId = imdbIdRaw.StartsWith("tt") ? imdbIdRaw : "tt" + imdbIdRaw;

                        linksQueue.Add(new KeyValuePair<string, string>(movieLensId, imdbId));
                    }
                    catch (Exception ex)
                    {
                        Interlocked.Increment(ref errorCount);
                        if (errorCount <= 10)
                        {
                            Console.WriteLine($"[Tags Parsing] Error parsing line: {ex.Message}");
                        }
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
        int errorCount = 0;

        for (int i = 0; i < processorCount; i++)
        {
            parserTasks[i] = Task.Run(() =>
            {
                foreach (var line in linesQueue.GetConsumingEnumerable())
                {
                    try
                    {
                        // only tagId, tag
                        int commaIdx = line.IndexOf(',');
                        if (commaIdx > 0 && commaIdx < line.Length - 1)
                        {
                            string tagId = line.Substring(0, commaIdx);
                            string tagName = line.Substring(commaIdx + 1);

                            tagsQueue.Add(new KeyValuePair<string, string>(tagId, tagName));
                        }
                    }
                    catch (Exception ex)
                    {
                        Interlocked.Increment(ref errorCount);
                        if (errorCount <= 10)
                        {
                            Console.WriteLine($"[Tag Names] Error parsing line: {ex.Message}");
                        }
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
        int tagsProcessed = 0;
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
                        // only movieId, tagId, relevance
                        var fields = StringParser.ExtractCSVField(line, 0);
                        string movieLensId = fields;
                        string tagId = StringParser.ExtractCSVField(line, 1);
                        string relevanceStr = StringParser.ExtractCSVField(line, 2);

                        if (float.TryParse(relevanceStr, out float relevance) && relevance > 0.5f)
                        {
                            Interlocked.Increment(ref relevantTags);

                            if (movieLensToImdb.TryGetValue(movieLensId, out string? imdbId) &&
                                movies.TryGetValue(imdbId, out var movie) &&
                                tagsIndex.TryGetValue(tagId, out string? tagName))
                            {
                                lock (movie.Tags)
                                {
                                    movie.Tags.Add(tagName);
                                }
                                Interlocked.Increment(ref tagsProcessed);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Interlocked.Increment(ref errorCount);
                        if (errorCount <= 10)
                        {
                            Console.WriteLine($"[People Parser] Error parsing line: {ex.Message}");
                        }
                    }
                }
            });
        }

        readerTask.Wait();
        Task.WaitAll(parserTasks);

        stopwatch.Stop();
        Console.WriteLine($"Processed {tagsProcessed} tags for movies");
        Console.WriteLine($"Relevant tags (>0.5): {relevantTags} from {totalLines} records");
        Console.WriteLine($"Processing time: {stopwatch.ElapsedMilliseconds} ms");
    }
}