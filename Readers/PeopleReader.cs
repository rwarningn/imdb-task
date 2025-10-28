using IMDbApplication.Models;
using IMDbApplication.Utilities;
using System.Collections.Concurrent;
using System.Diagnostics;

namespace IMDbApplication.Readers;

public class PeopleReader
{
    public static Dictionary<string, Person> LoadPeople(string path)
    {
        var stopwatch = Stopwatch.StartNew();
        int totalLines = 0;
        var linesQueue = new BlockingCollection<string>(boundedCapacity: 10000);

        var readerTask = Task.Run(() =>
        {
            try
            {
                using (var reader = new StreamReader(path))
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


        var localDictionaries = new ConcurrentBag<Dictionary<string, Person>>();
        var processorCount = Environment.ProcessorCount;
        var parserTasks = new Task[processorCount];
        var personIdCounter = 0;

        for (int i = 0; i < processorCount; i++)
        {
            parserTasks[i] = Task.Run(() =>
            {
                var localResult = new Dictionary<string, Person>(2_500_000);

                foreach (var line in linesQueue.GetConsumingEnumerable())
                {
                    if (HardcodedParsers.TryParsePersonLine(line, out var parsed))
                    {
                        parsed.person.ID = Interlocked.Increment(ref personIdCounter);
                        localResult[parsed.nconst] = parsed.person;
                    }
                }
                localDictionaries.Add(localResult);
            });
        }

        readerTask.Wait();
        Task.WaitAll(parserTasks);

        var largestDict = localDictionaries.OrderByDescending(d => d.Count).First();

         foreach (var localDict in localDictionaries)
        {
            if (ReferenceEquals(largestDict, localDict)) continue; 
            foreach (var pair in localDict)
            {
                largestDict.TryAdd(pair.Key, pair.Value); 
            }
        }


        stopwatch.Stop();
        Console.WriteLine($"Loaded {largestDict.Count} people from {totalLines} records in {stopwatch.ElapsedMilliseconds} ms");

        return largestDict;
    }

    public static void LinkPeopleToMovies(Dictionary<string, Movie> movies,
                                        Dictionary<string, Person> peopleIndex,
                                        string actorCodesPath)
    {
        var stopwatch = Stopwatch.StartNew();
        int totalLines = 0;
        var processorCount = Environment.ProcessorCount;
        var linesQueue = new BlockingCollection<string>(boundedCapacity: 10000);

        var readerTask = Task.Run(() =>
        {
            try
            {
                using (var reader = new StreamReader(actorCodesPath))
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

        var localLinks = new ConcurrentBag<List<(Movie movie, Person person, string category)>>();

        var parserTasks = new Task[processorCount];

        for (int i = 0; i < processorCount; i++)
        {
            parserTasks[i] = Task.Run(() =>
            {
                var localResult = new List<(Movie, Person, string)>(1_000_000);

                foreach (var line in linesQueue.GetConsumingEnumerable())
                {
                    if (HardcodedParsers.TryParseLinkLine(line, out var link))
                    {
                        if (movies.TryGetValue(link.tconst, out var movie) &&
                            peopleIndex.TryGetValue(link.nconst, out var person))
                        {
                            localResult.Add((movie, person, link.category));
                        }
                    }
                }
                localLinks.Add(localResult);
            });
        }

        readerTask.Wait();
        Task.WaitAll(parserTasks);


        var allLinks = new List<(Movie movie, Person person, string category)>();
        foreach (var list in localLinks)
        {
            allLinks.AddRange(list);
        }

        stopwatch.Stop();
        Console.WriteLine($"Parsed links and found objects in {stopwatch.ElapsedMilliseconds} ms");

        var mergeStopwatch = Stopwatch.StartNew();
        int linksCreated = 0;

        Parallel.ForEach(allLinks, link =>
        {
            if (link.category == "director")
            {
                lock (link.movie)
                {
                    link.movie.Director = link.person.FullName;
                }
                
                lock (link.person.DirectedMovies)
                {
                    link.person.DirectedMovies.Add(link.movie);
                }
                Interlocked.Increment(ref linksCreated);
            }
            else // actor or actress
            {
                lock (link.movie.Actors)
                {
                    link.movie.Actors.Add(link.person.FullName);
                }
                
                lock (link.person.ActedMovies)
                {
                    link.person.ActedMovies.Add(link.movie);
                }
                Interlocked.Increment(ref linksCreated);
            }
        });

        mergeStopwatch.Stop();

        Console.WriteLine($"Created {linksCreated} people-movie links from {totalLines} records in {mergeStopwatch.ElapsedMilliseconds} ms");
    }
}