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
        var people = new ConcurrentDictionary<string, Person>();
        int totalLines = 0;
        int personIdCounter = 0;

        var processorCount = Environment.ProcessorCount;

        var linesQueue = new BlockingCollection<string>(boundedCapacity: 10000);
        var peopleQueue = new BlockingCollection<KeyValuePair<string, Person>>(boundedCapacity: 1000);

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
                        // only nconst, primaryName, birthYear and deathYear
                        var fields = StringParser.ExtractTSVFields(line, 0, 1, 2, 3);

                        if (fields.Length < 4) continue;

                        string nconst = fields[0];
                        string fullName = fields[1];

                        // Parse name
                        int spaceIdx = fullName.IndexOf(' ');
                        string firstName = spaceIdx > 0 ? fullName.Substring(0, spaceIdx) : fullName;
                        string lastName = spaceIdx > 0 ? fullName.Substring(spaceIdx + 1) : "";

                        int.TryParse(fields[2], out int birthYear);
                        int? deathYear = fields[3] != "\\N" && int.TryParse(fields[3], out int death) ? death : null;

                        var person = new Person
                        {
                            ID = Interlocked.Increment(ref personIdCounter),
                            FirstName = firstName,
                            LastName = lastName,
                            BirthYear = birthYear,
                            DeathYear = deathYear
                        };

                        peopleQueue.Add(new KeyValuePair<string, Person>(nconst, person));
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

        var aggregatorTask = Task.Run(() =>
        {
            foreach (var kvp in peopleQueue.GetConsumingEnumerable())
            {
                people.TryAdd(kvp.Key, kvp.Value);
            }
        });

        readerTask.Wait();
        Task.WaitAll(parserTasks);
        peopleQueue.CompleteAdding();
        aggregatorTask.Wait();

        stopwatch.Stop();
        Console.WriteLine($"Loaded {people.Count} people from {totalLines} records in {stopwatch.ElapsedMilliseconds} ms");

        return new Dictionary<string, Person>(people);
    }

    public static void LinkPeopleToMovies(Dictionary<string, Movie> movies,
                                        Dictionary<string, Person> peopleIndex,
                                        string actorCodesPath)
    {
        var stopwatch = Stopwatch.StartNew();
        int totalLines = 0;
        int linksCreated = 0;

        var processorCount = Environment.ProcessorCount;

        var linkOperations = new ConcurrentBag<Action>();
        var linesQueue = new BlockingCollection<string>(boundedCapacity: 10000);

        // Reader
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
                        // Extract: tconst(0), nconst(2), category(3)
                        var fields = StringParser.ExtractTSVFields(line, 0, 2, 3);

                        if (fields.Length < 3) continue;

                        string tconst = fields[0];
                        string nconst = fields[1];
                        string category = fields[2];

                        if (movies.TryGetValue(tconst, out var movie) &&
                            peopleIndex.TryGetValue(nconst, out var person))
                        {
                            // Create link operation
                            linkOperations.Add(() =>
                            {
                                if (category == "director")
                                {
                                    lock (movie)
                                    {
                                        movie.Director = person.FullName;
                                    }
                                    lock (person.DirectedMovies)
                                    {
                                        person.DirectedMovies.Add(movie);
                                    }
                                }
                                else if (category == "actor" || category == "actress")
                                {
                                    lock (movie.Actors)
                                    {
                                        movie.Actors.Add(person.FullName);
                                    }
                                    lock (person.ActedMovies)
                                    {
                                        person.ActedMovies.Add(movie);
                                    }
                                }
                            });

                            Interlocked.Increment(ref linksCreated);
                        }
                    }
                    catch (Exception ex)
                    {
                        Interlocked.Increment(ref errorCount);
                        if (errorCount <= 10)
                        {
                            Console.WriteLine($"[Link People To Movies] Error connection lines: {ex.Message}");
                        }
                    }
                }
            });
        }

        readerTask.Wait();
        Task.WaitAll(parserTasks);

        var linkStopwatch = Stopwatch.StartNew();
        Parallel.ForEach(linkOperations, operation => operation());
        linkStopwatch.Stop();

        stopwatch.Stop();
        Console.WriteLine($"Created {linksCreated} people-movie links from {totalLines} records in {stopwatch.ElapsedMilliseconds} ms");
        Console.WriteLine($"Link execution time: {linkStopwatch.ElapsedMilliseconds} ms");
    }
}