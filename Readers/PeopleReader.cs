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
        int errorCount = 0;

        for (int i = 0; i < processorCount; i++)
        {
            parserTasks[i] = Task.Run(() =>
            {
                var localResult = new Dictionary<string, Person>(1_200_000);

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

                        localResult[nconst] = new Person
                        {
                            ID = Interlocked.Increment(ref personIdCounter),
                            FirstName = firstName,
                            LastName = lastName,
                            BirthYear = birthYear,
                            DeathYear = deathYear
                        };
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
                localDictionaries.Add(localResult);
            });
        }

        readerTask.Wait();
        Task.WaitAll(parserTasks);

        var finalPeople = new Dictionary<string, Person>(totalLines);
        foreach (var localDict in localDictionaries)
        {
            foreach (var pair in localDict)
            {
                finalPeople[pair.Key] = pair.Value;
            }            
        }


        if (errorCount > 0)
        {
            Console.WriteLine($"Warning: {errorCount} lines were skipped");
        }

        stopwatch.Stop();
        Console.WriteLine($"Loaded {finalPeople.Count} people from {totalLines} records in {stopwatch.ElapsedMilliseconds} ms");

        return finalPeople;
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
        int errorCount = 0;

        for (int i = 0; i < processorCount; i++)
        {
            parserTasks[i] = Task.Run(() =>
            {
                var localResult = new List<(Movie, Person, string)>(1_000_000);

                foreach (var line in linesQueue.GetConsumingEnumerable())
                {
                    try
                    {
                        // only tconst, nconst, category
                        var fields = StringParser.ExtractTSVFields(line, 0, 2, 3);

                        if (fields.Length < 3) continue;

                        string tconst = fields[0];
                        string nconst = fields[1];
                        string category = fields[2];

                        if (movies.TryGetValue(tconst, out var movie) &&
                            peopleIndex.TryGetValue(nconst, out var person))
                        {
                            localResult.Add((movie, person, category));
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
                localLinks.Add(localResult);
            });
        }

        readerTask.Wait();
        Task.WaitAll(parserTasks);

        int linksCreated = 0;
        foreach (var localList in localLinks)
        {
            foreach (var (movie, person, category) in localList)
            {
                if (category == "director")
                {
                    movie.Director = person.FullName;
                    person.DirectedMovies.Add(movie);
                    linksCreated++;
                }
                else if (category == "actor" || category == "actress")
                {
                    movie.Actors.Add(person.FullName);
                    person.ActedMovies.Add(movie);
                    linksCreated++;
                }
            }
        }


        stopwatch.Stop();
        Console.WriteLine($"Created {linksCreated} people-movie links from {totalLines} records in {stopwatch.ElapsedMilliseconds} ms");
    }
}