// Services/DataProvider.cs
using IMDbApplication.Models;
using IMDbApplication.Utilities;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;

namespace IMDbApplication.Services;

public class DataProvider
{
    // --- КОНВЕЙЕР ДЛЯ ФИЛЬМОВ (MovieCodes_IMDB.tsv) ---
    public async Task LoadMoviesAsync(string path, ConcurrentDictionary<int, Movie> movies)
    {
        var stopwatch = Stopwatch.StartNew();
        Console.WriteLine("-> Starting: Load Movies...");
        
        var lines = new BlockingCollection<string>(5000);
        var filtered = new BlockingCollection<string>(5000);
        var parsed = new BlockingCollection<(int id, string title)>(5000);

        var readTask = Task.Run(() => ReadFileLines(path, lines));
        var filterTask = Task.Run(() => FilterMovieLines(lines, filtered));
        var parseTask = Task.Run(() => ParseMovieLines(filtered, parsed));
        var appendTask = AppendMoviesAsync(parsed, movies);
        
        await Task.WhenAll(readTask, filterTask, parseTask, appendTask);
        
        stopwatch.Stop();
        Console.WriteLine($"   Completed: Movies loaded in {stopwatch.ElapsedMilliseconds} ms. Found: {movies.Count}");
    }
    
    // --- КОНВЕЙЕР ДЛЯ ИМЕН (ActorsDirectorsNames_IMDB.txt) ---
    public async Task LoadNamesAsync(string path, ConcurrentDictionary<int, Person> people)
    {
        var stopwatch = Stopwatch.StartNew();
        Console.WriteLine("-> Starting: Load People Names...");
        
        var lines = new BlockingCollection<string>(10000);
        var parsed = new BlockingCollection<(int id, Person person)>(10000);

        var readTask = Task.Run(() => ReadFileLines(path, lines));
        var parseTask = Task.Run(() => ParsePersonLines(lines, parsed));
        var appendTask = AppendPersonNamesAsync(parsed, people);
        
        await Task.WhenAll(readTask, parseTask, appendTask);
        
        stopwatch.Stop();
        Console.WriteLine($"   Completed: People Names loaded in {stopwatch.ElapsedMilliseconds} ms");
    }

    // --- КОНВЕЙЕР ДЛЯ СВЯЗЕЙ (ActorsDirectorsCodes_IMDB.tsv) ---
    public async Task LinkPeopleToMoviesAsync(string path, ConcurrentDictionary<int, Movie> movies, ConcurrentDictionary<int, Person> people)
    {
        var stopwatch = Stopwatch.StartNew();
        Console.WriteLine("-> Starting: Link People to Movies...");
        
        var lines = new BlockingCollection<string>(10000);
        var filtered = new BlockingCollection<string>(10000);
        var parsed = new BlockingCollection<(int movieId, int personId, string category)>(10000);
        
        var readTask = Task.Run(() => ReadFileLines(path, lines));
        var filterTask = Task.Run(() => FilterLinkLines(lines, filtered));
        var parseTask = Task.Run(() => ParseLinkLines(filtered, parsed));
        var appendTask = AppendLinksAsync(parsed, movies, people);
        
        await Task.WhenAll(readTask, filterTask, parseTask, appendTask);
        
        stopwatch.Stop();
        Console.WriteLine($"   Completed: Links created in {stopwatch.ElapsedMilliseconds} ms");
    }
    
    // --- КОНВЕЙЕР ДЛЯ РЕЙТИНГОВ (Ratings_IMDB.tsv) ---
    public async Task LoadRatingsAsync(string path, ConcurrentDictionary<int, Movie> movies)
    {
        var stopwatch = Stopwatch.StartNew();
        Console.WriteLine("-> Starting: Load Ratings...");
        
        var lines = new BlockingCollection<string>(5000);
        var parsed = new BlockingCollection<(int movieId, float rating)>(5000);

        var readTask = Task.Run(() => ReadFileLines(path, lines));
        var parseTask = Task.Run(() => ParseRatingLines(lines, parsed));
        var appendTask = AppendRatingsAsync(parsed, movies);
        
        await Task.WhenAll(readTask, parseTask, appendTask);
        
        stopwatch.Stop();
        Console.WriteLine($"   Completed: Ratings loaded in {stopwatch.ElapsedMilliseconds} ms");
    }
    
    // --- КОНВЕЙЕР ДЛЯ MOVIELENS ЛИНКОВ (links_IMDB_MovieLens.csv) ---
    public async Task LoadMovieLensLinksAsync(string path, ConcurrentDictionary<int, int> links)
    {
        var stopwatch = Stopwatch.StartNew();
        Console.WriteLine("-> Starting: Load MovieLens Links...");
        
        var lines = new BlockingCollection<string>(1000);
        var parsed = new BlockingCollection<(int movieLensId, int imdbId)>(1000);
        
        var readTask = Task.Run(() => ReadFileLines(path, lines));
        var parseTask = Task.Run(() => ParseMovieLensLinkLines(lines, parsed));
        var appendTask = AppendMovieLensLinksAsync(parsed, links);
        
        await Task.WhenAll(readTask, parseTask, appendTask);
        
        stopwatch.Stop();
        Console.WriteLine($"   Completed: MovieLens Links loaded in {stopwatch.ElapsedMilliseconds} ms");
    }
    
    // --- КОНВЕЙЕР ДЛЯ НАЗВАНИЙ ТЕГОВ (TagCodes_MovieLens.csv) ---
    public async Task LoadTagNamesAsync(string path, ConcurrentDictionary<int, string> tagNames)
    {
        var stopwatch = Stopwatch.StartNew();
        Console.WriteLine("-> Starting: Load Tag Names...");
        
        var lines = new BlockingCollection<string>(1000);
        var parsed = new BlockingCollection<(int tagId, string tagName)>(1000);
        
        var readTask = Task.Run(() => ReadFileLines(path, lines));
        var parseTask = Task.Run(() => ParseTagCodeLines(lines, parsed));
        var appendTask = AppendTagNamesAsync(parsed, tagNames);

        await Task.WhenAll(readTask, parseTask, appendTask);
        
        stopwatch.Stop();
        Console.WriteLine($"   Completed: Tag Names loaded in {stopwatch.ElapsedMilliseconds} ms");
    }
    
    // --- КОНВЕЙЕР ДЛЯ ТЕГОВ ФИЛЬМОВ (TagScores_MovieLens.csv) ---
    public async Task LinkTagsToMoviesAsync(string path, ConcurrentDictionary<int, Movie> movies, ConcurrentDictionary<int, int> links, ConcurrentDictionary<int, string> tagNames)
    {
        var stopwatch = Stopwatch.StartNew();
        Console.WriteLine("-> Starting: Link Tags to Movies...");
        
        var lines = new BlockingCollection<string>(10000);
        var filtered = new BlockingCollection<string>(10000);
        var parsed = new BlockingCollection<(int movieLensId, int tagId)>(10000);
        
        var readTask = Task.Run(() => ReadFileLines(path, lines));
        var filterTask = Task.Run(() => FilterTagScoreLines(lines, filtered));
        var parseTask = Task.Run(() => ParseTagScoreLines(filtered, parsed));
        var appendTask = AppendMovieTagsAsync(parsed, movies, links, tagNames);
        
        await Task.WhenAll(readTask, filterTask, parseTask, appendTask);
        
        stopwatch.Stop();
        Console.WriteLine($"   Completed: Movie Tags linked in {stopwatch.ElapsedMilliseconds} ms");
    }

    #region Pipeline Stages
    
    private void ReadFileLines(string path, BlockingCollection<string> lines)
    {
        using var reader = new StreamReader(path);
        reader.ReadLine();
        string? line;
        while ((line = reader.ReadLine()) != null) lines.Add(line);
        lines.CompleteAdding();
    }

    private void FilterMovieLines(BlockingCollection<string> input, BlockingCollection<string> output)
    {
        foreach (var line in input.GetConsumingEnumerable())
        {
            try
            {
                int tab3 = -1, tab4 = -1;
                int currentTab = 0;
                for(int i = 0; i < line.Length; i++) {
                    if(line[i] == '\t') {
                        currentTab++;
                        if(currentTab == 3) tab3 = i;
                        if(currentTab == 4) { tab4 = i; break; }
                    }
                }
                if(tab4 == -1) continue;

                var regionSpan = line.AsSpan(tab3 + 1, 2);
                var languageSpan = line.AsSpan(tab4 + 1, 2);

                if (regionSpan.Equals("us", StringComparison.OrdinalIgnoreCase) || regionSpan.Equals("ru", StringComparison.OrdinalIgnoreCase) ||
                    languageSpan.Equals("EN", StringComparison.OrdinalIgnoreCase) || languageSpan.Equals("RU", StringComparison.OrdinalIgnoreCase))
                {
                    output.Add(line);
                }
            } catch {}
        }
        output.CompleteAdding();
    }
    
    private void FilterLinkLines(BlockingCollection<string> input, BlockingCollection<string> output)
    {
        foreach (var line in input.GetConsumingEnumerable())
        {
            try
            {
                int tab3 = -1, tab4 = -1;
                int currentTab = 0;
                for(int i = 0; i < line.Length; i++) {
                    if(line[i] == '\t') {
                        currentTab++;
                        if(currentTab == 3) tab3 = i;
                        if(currentTab == 4) { tab4 = i; break; }
                    }
                }
                if(tab4 == -1) continue;

                var role = line.AsSpan(tab3 + 1, tab4 - tab3 - 1);
                if (role.Equals("director", StringComparison.OrdinalIgnoreCase) || 
                    role.Equals("actor", StringComparison.OrdinalIgnoreCase) || 
                    role.Equals("actress", StringComparison.OrdinalIgnoreCase))
                    output.Add(line);
            } catch {}
        }
        output.CompleteAdding();
    }
    
    private void FilterTagScoreLines(BlockingCollection<string> input, BlockingCollection<string> output)
    {
        foreach (var line in input.GetConsumingEnumerable())
        {
            int lastComma = line.LastIndexOf(',');
            if (lastComma == -1) continue;
            if (float.TryParse(line.AsSpan(lastComma + 1), NumberStyles.Any, CultureInfo.InvariantCulture, out float relevance) && relevance > 0.5f)
                output.Add(line);
        }
        output.CompleteAdding();
    }

    private void ParseMovieLines(BlockingCollection<string> input, BlockingCollection<(int id, string title)> output)
    {
        foreach (var line in input.GetConsumingEnumerable())
            if (HardcodedParsers.TryParseMovieLine(line, out var p)) output.Add(p);
        output.CompleteAdding();
    }

    private void ParsePersonLines(BlockingCollection<string> input, BlockingCollection<(int id, Person person)> output)
    {
        foreach (var line in input.GetConsumingEnumerable())
            if (HardcodedParsers.TryParsePersonLine(line, out var p)) output.Add(p);
        output.CompleteAdding();
    }
    
    private void ParseLinkLines(BlockingCollection<string> input, BlockingCollection<(int movieId, int personId, string category)> output)
    {
        foreach (var line in input.GetConsumingEnumerable())
            if (HardcodedParsers.TryParseLinkLine(line, out var p)) output.Add(p);
        output.CompleteAdding();
    }
    
    private void ParseRatingLines(BlockingCollection<string> input, BlockingCollection<(int movieId, float rating)> output)
    {
        foreach (var line in input.GetConsumingEnumerable())
            if (HardcodedParsers.TryParseRatingLine(line, out var p)) output.Add(p);
        output.CompleteAdding();
    }
    
    private void ParseMovieLensLinkLines(BlockingCollection<string> input, BlockingCollection<(int movieLensId, int imdbId)> output)
    {
        foreach (var line in input.GetConsumingEnumerable())
            if (HardcodedParsers.TryParseMovieLensLinkLine(line, out var p)) output.Add(p);
        output.CompleteAdding();
    }
    
    private void ParseTagCodeLines(BlockingCollection<string> input, BlockingCollection<(int tagId, string tagName)> output)
    {
        foreach (var line in input.GetConsumingEnumerable())
            if (HardcodedParsers.TryParseTagCodeLine(line, out var p)) output.Add(p);
        output.CompleteAdding();
    }
    
    private void ParseTagScoreLines(BlockingCollection<string> input, BlockingCollection<(int movieLensId, int tagId)> output)
    {
        foreach (var line in input.GetConsumingEnumerable())
            if (HardcodedParsers.TryParseTagScoreLine(line, out var p)) output.Add(p);
        output.CompleteAdding();
    }
    
    private Task AppendMoviesAsync(BlockingCollection<(int id, string title)> input, ConcurrentDictionary<int, Movie> movies)
    {
        var tasks = new Task[Environment.ProcessorCount];
        for (int i = 0; i < tasks.Length; i++) {
            tasks[i] = Task.Run(() => {
                foreach (var (id, title) in input.GetConsumingEnumerable())
                    movies.TryAdd(id, new Movie { ID = id, Title = title, ImdbID = "tt" + id.ToString("D7") });
            });
        }
        return Task.WhenAll(tasks);
    }
    
    private Task AppendPersonNamesAsync(BlockingCollection<(int id, Person person)> input, ConcurrentDictionary<int, Person> people)
    {
        var tasks = new Task[Environment.ProcessorCount];
        for (int i = 0; i < tasks.Length; i++) {
            tasks[i] = Task.Run(() => {
                foreach (var (id, personData) in input.GetConsumingEnumerable())
                    people.AddOrUpdate(id, personData, (key, existing) => {
                        existing.FirstName = personData.FirstName;
                        existing.LastName = personData.LastName;
                        existing.BirthYear = personData.BirthYear;
                        existing.DeathYear = personData.DeathYear;
                        return existing;
                    });
            });
        }
        return Task.WhenAll(tasks);
    }

    private Task AppendLinksAsync(BlockingCollection<(int movieId, int personId, string category)> input, ConcurrentDictionary<int, Movie> movies, ConcurrentDictionary<int, Person> people)
    {
        var tasks = new Task[Environment.ProcessorCount];
        for (int i = 0; i < tasks.Length; i++) {
            tasks[i] = Task.Run(() => {
                foreach (var (movieId, personId, category) in input.GetConsumingEnumerable())
                {
                    var person = people.GetOrAdd(personId, (id) => new Person { ID = id });
                    if (movies.TryGetValue(movieId, out var movie))
                    {
                        if (category.Equals("director", StringComparison.OrdinalIgnoreCase))
                        {
                            movie.Director = person;
                            lock (person.DirectedMovies) person.DirectedMovies.Add(movie);
                        }
                        else
                        {
                            lock (movie.Actors) movie.Actors.Add(person);
                            lock (person.ActedMovies) person.ActedMovies.Add(movie);
                        }
                    }
                }
            });
        }
        return Task.WhenAll(tasks);
    }
    
    private Task AppendRatingsAsync(BlockingCollection<(int movieId, float rating)> input, ConcurrentDictionary<int, Movie> movies)
    {
        var tasks = new Task[Environment.ProcessorCount];
        for (int i = 0; i < tasks.Length; i++) {
            tasks[i] = Task.Run(() => {
                foreach (var (movieId, rating) in input.GetConsumingEnumerable())
                    if (movies.TryGetValue(movieId, out var movie)) movie.Rating = rating;
            });
        }
        return Task.WhenAll(tasks);
    }
    
    private Task AppendMovieLensLinksAsync(BlockingCollection<(int movieLensId, int imdbId)> input, ConcurrentDictionary<int, int> links)
    {
        var tasks = new Task[Environment.ProcessorCount];
        for (int i = 0; i < tasks.Length; i++) {
            tasks[i] = Task.Run(() => {
                foreach (var (movieLensId, imdbId) in input.GetConsumingEnumerable())
                    links.TryAdd(movieLensId, imdbId);
            });
        }
        return Task.WhenAll(tasks);
    }
    
    private Task AppendTagNamesAsync(BlockingCollection<(int tagId, string tagName)> input, ConcurrentDictionary<int, string> tagNames)
    {
        var tasks = new Task[Environment.ProcessorCount];
        for (int i = 0; i < tasks.Length; i++) {
            tasks[i] = Task.Run(() => {
                foreach (var (tagId, tagName) in input.GetConsumingEnumerable())
                    tagNames.TryAdd(tagId, tagName);
            });
        }
        return Task.WhenAll(tasks);
    }
    
    private Task AppendMovieTagsAsync(BlockingCollection<(int movieLensId, int tagId)> input, ConcurrentDictionary<int, Movie> movies, ConcurrentDictionary<int, int> links, ConcurrentDictionary<int, string> tagNames)
    {
        var tasks = new Task[Environment.ProcessorCount];
        for (int i = 0; i < tasks.Length; i++) {
            tasks[i] = Task.Run(() => {
                foreach (var (movieLensId, tagId) in input.GetConsumingEnumerable())
                    if (links.TryGetValue(movieLensId, out int imdbId) &&
                        movies.TryGetValue(imdbId, out var movie) &&
                        tagNames.TryGetValue(tagId, out var tagName))
                            lock(movie.Tags) movie.Tags.Add(tagName);
            });
        }
        return Task.WhenAll(tasks);
    }

    #endregion
}