using IMDbApplication.Models;
using System.Collections.Concurrent;
using System.Globalization;

namespace IMDbApplication.Services;

public class Processor
{
    public ConcurrentDictionary<int, Movie> Movies { get; } = new();
    public ConcurrentDictionary<int, Person> People { get; } = new();
    public ConcurrentDictionary<int, Tag> Tags { get; } = new();
    public ConcurrentDictionary<int, int> MovieLensLinks { get; } = new();

    #region Конвейеры
    
    public Task ProcessMoviesAsync(string path)
    {
        var raw = new BlockingCollection<string>(10000);
        var filtered = new BlockingCollection<string>(10000);
        var parsed = new BlockingCollection<(int id, string title)>(10000);

        var read = Task.Run(() => ReadFile(path, raw));
        var filter = Task.Run(() => FilterMovieLines(raw, filtered));
        var parse = Task.Run(() => ParseMovieLines(filtered, parsed));
        var process = Task.Run(() => ProcessMovieLines(parsed, Movies));

        return Task.WhenAll(read, filter, parse, process);
    }
    
    public Task ProcessPeopleAsync(string path)
    {
        var raw = new BlockingCollection<string>(10000);
        var parsed = new BlockingCollection<(int id, string name)>(10000);

        var read = Task.Run(() => ReadFile(path, raw));
        
        var parseTasks = Enumerable.Range(0, Environment.ProcessorCount)
            .Select(_ => Task.Run(() => ParsePeopleLines(raw, parsed))).ToArray();
        
        var processingCompleted = Task.WhenAll(parseTasks).ContinueWith(_ => parsed.CompleteAdding());

        var processTasks = Enumerable.Range(0, Environment.ProcessorCount)
            .Select(_ => Task.Run(() => ProcessPeopleLines(parsed, People))).ToArray();

        return Task.WhenAll(new[] { read }.Concat(parseTasks).Concat(new[] { processingCompleted }).Concat(processTasks));
    }
    
    public Task ProcessRatingsAsync(string path)
    {
        var raw = new BlockingCollection<string>(10000);
        var parsed = new BlockingCollection<(int id, float rating)>(10000);

        var read = Task.Run(() => ReadFile(path, raw));
        var parse = Task.Run(() => ParseRatingLines(raw, parsed));
        var process = Task.Run(() => ProcessRatingLines(parsed, Movies));

        return Task.WhenAll(read, parse, process);
    }
    
    public Task ProcessMovieLensLinksAsync(string path)
    {
        var raw = new BlockingCollection<string>(10000);
        var parsed = new BlockingCollection<(int movieLensId, int imdbId)>(10000);

        var read = Task.Run(() => ReadFile(path, raw));
        var parse = Task.Run(() => ParseMovieLensLinkLines(raw, parsed));
        var process = Task.Run(() => ProcessMovieLensLinkLines(parsed, MovieLensLinks));

        return Task.WhenAll(read, parse, process);
    }

    public Task ProcessTagsAsync(string path)
    {
        var raw = new BlockingCollection<string>(10000);
        var parsed = new BlockingCollection<(int id, string name)>(10000);

        var read = Task.Run(() => ReadFile(path, raw));
        var parse = Task.Run(() => ParseTagLines(raw, parsed));
        var process = Task.Run(() => ProcessTagLines(parsed, Tags));

        return Task.WhenAll(read, parse, process);
    }
    
    public Task ProcessTagScoresAsync(string path)
    {
        var raw = new BlockingCollection<string>(10000);
        var filtered = new BlockingCollection<string>(10000);
        var parsed = new BlockingCollection<(int movieId, int tagId)>(10000);

        var read = Task.Run(() => ReadFile(path, raw));
        var filter = Task.Run(() => FilterTagScoreLines(raw, filtered));
        var parse = Task.Run(() => ParseTagScoreLines(filtered, parsed));
        var process = ProcessTagScoreLinesAsync(parsed, Movies, Tags, MovieLensLinks);

        return Task.WhenAll(read, filter, parse, process);
    }
    
    public Task ProcessActorLinksAsync(string path)
    {
        var raw = new BlockingCollection<string>(10000);
        var filtered = new BlockingCollection<string>(10000);
        var parsed = new BlockingCollection<(int movieId, int personId, string category)>(10000);

        var read = Task.Run(() => ReadFile(path, raw));
        var filter = Task.Run(() => FilterActorLinkLines(raw, filtered));
        var parse = Task.Run(() => ParseActorLinkLines(filtered, parsed));
        var process = ProcessActorLinkLinesAsync(parsed, Movies, People);

        return Task.WhenAll(read, filter, parse, process);
    }

    #endregion

    #region Этапы конвейера
    
    private void ReadFile(string path, BlockingCollection<string> output)
    {
        try {
            using var reader = new StreamReader(path);
            reader.ReadLine();
            while (reader.ReadLine() is { } line) output.Add(line);
        } finally { output.CompleteAdding(); }
    }
    
    private void FilterMovieLines(BlockingCollection<string> input, BlockingCollection<string> output)
    {
        try {
            foreach (var line in input.GetConsumingEnumerable()) {
                int firstTabIndex = line.IndexOf('\t');
                int secondTabIndex = line.IndexOf('\t', firstTabIndex + 1);
                int thirdTabIndex = line.IndexOf('\t', secondTabIndex + 1);
                int tab4 = line.IndexOf('\t', thirdTabIndex + 1);

                string region = line.Substring(thirdTabIndex + 1, 2).ToLower();
                string language = line.Substring(tab4 + 1, 2).ToUpper();
                bool isSuitable = (region == "us" || region == "ru") || (language == "RU" || language == "EN");
                
                if (isSuitable) {
                    output.Add(line);
                }
            }
        } finally { output.CompleteAdding(); }
    }

    private void FilterTagScoreLines(BlockingCollection<string> input, BlockingCollection<string> output)
    {
        try {
            foreach (var line in input.GetConsumingEnumerable()) {
                int lastComma = line.LastIndexOf(',');
                if (lastComma != -1 && float.TryParse(line.AsSpan(lastComma + 1), NumberStyles.Any, CultureInfo.InvariantCulture, out float relevance) && relevance > 0.5f)
                    output.Add(line);
            }
        } finally { output.CompleteAdding(); }
    }

    private void FilterActorLinkLines(BlockingCollection<string> input, BlockingCollection<string> output)
    {
        try {
            foreach (var line in input.GetConsumingEnumerable()) {
                int tab3 = -1, tab4 = -1;
                int currentTab = 0;
                for (int i = 0; i < line.Length; i++) {
                    if (line[i] == '\t') {
                        currentTab++;
                        if (currentTab == 3) tab3 = i;
                        if (currentTab == 4) { tab4 = i; break; }
                    }
                }
                if (tab4 == -1) continue;
                var role = line.AsSpan(tab3 + 1, tab4 - tab3 - 1);
                if (role.Equals("director", StringComparison.OrdinalIgnoreCase) || 
                    role.Equals("actor", StringComparison.OrdinalIgnoreCase) || 
                    role.Equals("actress", StringComparison.OrdinalIgnoreCase))
                    output.Add(line);
            }
        } finally { output.CompleteAdding(); }
    }

    private void ParseMovieLines(BlockingCollection<string> input, BlockingCollection<(int id, string title)> output)
    {
        try {
            foreach (var line in input.GetConsumingEnumerable()) {
                int id = int.Parse(line.AsSpan(2, 7));
                int firstTabIndex = line.IndexOf('\t', 10);
                string substringAfterFirstTab = line.Substring(firstTabIndex + 1);
                int secondTabIndex = substringAfterFirstTab.IndexOf('\t');
                string title = substringAfterFirstTab.Substring(0, secondTabIndex);
                output.Add((id, title));
            }
        } finally { output.CompleteAdding(); }
    }

    private void ParsePeopleLines(BlockingCollection<string> input, BlockingCollection<(int id, string name)> output)
    {
        foreach (var line in input.GetConsumingEnumerable()) {
            try {
                int id = int.Parse(line.AsSpan(2, 7));
                int firstTabIndex = line.IndexOf('\t');
                int secondTabIndex = line.IndexOf('\t', firstTabIndex + 1);
                string name = line.Substring(firstTabIndex + 1, secondTabIndex - firstTabIndex - 1);
                output.Add((id, name));
            } catch {}
        }
    }
    
    private void ParseRatingLines(BlockingCollection<string> input, BlockingCollection<(int id, float rating)> output)
    {
        try {
            foreach (var line in input.GetConsumingEnumerable()) {
                int titleId = int.Parse(line.AsSpan(2, 7));
                float rating = float.Parse(line.AsSpan(10, 3), CultureInfo.InvariantCulture);
                output.Add((titleId, rating));
            }
        } finally { output.CompleteAdding(); }
    }

    private void ParseMovieLensLinkLines(BlockingCollection<string> input, BlockingCollection<(int movieLensId, int imdbId)> output)
    {
        try {
            foreach (var line in input.GetConsumingEnumerable()) {
                int firstCommaIndex = line.IndexOf(',');
                int movieId = int.Parse(line.AsSpan(0, firstCommaIndex));
                int secondCommaIndex = line.IndexOf(',', firstCommaIndex + 1);
                int imdbId = int.Parse(line.AsSpan(firstCommaIndex + 1, secondCommaIndex - firstCommaIndex - 1));
                output.Add((movieId, imdbId));
            }
        } finally { output.CompleteAdding(); }
    }

    private void ParseTagLines(BlockingCollection<string> input, BlockingCollection<(int id, string name)> output)
    {
        try {
            foreach (var line in input.GetConsumingEnumerable()) {
                int firstCommaIndex = line.IndexOf(',');
                int tagId = int.Parse(line.AsSpan(0, firstCommaIndex));
                string tag = line.Substring(firstCommaIndex + 1);
                output.Add((tagId, tag));
            }
        } finally { output.CompleteAdding(); }
    }

    private void ParseTagScoreLines(BlockingCollection<string> input, BlockingCollection<(int movieId, int tagId)> output)
    {
        try {
            foreach (var line in input.GetConsumingEnumerable()) {
                int firstCommaIndex = line.IndexOf(',');
                int secondCommaIndex = line.IndexOf(',', firstCommaIndex + 1);
                int movieId = int.Parse(line.AsSpan(0, firstCommaIndex));
                int tagId = int.Parse(line.AsSpan(firstCommaIndex + 1, secondCommaIndex - firstCommaIndex - 1));
                output.Add((movieId, tagId));
            }
        } finally { output.CompleteAdding(); }
    }

    private void ParseActorLinkLines(BlockingCollection<string> input, BlockingCollection<(int movieId, int personId, string category)> output)
    {
        try {
            foreach (var line in input.GetConsumingEnumerable()) {
                int titleId = int.Parse(line.AsSpan(2, 7));
                string remainingPart = line.Substring(10);
                int tabIndex = remainingPart.IndexOf('\t');
                string afterTab = remainingPart.Substring(tabIndex + 1);
                int humanId = int.Parse(afterTab.AsSpan(2, 7));
                string category = afterTab.Substring(10, 1) == "d" ? "director" : "actor";
                output.Add((titleId, humanId, category));
            }
        } finally { output.CompleteAdding(); }
    }
    
    private void ProcessMovieLines(BlockingCollection<(int, string)> input, ConcurrentDictionary<int, Movie> output)
    {
        foreach (var (id, title) in input.GetConsumingEnumerable()) {
            output.TryAdd(id, new Movie { ID = id, Title = title });
        }
    }
    
    private void ProcessPeopleLines(BlockingCollection<(int, string)> input, ConcurrentDictionary<int, Person> output)
    {
        foreach (var (id, name) in input.GetConsumingEnumerable()) {
            output.TryAdd(id, new Person { ID = id, Name = name });
        }
    }

    private void ProcessRatingLines(BlockingCollection<(int, float)> input, ConcurrentDictionary<int, Movie> output)
    {
        foreach (var (id, rating) in input.GetConsumingEnumerable()) {
            if (output.TryGetValue(id, out var movie))
                movie.Rating = rating;
        }
    }

    private void ProcessMovieLensLinkLines(BlockingCollection<(int, int)> input, ConcurrentDictionary<int, int> output)
    {
        foreach (var (movieLensId, imdbId) in input.GetConsumingEnumerable()) {
            output.TryAdd(movieLensId, imdbId);
        }
    }

    private void ProcessTagLines(BlockingCollection<(int, string)> input, ConcurrentDictionary<int, Tag> output)
    {
        foreach (var (id, name) in input.GetConsumingEnumerable()) {
            output.TryAdd(id, new Tag { ID = id, Name = name });
        }
    }

    private Task ProcessTagScoreLinesAsync(BlockingCollection<(int, int)> input, ConcurrentDictionary<int, Movie> movies, ConcurrentDictionary<int, Tag> tags, ConcurrentDictionary<int, int> links)
    {
        var tasks = Enumerable.Range(0, Environment.ProcessorCount).Select(_ => Task.Run(() => {
            foreach (var (movieId, tagId) in input.GetConsumingEnumerable()) {
                if (links.TryGetValue(movieId, out var imdbId) && movies.TryGetValue(imdbId, out var movie) && tags.TryGetValue(tagId, out var tag)) 
                {
                    lock (movie.Tags)
                    {
                        movie.Tags.Add(tag);
                    }
                }
            }
        })).ToArray();
        return Task.WhenAll(tasks);
    }

    private Task ProcessActorLinkLinesAsync(BlockingCollection<(int, int, string)> input, ConcurrentDictionary<int, Movie> movies, ConcurrentDictionary<int, Person> people)
    {
        var tasks = Enumerable.Range(0, Environment.ProcessorCount).Select(_ => Task.Run(() => {
            foreach (var (movieId, personId, category) in input.GetConsumingEnumerable()) {
                if (movies.TryGetValue(movieId, out var movie) && people.TryGetValue(personId, out var person)) 
                {
                    lock (person.Movies)
                    {
                        person.Movies.Add(movie);
                    }

                    if (category == "director") movie.Director = person.Name;

                    else
                    {
                        lock (movie.Actors)
                        {
                            movie.Actors.Add(person);
                        }
                    }
                }
            }
        })).ToArray();
        return Task.WhenAll(tasks);
    }
    
    #endregion
}