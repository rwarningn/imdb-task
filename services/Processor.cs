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
        var process = ProcessItemsAsync(parsed, (p) => Movies.TryAdd(p.id, new Movie { ID = p.id, Title = p.title }));

        return Task.WhenAll(read, filter, parse, process);
    }
    
    public Task ProcessPeopleAsync(string path)
    {
        var raw = new BlockingCollection<string>(10000);
        var parsed = new BlockingCollection<(int id, string name)>(10000);

        var read = Task.Run(() => ReadFile(path, raw));
        var parse = Task.Run(() => ParsePeopleLines(raw, parsed));
        var process = ProcessItemsAsync(parsed, (p) => People.TryAdd(p.id, new Person { ID = p.id, Name = p.name }));

        return Task.WhenAll(read, parse, process);
    }
    
    public Task ProcessRatingsAsync(string path)
    {
        var raw = new BlockingCollection<string>(10000);
        var parsed = new BlockingCollection<(int id, float rating)>(10000);

        var read = Task.Run(() => ReadFile(path, raw));
        var parse = Task.Run(() => ParseRatingLines(raw, parsed));
        var process = ProcessRatingsAsync(parsed);

        return Task.WhenAll(read, parse, process);
    }
    
    public Task ProcessMovieLensLinksAsync(string path)
    {
        var raw = new BlockingCollection<string>(10000);
        var parsed = new BlockingCollection<(int movieLensId, int imdbId)>(10000);

        var read = Task.Run(() => ReadFile(path, raw));
        var parse = Task.Run(() => ParseMovieLensLinkLines(raw, parsed));
        var process = ProcessMovieLensLinkLinesAsync(parsed);

        return Task.WhenAll(read, parse, process);
    }

    public Task ProcessTagsAsync(string path)
    {
        var raw = new BlockingCollection<string>(10000);
        var parsed = new BlockingCollection<(int id, string name)>(10000);

        var read = Task.Run(() => ReadFile(path, raw));
        var parse = Task.Run(() => ParseTagLines(raw, parsed));
        var process = ProcessTagLinesAsync(parsed);

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
        var process = ProcessTagScoreLinesAsync(parsed);

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
        var process = ProcessActorLinkLinesAsync(parsed);

        return Task.WhenAll(read, filter, parse, process);
    }

    #endregion

    #region Этапы конвейера
    
    private void ReadFile(string path, BlockingCollection<string> output)
    {
        try { using var reader = new StreamReader(path); reader.ReadLine(); while (reader.ReadLine() is { } line) output.Add(line); } finally { output.CompleteAdding(); }
    }
    
    private void FilterMovieLines(BlockingCollection<string> input, BlockingCollection<string> output)
    {
        try { foreach (var line in input.GetConsumingEnumerable()) {
            var parts = line.Split('\t');
            if (parts.Length < 5) continue;
            string region = parts[3];
            string language = parts[4];
            if (string.Equals(region, "us", StringComparison.OrdinalIgnoreCase) || string.Equals(region, "ru", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(language, "en", StringComparison.OrdinalIgnoreCase) || string.Equals(language, "ru", StringComparison.OrdinalIgnoreCase))
                output.Add(line);
        } } finally { output.CompleteAdding(); }
    }

    private void FilterTagScoreLines(BlockingCollection<string> input, BlockingCollection<string> output)
    {
        try { foreach (var line in input.GetConsumingEnumerable()) {
            var parts = line.Split(',');
            if (parts.Length < 3) continue;
            if (float.TryParse(parts[2], NumberStyles.Any, CultureInfo.InvariantCulture, out float relevance) && relevance > 0.5f)
                output.Add(line);
        } } finally { output.CompleteAdding(); }
    }

    private void FilterActorLinkLines(BlockingCollection<string> input, BlockingCollection<string> output)
    {
        try { foreach (var line in input.GetConsumingEnumerable()) {
            var parts = line.Split('\t');
            if (parts.Length < 4) continue;
            string category = parts[3];
            if (category == "actor" || category == "actress" || category == "director")
                output.Add(line);
        } } finally { output.CompleteAdding(); }
    }

    private void ParseMovieLines(BlockingCollection<string> input, BlockingCollection<(int id, string title)> output)
    {
        try { foreach (var line in input.GetConsumingEnumerable()) { try {
            var parts = line.Split('\t');
            output.Add((int.Parse(parts[0].AsSpan(2)), parts[2]));
        } catch {} } } finally { output.CompleteAdding(); }
    }

    private void ParsePeopleLines(BlockingCollection<string> input, BlockingCollection<(int id, string name)> output)
    {
        try { foreach (var line in input.GetConsumingEnumerable()) { try {
            var parts = line.Split('\t');
            output.Add((int.Parse(parts[0].AsSpan(2)), parts[1]));
        } catch {} } } finally { output.CompleteAdding(); }
    }
    
    private void ParseRatingLines(BlockingCollection<string> input, BlockingCollection<(int id, float rating)> output)
    {
        try { foreach (var line in input.GetConsumingEnumerable()) { try {
            var parts = line.Split('\t');
            output.Add((int.Parse(parts[0].AsSpan(2)), float.Parse(parts[1], CultureInfo.InvariantCulture)));
        } catch {} } } finally { output.CompleteAdding(); }
    }

    private void ParseMovieLensLinkLines(BlockingCollection<string> input, BlockingCollection<(int movieLensId, int imdbId)> output)
    {
        try { foreach (var line in input.GetConsumingEnumerable()) { try {
            var parts = line.Split(',');
            output.Add((int.Parse(parts[0]), int.Parse(parts[1])));
        } catch {} } } finally { output.CompleteAdding(); }
    }

    private void ParseTagLines(BlockingCollection<string> input, BlockingCollection<(int id, string name)> output)
    {
        try { foreach (var line in input.GetConsumingEnumerable()) { try {
            var parts = line.Split(',');
            output.Add((int.Parse(parts[0]), parts[1]));
        } catch {} } } finally { output.CompleteAdding(); }
    }

    private void ParseTagScoreLines(BlockingCollection<string> input, BlockingCollection<(int movieId, int tagId)> output)
    {
        try { foreach (var line in input.GetConsumingEnumerable()) { try {
            var parts = line.Split(',');
            output.Add((int.Parse(parts[0]), int.Parse(parts[1])));
        } catch {} } } finally { output.CompleteAdding(); }
    }

    private void ParseActorLinkLines(BlockingCollection<string> input, BlockingCollection<(int movieId, int personId, string category)> output)
    {
        try { foreach (var line in input.GetConsumingEnumerable()) { try {
            var parts = line.Split('\t');
            output.Add((int.Parse(parts[0].AsSpan(2)), int.Parse(parts[2].AsSpan(2)), parts[3]));
        } catch {} } } finally { output.CompleteAdding(); }
    }
    
    private Task ProcessItemsAsync<T>(BlockingCollection<T> input, Action<T> processAction)
    {
        var tasks = Enumerable.Range(0, Environment.ProcessorCount).Select(_ => Task.Run(() => {
            foreach (var item in input.GetConsumingEnumerable()) {
                processAction(item);
            }
        })).ToArray();
        return Task.WhenAll(tasks);
    }
    
    private Task ProcessMoviesAsync(BlockingCollection<(int id, string title)> input) => 
        ProcessItemsAsync(input, (p) => Movies.TryAdd(p.id, new Movie { ID = p.id, Title = p.title }));

    private Task ProcessPeopleAsync(BlockingCollection<(int id, string name)> input) => 
        ProcessItemsAsync(input, (p) => People.TryAdd(p.id, new Person { ID = p.id, Name = p.name }));
    
    private Task ProcessRatingsAsync(BlockingCollection<(int id, float rating)> input) => 
        ProcessItemsAsync(input, (p) => { if (Movies.TryGetValue(p.id, out var movie)) movie.Rating = p.rating; });

    private Task ProcessMovieLensLinkLinesAsync(BlockingCollection<(int movieLensId, int imdbId)> input) => 
        ProcessItemsAsync(input, (p) => MovieLensLinks.TryAdd(p.movieLensId, p.imdbId));

    private Task ProcessTagLinesAsync(BlockingCollection<(int id, string name)> input) => 
        ProcessItemsAsync(input, (p) => Tags.TryAdd(p.id, new Tag { ID = p.id, Name = p.name }));

    private Task ProcessTagScoreLinesAsync(BlockingCollection<(int movieId, int tagId)> input)
    {
        return ProcessItemsAsync(input, (p) => {
            if (MovieLensLinks.TryGetValue(p.movieId, out var imdbId) && Movies.TryGetValue(imdbId, out var movie) && Tags.TryGetValue(p.tagId, out var tag)) {
                lock (movie.Tags) movie.Tags.Add(tag);
                lock (tag.Movies) tag.Movies.Add(movie);
            }
        });
    }

    private Task ProcessActorLinkLinesAsync(BlockingCollection<(int movieId, int personId, string category)> input)
    {
        return ProcessItemsAsync(input, (p) => {
            if (Movies.TryGetValue(p.movieId, out var movie) && People.TryGetValue(p.personId, out var person)) {
                if (p.category == "director") {
                    movie.Director = person;
                    lock (person.DirectedMovies) person.DirectedMovies.Add(movie);
                } else {
                    lock (movie.Actors) movie.Actors.Add(person);
                    lock (person.ActedInMovies) person.ActedInMovies.Add(movie);
                }
            }
        });
    }
    
    #endregion
}