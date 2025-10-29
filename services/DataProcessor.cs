using IMDbApplication.Models;
using System.Collections.Concurrent;
using System.Diagnostics;

namespace IMDbApplication.Services;

public class DataProcessor
{
    public static async Task<(
        Dictionary<string, Movie> movies, 
        Dictionary<string, HashSet<Movie>> peopleToMovies, 
        Dictionary<string, HashSet<Movie>> tagsToMovies
    )> LoadAllData(
        string movieCodesPath, string actorNamesPath, string actorCodesPath, 
        string ratingsPath, string linksPath, string tagCodesPath, string tagScoresPath)
    {
        var totalStopwatch = Stopwatch.StartNew();
        var provider = new DataProvider();

        var movies = new ConcurrentDictionary<int, Movie>();
        var people = new ConcurrentDictionary<int, Person>();
        var movieLensLinks = new ConcurrentDictionary<int, int>();
        var tagNames = new ConcurrentDictionary<int, string>();

        Console.WriteLine("\n--- PHASE 1: Loading Base Data & Links ---");
        var moviesTask = provider.LoadMoviesAsync(movieCodesPath, movies);
        var linksTask = provider.LinkPeopleToMoviesAsync(actorCodesPath, movies, people);
        var movieLensLinksTask = provider.LoadMovieLensLinksAsync(linksPath, movieLensLinks);
        var tagNamesTask = provider.LoadTagNamesAsync(tagCodesPath, tagNames);
        await Task.WhenAll(moviesTask, linksTask, movieLensLinksTask, tagNamesTask);

        Console.WriteLine("\n--- PHASE 2: Loading Dependent Data ---");
        var namesTask = provider.LoadNamesAsync(actorNamesPath, people);
        var ratingsTask = provider.LoadRatingsAsync(ratingsPath, movies);
        var movieTagsTask = provider.LinkTagsToMoviesAsync(tagScoresPath, movies, movieLensLinks, tagNames);
        await Task.WhenAll(namesTask, ratingsTask, movieTagsTask);
        
        totalStopwatch.Stop();
        Console.WriteLine($"\n=== DATA LOADING COMPLETED IN {totalStopwatch.ElapsedMilliseconds} MS ===");
        
        Console.WriteLine("\nCreating search indexes...");
        var indexStopwatch = Stopwatch.StartNew();
        
        var finalMovies = movies.Values.ToDictionary(m => m.ImdbID);
        var peopleToMovies = CreatePeopleToMoviesIndex(people.Values);
        var tagsToMovies = CreateTagsToMoviesIndex(finalMovies.Values);
        
        indexStopwatch.Stop();
        Console.WriteLine($"Indexes created in {indexStopwatch.ElapsedMilliseconds} ms");

        return (finalMovies, peopleToMovies, tagsToMovies);
    }
    
    private static Dictionary<string, HashSet<Movie>> CreatePeopleToMoviesIndex(IEnumerable<Person> people)
    {
        var peopleToMovies = new Dictionary<string, HashSet<Movie>>();
        foreach (var person in people.Where(p => !string.IsNullOrEmpty(p.FullName)))
        {
            var allMovies = new HashSet<Movie>(person.ActedMovies.Concat(person.DirectedMovies));
            if (allMovies.Count > 0)
                peopleToMovies[person.FullName] = allMovies;
        }
        return peopleToMovies;
    }
    
    private static Dictionary<string, HashSet<Movie>> CreateTagsToMoviesIndex(IEnumerable<Movie> movies)
    {
        var tagsToMovies = new Dictionary<string, HashSet<Movie>>();
        foreach (var movie in movies)
        {
            foreach (var tag in movie.Tags)
            {
                if (!tagsToMovies.ContainsKey(tag))
                    tagsToMovies[tag] = new HashSet<Movie>();
                tagsToMovies[tag].Add(movie);
            }
        }
        return tagsToMovies;
    }
}