using IMDbApplication.Models;
using IMDbApplication.Readers;
using System.Diagnostics;

namespace IMDbApplication.Services;

public class DataProcessor
{
    public static (Dictionary<string, Movie> movies, 
                   Dictionary<string, HashSet<Movie>> peopleToMovies, 
                   Dictionary<string, HashSet<Movie>> tagsToMovies) 
        LoadAllData(string movieCodesPath, string actorNamesPath, string actorCodesPath, 
                   string ratingsPath, string linksPath, string tagCodesPath, string tagScoresPath)
    {
        var totalStopwatch = Stopwatch.StartNew();
        Console.WriteLine("=== START DATA LOADING WITH PIPELINES ===\n");
        Console.WriteLine($"System: {Environment.ProcessorCount} processor cores available\n");
        
        Console.WriteLine("1. Loading movies...");
        var movies = MoviesReader.LoadMovies(movieCodesPath);
        
        Console.WriteLine("\n2. Loading people info...");
        var peopleIndex = PeopleReader.LoadPeople(actorNamesPath);
        
        Console.WriteLine("\n3. Establishing connections actors/directors with movies...");
        PeopleReader.LinkPeopleToMovies(movies, peopleIndex, actorCodesPath);
        
        Console.WriteLine("\n4. Loading movie ratings...");
        RatingsReader.LoadRatings(movies, ratingsPath);
        
        Console.WriteLine("\n5. Processing tags...");
        var movieLensToImdb = TagsReader.LoadLinks(linksPath);
        var tagsIndex = TagsReader.LoadTagNames(tagCodesPath);
        TagsReader.ProcessTagScores(movies, movieLensToImdb, tagsIndex, tagScoresPath);

        Console.WriteLine("\n6. Creating indexes...");
        var peopleToMovies = CreatePeopleToMoviesIndex(movies, peopleIndex);
        var tagsToMovies = CreateTagsToMoviesIndex(movies);

        Console.WriteLine("\n=== COMPLETE ===");
        Console.WriteLine($"Total execution time: {totalStopwatch.ElapsedMilliseconds} ms ({totalStopwatch.ElapsedMilliseconds/1000} sec)");
        Console.WriteLine($"Total: {movies.Count} movies, {peopleToMovies.Count} people, {tagsToMovies.Count} tags\n");
        
        return (movies, peopleToMovies, tagsToMovies);
    }
    

    private static Dictionary<string, HashSet<Movie>> CreatePeopleToMoviesIndex(
        Dictionary<string, Movie> movies, Dictionary<string, Person> peopleIndex)
    {
        var peopleToMovies = new Dictionary<string, HashSet<Movie>>();
        
        foreach (var person in peopleIndex.Values)
        {
            var allMovies = new HashSet<Movie>();
            
            foreach (var movie in person.ActedMovies)
                allMovies.Add(movie);
            
            foreach (var movie in person.DirectedMovies)
                allMovies.Add(movie);
            
            if (allMovies.Count > 0)
                peopleToMovies[person.FullName] = allMovies;
        }
        
        return peopleToMovies;
    }
    
    private static Dictionary<string, HashSet<Movie>> CreateTagsToMoviesIndex(Dictionary<string, Movie> movies)
    {
        var tagsToMovies = new Dictionary<string, HashSet<Movie>>();
        
        foreach (var movie in movies.Values)
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