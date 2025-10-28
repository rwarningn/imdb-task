using IMDbApplication.Models;

namespace IMDbApplication.Services;

public class DataPrinter
{
    static string template = string.Concat(Enumerable.Repeat("=" , 50));  
    public static void PrintMovieInfo(string movieTitle, Dictionary<string, Movie> movies)
    { 
        var movie = movies.Values.FirstOrDefault(m => 
            m.Title.Equals(movieTitle, StringComparison.OrdinalIgnoreCase));
        
        if (movie == null)
        {
            Console.WriteLine($"Movie '{movieTitle}' is not found.");
            return;
        }
        
        Console.WriteLine($"\n{template}");
        Console.WriteLine($"MOVIE INFO");
        Console.WriteLine($"{template}"); 
        Console.WriteLine($"Title: {movie.Title}");
        Console.WriteLine($"IMDB ID: {movie.ImdbID}");
        Console.WriteLine($"Rating: {(movie.Rating > 0 ? movie.Rating.ToString("F1") + "/10" : "N/D")}");
        Console.WriteLine($"Director: {(string.IsNullOrEmpty(movie.Director) ? "N/D" : movie.Director)}");
        
        Console.WriteLine($"\nActors ({movie.Actors.Count}):");
        var actorsList = movie.Actors.Take(15).ToList();
        for (int i = 0; i < actorsList.Count; i++)
        {
            Console.WriteLine($"  {i + 1}. {actorsList[i]}");
        }
        if (movie.Actors.Count > 15)
            Console.WriteLine($"  ... and more {movie.Actors.Count - 15} actors");
        
        Console.WriteLine($"\nTags ({movie.Tags.Count}):");
        var tagsList = movie.Tags.Take(15).ToList();
        for (int i = 0; i < tagsList.Count; i++)
        {
            Console.WriteLine($"  {i + 1}. {tagsList[i]}");
        }
        if (movie.Tags.Count > 15)
            Console.WriteLine($"  ... and more {movie.Tags.Count - 15} tags");
    }
    
    public static void PrintPersonInfo(string personName, Dictionary<string, HashSet<Movie>> peopleToMovies)
    {
        if (!peopleToMovies.ContainsKey(personName))
        {
            Console.WriteLine($"Person '{personName}' is not found.");
            return;
        }
        
        var movies = peopleToMovies[personName];
        Console.WriteLine($"\n{template}");
        Console.WriteLine($"INFORMATION ABOUT {personName.ToUpper()}");
        Console.WriteLine($"{template}");
        Console.WriteLine($"Participated in {movies.Count} movies:");
        
        var sortedMovies = movies.OrderByDescending(m => m.Rating).Take(20).ToList();
        for (int i = 0; i < sortedMovies.Count; i++)
        {
            var movie = sortedMovies[i];
            string role = movie.Director == personName ? "Director" : "Actor";
            string ratingStr = movie.Rating > 0 ? movie.Rating.ToString("F1") : "N/D";
            Console.WriteLine($"  {i + 1}. {role}: {movie.Title} (Rating: {ratingStr})");
        }
        
        if (movies.Count > 20)
            Console.WriteLine($"  ... and more {movies.Count - 20} movies");
    }
    
    public static void PrintTagInfo(string tagName, Dictionary<string, HashSet<Movie>> tagsToMovies)
    {
        if (!tagsToMovies.ContainsKey(tagName))
        {
            Console.WriteLine($"Tag '{tagName}' is not found.");
            return;
        }
        
        var movies = tagsToMovies[tagName];
        Console.WriteLine($"\n{template}");
        Console.WriteLine($"MOVIES WITH TAG '{tagName.ToUpper()}'");
        Console.WriteLine($"{template}");
        Console.WriteLine($"Found {movies.Count} movies:");
        
        var sortedMovies = movies.OrderByDescending(m => m.Rating).Take(20).ToList();
        for (int i = 0; i < sortedMovies.Count; i++)
        {
            var movie = sortedMovies[i];
            string ratingStr = movie.Rating > 0 ? movie.Rating.ToString("F1") : "N/D";
            Console.WriteLine($"  {i + 1}. {movie.Title} (Rating: {ratingStr})");
        }
        
        if (movies.Count > 20)
            Console.WriteLine($"  ... and more {movies.Count - 20} movies");
    }
    
    public static void PrintStatistics(Dictionary<string, Movie> movies, 
                                     Dictionary<string, HashSet<Movie>> peopleToMovies, 
                                     Dictionary<string, HashSet<Movie>> tagsToMovies)
    {
        Console.WriteLine($"\n====================");
        Console.WriteLine($"GENERAL STATISTICS");
        Console.WriteLine($"====================");
        Console.WriteLine($"Total movies: {movies.Count}");
        Console.WriteLine($"movies with rating: {movies.Values.Count(m => m.Rating > 0)}");
        Console.WriteLine($"movies with director: {movies.Values.Count(m => !string.IsNullOrEmpty(m.Director))}");
        Console.WriteLine($"movies with actors: {movies.Values.Count(m => m.Actors.Count > 0)}");
        Console.WriteLine($"movies with tags: {movies.Values.Count(m => m.Tags.Count > 0)}");
        Console.WriteLine($"Total people: {peopleToMovies.Count}");
        Console.WriteLine($"Total unique tags: {tagsToMovies.Count}");
        
        if (movies.Values.Any(m => m.Rating > 0))
        {
            var avgRating = movies.Values.Where(m => m.Rating > 0).Average(m => m.Rating);
            Console.WriteLine($"Average rating: {avgRating:F2}");
        }
    }
}