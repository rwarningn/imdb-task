using IMDbApplication.Models;

namespace IMDbApplication.Services;

public static class DataPrinter
{
    public static void PrintStatistics(
        Dictionary<string, Movie> movies, 
        Dictionary<string, HashSet<Movie>> peopleToMovies, 
        Dictionary<string, HashSet<Movie>> tagsToMovies)
    {
        Console.WriteLine("\n====================");
        Console.WriteLine("GENERAL STATISTICS");
        Console.WriteLine("====================");
        Console.WriteLine($"Total movies: {movies.Count}");
        
        Console.WriteLine($"movies with rating: {movies.Values.Count(m => m.Rating > 0)}");
        Console.WriteLine($"movies with director: {movies.Values.Count(m => m.Director != null)}");
        Console.WriteLine($"movies with actors: {movies.Values.Count(m => m.Actors.Any())}");
        Console.WriteLine($"movies with tags: {movies.Values.Count(m => m.Tags.Any())}");
        
        Console.WriteLine($"Total people: {peopleToMovies.Count}");
        Console.WriteLine($"Total unique tags: {tagsToMovies.Count}");
        
        var avgRating = movies.Values.Where(m => m.Rating > 0).Average(m => m.Rating);
        Console.WriteLine($"Average rating: {avgRating:F2}");
    }

    public static void PrintMovieInfo(string title, Dictionary<string, Movie> movies)
    {
        var foundMovies = movies.Values
            .Where(m => m.Title.Contains(title, StringComparison.OrdinalIgnoreCase))
            .OrderByDescending(m => m.Rating)
            .Take(10)
            .ToList();
            
        if (!foundMovies.Any())
        {
            Console.WriteLine($"Movie '{title}' not found.");
            return;
        }
        
        Console.WriteLine($"Found {foundMovies.Count} movies for '{title}':");
        foreach (var movie in foundMovies)
        {
            Console.WriteLine("--------------------");
            Console.WriteLine($"Title: {movie.Title} ({movie.ImdbID})");
            Console.WriteLine($"Rating: {(movie.Rating > 0 ? movie.Rating.ToString("F1") : "N/A")}");
            
            Console.WriteLine($"Director: {(movie.Director != null ? movie.Director.FullName : "N/A")}");
            
            if (movie.Actors.Any())
            {
                var actorNames = movie.Actors.Select(a => a.FullName).OrderBy(n => n);
                Console.WriteLine($"Actors: {string.Join(", ", actorNames)}");
            }
            
            if (movie.Tags.Any())
            {
                Console.WriteLine($"Tags: {string.Join(", ", movie.Tags.OrderBy(t => t))}");
            }
        }
    }

    public static void PrintPersonInfo(string name, Dictionary<string, HashSet<Movie>> peopleToMovies)
    {
        var foundPerson = peopleToMovies
            .FirstOrDefault(p => p.Key.Equals(name, StringComparison.OrdinalIgnoreCase));
            
        if (foundPerson.Key == null)
        {
            Console.WriteLine($"Person '{name}' not found.");
            return;
        }
        
        Console.WriteLine($"Movies for '{foundPerson.Key}':");
        
        var personMovies = foundPerson.Value.OrderByDescending(m => m.Rating).ToList();
        
        foreach (var movie in personMovies)
        {
            string role = "Actor";
            if (movie.Director != null && movie.Director.FullName.Equals(foundPerson.Key, StringComparison.OrdinalIgnoreCase))
            {
                role = "Director";
            }
            
            Console.WriteLine($"  - {movie.Title} ({movie.Rating:F1}) as {role}");
        }
    }

    public static void PrintTagInfo(string tagName, Dictionary<string, HashSet<Movie>> tagsToMovies)
    {
        if (!tagsToMovies.TryGetValue(tagName, out var foundMovies))
        {
            var foundPair = tagsToMovies.FirstOrDefault(t => t.Key.Equals(tagName, StringComparison.OrdinalIgnoreCase));
            if (foundPair.Key != null)
            {
                foundMovies = foundPair.Value;
                tagName = foundPair.Key;
            }
            else
            {
                Console.WriteLine($"Tag '{tagName}' not found.");
                return;
            }
        }
        
        Console.WriteLine($"Top rated movies for tag '{tagName}':");
        
        var taggedMovies = foundMovies.OrderByDescending(m => m.Rating).Take(20).ToList();
        
        foreach (var movie in taggedMovies)
        {
            string director = movie.Director != null ? movie.Director.FullName : "N/A";
            string actors = movie.Actors.Any() ? $"{movie.Actors.First().FullName}, ..." : "N/A";
            
            Console.WriteLine($"  - {movie.Title} (Rating: {movie.Rating:F1}, Director: {director}, Actors: {actors})");
        }
    }
}