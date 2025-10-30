using IMDbApplication.Models;
using System.Linq;

namespace IMDbApplication.Services;

public static class DataPrinter
{
    private static readonly string Separator = new('=', 50);

    public static void PrintMovieInfo(string title, Dictionary<string, Movie> movies)
    {
        if (!movies.TryGetValue(title, out var movie))
        {
            Console.WriteLine($"Movie '{title}' is not found.");
            return;
        }

        Console.WriteLine($"\n{Separator}");
        Console.WriteLine("MOVIE INFO");
        Console.WriteLine(Separator);
        Console.WriteLine($"Title: {movie.Title}");
        Console.WriteLine($"IMDB ID: tt{movie.ID:D7}");
        Console.WriteLine($"Rating: {(movie.Rating > 0 ? movie.Rating.ToString("F1") + "/10" : "N/A")}");
        
        Console.WriteLine($"Director: {(movie.Director != null ? movie.Director.Name : "N/A")}");

        if (movie.Actors.Any())
        {
            Console.WriteLine($"\nActors ({movie.Actors.Count}):");
            var actorsList = movie.Actors.Select(a => a.Name).OrderBy(n => n).Take(15).ToList();
            for (int i = 0; i < actorsList.Count; i++)
            {
                Console.WriteLine($"  {i + 1}. {actorsList[i]}");
            }
            if (movie.Actors.Count > 15)
                Console.WriteLine($"  ... and {movie.Actors.Count - 15} more actors");
        }

        if (movie.Tags.Any())
        {
            Console.WriteLine($"\nTags ({movie.Tags.Count}):");
            var tagsList = movie.Tags.Select(t => t.Name).OrderBy(n => n).Take(15).ToList();
            for (int i = 0; i < tagsList.Count; i++)
            {
                Console.WriteLine($"  {i + 1}. {tagsList[i]}");
            }
            if (movie.Tags.Count > 15)
                Console.WriteLine($"  ... and {movie.Tags.Count - 15} more tags");
        }
    }

    public static void PrintPersonInfo(string name, Dictionary<string, Person> people)
    {
        if (!people.TryGetValue(name, out var person))
        {
            Console.WriteLine($"Person '{name}' is not found.");
            return;
        }
        
        var movies = person.GetAllMovies();
        Console.WriteLine($"\n{Separator}");
        Console.WriteLine($"INFORMATION ABOUT {person.Name.ToUpper()}");
        Console.WriteLine(Separator);
        Console.WriteLine($"Participated in {movies.Count()} movies:");

        var sortedMovies = movies.OrderByDescending(m => m.Rating).Take(20).ToList();
        for (int i = 0; i < sortedMovies.Count; i++)
        {
            var movie = sortedMovies[i];
            string role = (movie.Director != null && movie.Director.ID == person.ID) ? "Director" : "Actor";
            string ratingStr = movie.Rating > 0 ? movie.Rating.ToString("F1") : "N/A";
            Console.WriteLine($"  {i + 1}. {role}: {movie.Title} (Rating: {ratingStr})");
        }

        if (movies.Count() > 20)
            Console.WriteLine($"  ... and {movies.Count() - 20} more movies");
    }

    public static void PrintTagInfo(string name, Dictionary<string, List<Movie>> tagsToMovies)
    {
        if (!tagsToMovies.TryGetValue(name, out var movies))
        {
            Console.WriteLine($"Tag '{name}' not found.");
            return;
        }

        Console.WriteLine($"\n{Separator}");
        Console.WriteLine($"MOVIES WITH TAG '{name.ToUpper()}'");
        Console.WriteLine(Separator);
        Console.WriteLine($"Found {movies.Count} movies:");

        var sortedMovies = movies.OrderByDescending(m => m.Rating).Take(20).ToList();
        for (int i = 0; i < sortedMovies.Count; i++)
        {
            var movie = sortedMovies[i];
            string ratingStr = movie.Rating > 0 ? movie.Rating.ToString("F1") : "N/A";
            Console.WriteLine($"  {i + 1}. {movie.Title} (Rating: {ratingStr})");
        }

        if (movies.Count > 20)
            Console.WriteLine($"  ... and {movies.Count - 20} more movies");
    }

    public static void PrintStatistics(
        Dictionary<string, Movie> movies, 
        Dictionary<string, Person> people, 
        Dictionary<string, List<Movie>> tags)
    {
        Console.WriteLine($"\n{Separator}");
        Console.WriteLine("GENERAL STATISTICS");
        Console.WriteLine(Separator);
        Console.WriteLine($"Total movies: {movies.Count}");
        Console.WriteLine($"Movies with rating: {movies.Values.Count(m => m.Rating > 0)}");
        Console.WriteLine($"Movies with director: {movies.Values.Count(m => m.Director != null)}");
        Console.WriteLine($"Movies with actors: {movies.Values.Count(m => m.Actors.Any())}");
        Console.WriteLine($"Movies with tags: {movies.Values.Count(m => m.Tags.Any())}");
        Console.WriteLine($"Total people: {people.Count}");
        Console.WriteLine($"Total unique tags: {tags.Count}");

        if (movies.Values.Any(m => m.Rating > 0))
        {
            var avgRating = movies.Values.Where(m => m.Rating > 0).Average(m => m.Rating);
            Console.WriteLine($"Average rating: {avgRating:F2}");
        }
    }
}