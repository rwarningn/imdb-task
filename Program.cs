using IMDbApplication.Models;
using IMDbApplication.Services;

namespace IMDbApplication;

public static class Program
{
    public static async Task Main(string[] args)
    {
        var dbService = new DatabaseService();
        var filePaths = new[] {
            "data/MovieCodes_IMDB.tsv", 
            "data/ActorsDirectorsNames_IMDB.txt",
            "data/TagCodes_MovieLens.csv", 
            "data/links_IMDB_MovieLens.csv",
            "data/ActorsDirectorsCodes_IMDB.tsv", 
            "data/TagScores_MovieLens.csv",
            "data/Ratings_IMDB.tsv"
        };

        if (args.Length > 0 && args[0].Equals("rebuild-db", StringComparison.OrdinalIgnoreCase))
        {
            await dbService.RebuildDatabaseAsync(filePaths);
            Console.WriteLine("\nDatabase rebuild complete. Exiting.");
            return;
        }

        if (!File.Exists("movies.db"))
        {
            Console.WriteLine("Database file 'movies.db' not found.");
            Console.WriteLine("Please run the application with the 'rebuild-db' command first: dotnet run -- rebuild-db");
            return;
        }

        var (moviesByTitle, peopleByName, tagsToMovies) = await dbService.LoadDataFromDbAsync();

        PrintStatistics(moviesByTitle, peopleByName, tagsToMovies);
        RunInteractiveMode(moviesByTitle, peopleByName, tagsToMovies);
    }

    #region DataPrinter Logic
    
    private static readonly string Separator = new('=', 50);

    private static void RunInteractiveMode(Dictionary<string, Movie> movies, Dictionary<string, Person> people, Dictionary<string, List<Movie>> tags)
    {
        Console.WriteLine($"\n{Separator}");
        Console.WriteLine("INTERACTIVE MODE");
        Console.WriteLine(Separator);
        Console.WriteLine("Commands:");
        Console.WriteLine("  movie <title>  - Show movie information");
        Console.WriteLine("  person <name>  - Show person information");
        Console.WriteLine("  tag <name>     - Show movies with tag");
        Console.WriteLine("  stats          - Show statistics");
        Console.WriteLine("  exit           - Exit the program");
        Console.WriteLine(Separator);

        while (true)
        {
            Console.Write("\n> ");
            var input = Console.ReadLine()?.Trim();
            if (string.IsNullOrEmpty(input)) continue;

            var parts = input.Split(' ', 2, StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length == 0) continue;

            var command = parts[0].ToLower();
            var argument = parts.Length > 1 ? parts[1] : string.Empty;

            switch (command)
            {
                case "movie":
                    if (string.IsNullOrEmpty(argument))
                        Console.WriteLine("Usage: movie <title>");
                    else
                        PrintMovieInfo(argument, movies);
                    break;
                
                case "person":
                    if (string.IsNullOrEmpty(argument))
                        Console.WriteLine("Usage: person <name>");
                    else
                        PrintPersonInfo(argument, people);
                    break;
                
                case "tag":
                    if (string.IsNullOrEmpty(argument))
                        Console.WriteLine("Usage: tag <name>");
                    else
                        PrintTagInfo(argument, tags);
                    break;
                
                case "stats":
                    PrintStatistics(movies, people, tags);
                    break;
                
                case "exit":
                    Console.WriteLine("Goodbye!");
                    return;
                
                default:
                    Console.WriteLine($"Unknown command: {command}");
                    break;
            }
        }
    }

    private static void PrintMovieInfo(string title, Dictionary<string, Movie> movies)
    {
        if (!movies.TryGetValue(title, out var movie)) 
        { 
            Console.WriteLine($"Movie '{title}' not found."); 
            return; 
        }

        Console.WriteLine($"\n{Separator}\nMOVIE INFO\n{Separator}");
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

    private static void PrintPersonInfo(string name, Dictionary<string, Person> people)
    {
        if (!people.TryGetValue(name, out var person)) 
        { 
            Console.WriteLine($"Person '{name}' not found."); 
            return; 
        }
        
        var movies = person.GetAllMovies();
        Console.WriteLine($"\n{Separator}\nINFORMATION ABOUT {person.Name.ToUpper()}\n{Separator}");
        Console.WriteLine($"Participated in {movies.Count()} movies:");

        var sortedMovies = movies.OrderByDescending(m => m.Rating).Take(20).ToList();
        for (int i = 0; i < sortedMovies.Count; i++)
        {
            var m = sortedMovies[i];
            string role = (m.Director != null && m.Director.ID == person.ID) ? "Director" : "Actor";
            string ratingStr = m.Rating > 0 ? m.Rating.ToString("F1") : "N/A";
            Console.WriteLine($"  {i + 1}. {role}: {m.Title} (Rating: {ratingStr})");
        }

        if (movies.Count() > 20) 
            Console.WriteLine($"  ... and {movies.Count() - 20} more movies");
    }

    private static void PrintTagInfo(string name, Dictionary<string, List<Movie>> tags)
    {
        if (!tags.TryGetValue(name, out var movies))
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
    
    private static void PrintStatistics(Dictionary<string, Movie> movies, Dictionary<string, Person> people, Dictionary<string, List<Movie>> tags) 
    {
        Console.WriteLine($"\n{Separator}\nGENERAL STATISTICS\n{Separator}");
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

    #endregion
}