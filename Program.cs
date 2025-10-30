using IMDbApplication.Models;
using IMDbApplication.Services;
using System.Diagnostics;

namespace IMDbApplication;

public static class Program
{
    public static async Task Main()
    {
        var totalStopwatch = Stopwatch.StartNew();
        var processor = new Processor();

        try
        {
            Console.WriteLine("--- PHASE 1: Loading base entities ---");
            var phase1Stopwatch = Stopwatch.StartNew();
            var moviesTask = processor.ProcessMoviesAsync("data/MovieCodes_IMDB.tsv");
            var peopleTask = processor.ProcessPeopleAsync("data/ActorsDirectorsNames_IMDB.txt");
            var tagsTask = processor.ProcessTagsAsync("data/TagCodes_MovieLens.csv");
            var linksTask = processor.ProcessMovieLensLinksAsync("data/links_IMDB_MovieLens.csv");
            
            await Task.WhenAll(moviesTask, peopleTask, tagsTask, linksTask);
            phase1Stopwatch.Stop();
            Console.WriteLine($"--- PHASE 1 COMPLETE in {phase1Stopwatch.ElapsedMilliseconds} ms ---\n");

            Console.WriteLine("--- PHASE 2: Loading and linking dependent data ---");
            var phase2Stopwatch = Stopwatch.StartNew();
            var ratingsTask = processor.ProcessRatingsAsync("data/Ratings_IMDB.tsv");
            var tagScoresTask = processor.ProcessTagScoresAsync("data/TagScores_MovieLens.csv");
            var actorLinksTask = processor.ProcessActorLinksAsync("data/ActorsDirectorsCodes_IMDB.tsv");

            await Task.WhenAll(ratingsTask, tagScoresTask, actorLinksTask);
            phase2Stopwatch.Stop();
            Console.WriteLine($"--- PHASE 2 COMPLETE in {phase2Stopwatch.ElapsedMilliseconds} ms ---\n");

            Console.WriteLine("Data loading complete. Finalizing...");
            
            var moviesByTitle = processor.Movies.Values.GroupBy(m => m.Title, StringComparer.OrdinalIgnoreCase)
                                                 .ToDictionary(g => g.Key, g => g.First(), StringComparer.OrdinalIgnoreCase);
            var peopleByName = processor.People.Values.Where(p => p.Movies.Any())
                                                .GroupBy(p => p.Name, StringComparer.OrdinalIgnoreCase)
                                                .ToDictionary(g => g.Key, g => g.First(), StringComparer.OrdinalIgnoreCase);

            
            var tagsToMovies = new Dictionary<string, List<Movie>>(StringComparer.OrdinalIgnoreCase);
            foreach(var movie in processor.Movies.Values)
            {
                foreach(var tag in movie.Tags)
                {
                    if(!tagsToMovies.ContainsKey(tag.Name))
                    {
                        tagsToMovies[tag.Name] = new List<Movie>();
                    }
                    tagsToMovies[tag.Name].Add(movie);
                }
            }
            
            totalStopwatch.Stop();
            Console.WriteLine($"\n\nTOTAL LOADING TIME: {totalStopwatch.Elapsed.TotalSeconds:F1} seconds\n");
            
            PrintStatistics(moviesByTitle, peopleByName, tagsToMovies);
            RunInteractiveMode(moviesByTitle, peopleByName, tagsToMovies);

        }
        catch (Exception ex)
        {
            Console.WriteLine($"\n\nFATAL ERROR: {ex.Message}");
            Console.WriteLine(ex.StackTrace);
        }
    }

    #region DataPrinter Logic

    private static readonly string Separator = new('=', 50);

    private static void RunInteractiveMode(
        Dictionary<string, Movie> movies, 
        Dictionary<string, Person> people, 
        Dictionary<string, List<Movie>> tags)
    {
        Console.WriteLine("\n=== INTERACTIVE SEARCH ===");
        Console.WriteLine("Commands:");
        Console.WriteLine("  movie <TITLE>  - search a movie");
        Console.WriteLine("  person <NAME>  - search an actor/director");
        Console.WriteLine("  tag <TAG>      - search movies by tag");
        Console.WriteLine("  stats          - show statistics again");
        Console.WriteLine("  exit           - exit");

        while (true)
        {
            Console.Write("\nType a command: ");
            string? input = Console.ReadLine();

            if (string.IsNullOrWhiteSpace(input) || input.Equals("exit", StringComparison.OrdinalIgnoreCase))
                break;

            var parts = input.Split(' ', 2, StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length == 0) continue;

            string command = parts[0].ToLower();

            if (command == "stats") {
                PrintStatistics(movies, people, tags);
                continue;
            }

            if (parts.Length < 2) {
                Console.WriteLine("Invalid command. Please specify a search query.");
                continue;
            }
            
            string query = parts[1];
            var searchStopwatch = Stopwatch.StartNew();

            switch (command)
            {
                case "movie":
                    PrintMovieInfo(query, movies);
                    break;
                case "person":
                    PrintPersonInfo(query, people);
                    break;
                case "tag":
                    PrintTagInfo(query, tags);
                    break;
                default:
                    Console.WriteLine("Unknown command.");
                    break;
            }
            searchStopwatch.Stop();
            Console.WriteLine($"\nSearch completed in {searchStopwatch.ElapsedMilliseconds} ms");
        }
    }

    private static void PrintMovieInfo(string title, Dictionary<string, Movie> movies)
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
        Console.WriteLine($"Director: {(string.IsNullOrEmpty(movie.Director) ? "N/A" : movie.Director)}");

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
            Console.WriteLine($"Person '{name}' is not found.");
            return;
        }
        
        var movies = person.Movies;
        Console.WriteLine($"\n{Separator}");
        Console.WriteLine($"INFORMATION ABOUT {person.Name.ToUpper()}");
        Console.WriteLine(Separator);
        Console.WriteLine($"Participated in {movies.Count} movies:");

        var sortedMovies = movies.OrderByDescending(m => m.Rating).Take(20).ToList();
        for (int i = 0; i < sortedMovies.Count; i++)
        {
            var movie = sortedMovies[i];
            string role = movie.Director == person.Name ? "Director" : "Actor";
            string ratingStr = movie.Rating > 0 ? movie.Rating.ToString("F1") : "N/A";
            Console.WriteLine($"  {i + 1}. {role}: {movie.Title} (Rating: {ratingStr})");
        }

        if (movies.Count > 20)
            Console.WriteLine($"  ... and {movies.Count - 20} more movies");
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
    
    private static void PrintStatistics(
        Dictionary<string, Movie> movies, 
        Dictionary<string, Person> people, 
        Dictionary<string, List<Movie>> tags)
    {
        Console.WriteLine($"\n{Separator}");
        Console.WriteLine("GENERAL STATISTICS");
        Console.WriteLine(Separator);
        Console.WriteLine($"Total movies: {movies.Count}");
        Console.WriteLine($"Movies with rating: {movies.Values.Count(m => m.Rating > 0)}");
        Console.WriteLine($"Movies with director: {movies.Values.Count(m => !string.IsNullOrEmpty(m.Director))}");
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