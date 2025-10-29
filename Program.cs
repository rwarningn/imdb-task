using IMDbApplication.Services;
using System.Diagnostics;

namespace IMDbApplication;

public class Program
{
    public static async Task Main()
    {
        try
        {
            var programStopwatch = Stopwatch.StartNew();
            
            var (movies, peopleToMovies, tagsToMovies) = await DataProcessor.LoadAllData(
                movieCodesPath: "data/MovieCodes_IMDB.tsv",
                actorNamesPath: "data/ActorsDirectorsNames_IMDB.txt", 
                actorCodesPath: "data/ActorsDirectorsCodes_IMDB.tsv",
                ratingsPath: "data/Ratings_IMDB.tsv",
                linksPath: "data/links_IMDB_MovieLens.csv", 
                tagCodesPath: "data/TagCodes_MovieLens.csv",
                tagScoresPath: "data/TagScores_MovieLens.csv"
            );
            
            DataPrinter.PrintStatistics(movies, peopleToMovies, tagsToMovies);
            
            programStopwatch.Stop();
            Console.WriteLine($"\n COMPLETED IN {programStopwatch.ElapsedMilliseconds} MS ({programStopwatch.ElapsedMilliseconds/1000} SEC)");
            
            // Интерактивный режим
            Console.WriteLine("\n=== INTERACTIVE SEARCH ===");
            Console.WriteLine("Commands:");
            Console.WriteLine("  movie <TITLE>  - search a movie");
            Console.WriteLine("  person <NAME>      - search an actor/director");
            Console.WriteLine("  tag <TAG>         - search movie by tag");
            Console.WriteLine("  stats             - show statistics");
            Console.WriteLine("  exit              - exit");
            
            while (true)
            {
                Console.Write("\nType a command: ");
                string? input = Console.ReadLine();
                
                if (string.IsNullOrWhiteSpace(input) || input.ToLower() == "exit")
                    break;
                
                if (input.ToLower() == "stats")
                {
                    DataPrinter.PrintStatistics(movies, peopleToMovies, tagsToMovies);
                    continue;
                }
                
                var parts = input.Split(' ', 2);
                if (parts.Length < 2) 
                {
                    Console.WriteLine("Unknown command. Use: movie <TITLE>, person <NAME>, tag <TAG>");
                    continue;
                }
                
                string command = parts[0].ToLower();
                string query = parts[1];
                
                var searchStopwatch = Stopwatch.StartNew();
                
                switch (command)
                {
                    case "movie":
                        DataPrinter.PrintMovieInfo(query, movies);
                        break;
                    case "person":
                        DataPrinter.PrintPersonInfo(query, peopleToMovies);
                        break;
                    case "tag":
                        DataPrinter.PrintTagInfo(query, tagsToMovies);
                        break;
                    default:
                        Console.WriteLine("Unknown command. Use: movie, person, tag, stats, exit");
                        continue;
                }
                
                searchStopwatch.Stop();
                Console.WriteLine($"\n Search completed in {searchStopwatch.ElapsedMilliseconds} ms");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error while processing data: {ex.Message}");
            Console.WriteLine($"Call stack: {ex.StackTrace}");
        }
    }
}