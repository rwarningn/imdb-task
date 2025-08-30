using IMDbApplication.Models;
using System.Diagnostics;

namespace IMDbApplication.Readers;

public class MoviesReader
{
    public static Dictionary<string, Movie> LoadMovies(string path)
    {
        var stopwatch = Stopwatch.StartNew();
        var movies = new Dictionary<string, Movie>();
        int totalLines = 0;
        
        using (var reader = new StreamReader(path))
        {
            string? line;
            reader.ReadLine(); // skip header
            
            while ((line = reader.ReadLine()) != null)
            {
                totalLines++;
                var parts = line.Split('\t');
                if (parts.Length < 5) continue;
                
                string imdbID = parts[0];
                string title = parts[2];
                string region = parts[3].ToLower();
                string language = parts[4].ToUpper();
                
                bool isSuitable = (region == "us" || region == "ru") ||
                                  (language == "us" || language == "ru");

                if (isSuitable && !movies.ContainsKey(imdbID))
                { 
                    movies.Add(imdbID, new Movie 
                        {
                            ID = movies.Count + 1,
                            ImdbID = imdbID,
                            Title = title
                        }
                    );
                }
            }
        }
        
        stopwatch.Stop();
        Console.WriteLine($"Loaded {movies.Count}/{totalLines} lines in {stopwatch.ElapsedMilliseconds} ms");
        return movies;
    }
}