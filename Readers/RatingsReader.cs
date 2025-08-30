using IMDbApplication.Models;
using System.Diagnostics;
using System.Globalization;

namespace IMDbApplication.Readers;

public class RatingsReader
{
    public static void LoadRatings(Dictionary<string, Movie> movies, string ratingsPath)
    {
        var stopwatch = Stopwatch.StartNew();
        int ratingsLoaded = 0;
        int totalLines = 0;
        
        using (var reader = new StreamReader(ratingsPath))
        {
            reader.ReadLine(); // skip header
            string? line;
            
            while ((line = reader.ReadLine()) != null)
            {
                totalLines++;
                var parts = line.Split('\t');
                if (parts.Length < 3) continue;
                
                string tconst = parts[0];
                
                if (movies.ContainsKey(tconst) && float.TryParse(parts[1], 
                        NumberStyles.Any, CultureInfo.InvariantCulture, out float rating))
                {
                    movies[tconst].Rating = rating;
                    ratingsLoaded++;
                }
            }
        }
        
        stopwatch.Stop();
        Console.WriteLine($"Loaded {ratingsLoaded}/{totalLines} lines of rating in {stopwatch.ElapsedMilliseconds} ms");
    }
}