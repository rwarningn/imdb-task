using IMDbApplication.Models;
using System.Diagnostics;
using System.Globalization;

namespace IMDbApplication.Readers;

public class TagsReader
{
    public static Dictionary<string, string> LoadLinks(string linksPath)
    {
        var stopwatch = Stopwatch.StartNew();
        var linksIndex = new Dictionary<string, string>(); // movieLensId -> imdbId
        int totalLines = 0;
        
        using (var reader = new StreamReader(linksPath))
        {
            reader.ReadLine(); // skip header
            string? line;
            
            while ((line = reader.ReadLine()) != null)
            {
                totalLines++;
                var parts = line.Split(',');
                if (parts.Length < 2) continue;
                
                string movieLensId = parts[0]; // movieId
                string imdbIdRaw = parts[1];   // imdbId
                
                string imdbId = imdbIdRaw.StartsWith("tt") ? imdbIdRaw : "tt" + imdbIdRaw;
                
                linksIndex[movieLensId] = imdbId;
            }
        }
        
        stopwatch.Stop();
        Console.WriteLine($"Loaded {linksIndex.Count}/{totalLines} lines IMDb-Movielens connections in {stopwatch.ElapsedMilliseconds} ms");
        return linksIndex;
    }
    
    public static Dictionary<string, string> LoadTagNames(string tagCodesPath)
    {
        var stopwatch = Stopwatch.StartNew();
        var tagsIndex = new Dictionary<string, string>(); // tagId -> tagName
        int totalLines = 0;
        
        using (var reader = new StreamReader(tagCodesPath))
        {
            reader.ReadLine(); // skip header
            string? line;
            
            while ((line = reader.ReadLine()) != null)
            {
                totalLines++;
                var parts = line.Split(',', 2); 
                if (parts.Length < 2) continue;
                
                string tagId = parts[0];
                string tagName = parts[1];
                tagsIndex[tagId] = tagName;
            }
        }
        
        stopwatch.Stop();
        Console.WriteLine($"Loaded {tagsIndex.Count}/{totalLines} tag lines in {stopwatch.ElapsedMilliseconds} ms");
        return tagsIndex;
    }
    
    public static void ProcessTagScores(Dictionary<string, Movie> movies, 
                                      Dictionary<string, string> movieLensToImdb,
                                      Dictionary<string, string> tagsIndex, 
                                      string tagScoresPath)
    {
        var stopwatch = Stopwatch.StartNew();
        int tagsProcessed = 0;
        int totalLines = 0;
        int relevantTags = 0;
        
        using (var reader = new StreamReader(tagScoresPath))
        {
            reader.ReadLine(); // skip header
            string? line;
            
            while ((line = reader.ReadLine()) != null)
            {
                totalLines++;
                var parts = line.Split(',');
                if (parts.Length < 3) continue;
                
                string movieLensId = parts[0];
                string tagId = parts[1];
                
                if (float.TryParse(parts[2], NumberStyles.Any, CultureInfo.InvariantCulture,
                        out float relevance) && relevance > 0.5f)
                {
                    relevantTags++;
                    
                    if (movieLensToImdb.ContainsKey(movieLensId))
                    {
                        string imdbId = movieLensToImdb[movieLensId];
                        
                        if (movies.ContainsKey(imdbId) && tagsIndex.ContainsKey(tagId))
                        {
                            movies[imdbId].Tags.Add(tagsIndex[tagId]);
                            tagsProcessed++;
                        }
                    }
                }
            }
        }
        
        stopwatch.Stop();
        Console.WriteLine($"Processed {tagsProcessed} movie tags");
        Console.WriteLine($"Relevant tags (>0.5): {relevantTags}/{totalLines} lines");
        Console.WriteLine($"Processing time: {stopwatch.ElapsedMilliseconds} ms");
    }
}