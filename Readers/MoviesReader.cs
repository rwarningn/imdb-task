using IMDbApplication.Models;

namespace IMDbApplication.Reader;

public class MoviesReader
{
    public static Dictionary<string, Movie> LoadMovies(string path)
    {
        var movies = new Dictionary<string, Movie>();
        
        // titleId	ordering	title	region	language	types	attributes	isOriginalTitle
        
        using (var reader = new StreamReader(path))
        {
            string? line;

            reader.ReadLine(); // header
            
            while ((line = reader.ReadLine()) != null)
            {
                var splittedLine = line.Split('\t');
                string imdbID = splittedLine[0];
                string title = splittedLine[2];
                string region = splittedLine[3].ToLower();
                string language = splittedLine[4].ToUpper();
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
            
            return movies;
        }
    }
}