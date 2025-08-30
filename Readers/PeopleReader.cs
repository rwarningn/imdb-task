using IMDbApplication.Models;
using System.Diagnostics;

namespace IMDbApplication.Readers;

public class PeopleReader
{
    public static Dictionary<string, Person> LoadPeople(string path)
    {
        var stopwatch = Stopwatch.StartNew();
        var peopleIndex = new Dictionary<string, Person>();
        int totalLines = 0;
        
        using (var reader = new StreamReader(path))
        {
            reader.ReadLine(); // skip header
            string? line;
            
            while ((line = reader.ReadLine()) != null)
            {
                totalLines++;
                var parts = line.Split('\t');
                if (parts.Length < 4) continue;
                
                string nconst = parts[0];  
                string fullName = parts[1]; 
                
                var nameParts = fullName.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                string firstName = nameParts.Length > 0 ? nameParts[0] : "";
                string lastName = nameParts.Length > 1 ? string.Join(" ", nameParts.Skip(1)) : "";
                
                int.TryParse(parts[2], out int birthYear);
                int? deathYear = parts[3] != "\\N" && int.TryParse(parts[3], out int death) ? death : null;
                
                peopleIndex[nconst] = new Person
                {
                    ID = peopleIndex.Count + 1,
                    FirstName = firstName,
                    LastName = lastName,
                    BirthYear = birthYear,
                    DeathYear = deathYear
                };
            }
        }
        
        stopwatch.Stop();
        Console.WriteLine($"Loaded {peopleIndex.Count}/{totalLines} lines in {stopwatch.ElapsedMilliseconds} ms");
        return peopleIndex;
    }
    
    public static void LinkPeopleToMovies(Dictionary<string, Movie> movies, 
                                        Dictionary<string, Person> peopleIndex, 
                                        string actorCodesPath)
    {
        var stopwatch = Stopwatch.StartNew();
        int totalLines = 0;
        int linksCreated = 0;
        
        using (var reader = new StreamReader(actorCodesPath))
        {
            reader.ReadLine(); // skip header
            string? line;
            
            while ((line = reader.ReadLine()) != null)
            {
                totalLines++;
                var parts = line.Split('\t');
                if (parts.Length < 4) continue;
                
                string tconst = parts[0]; 
                string nconst = parts[2]; 
                string category = parts[3]; 
                
                if (movies.ContainsKey(tconst) && peopleIndex.ContainsKey(nconst))
                {
                    var movie = movies[tconst];
                    var person = peopleIndex[nconst];
                    
                    if (category == "director")
                    {
                        movie.Director = person.FullName;
                        person.DirectedMovies.Add(movie);
                        linksCreated++;
                    }
                    else if (category == "actor" || category == "actress")
                    {
                        movie.Actors.Add(person.FullName);
                        person.ActedMovies.Add(movie);
                        linksCreated++;
                    }
                }
            }
        }
        
        stopwatch.Stop();
        Console.WriteLine($"Created {linksCreated}/{totalLines} lines connections between people with movies in {stopwatch.ElapsedMilliseconds} ms");
    }
}