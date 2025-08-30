namespace IMDbApplication.Models;

public class Person
{
    public int ID { get; set; }
    public string FirstName { get; set; } = string.Empty;
    public string LastName { get; set; } = string.Empty;
    public int BirthYear { get; set; }
    public int? DeathYear { get; set; }

    public List<Movie> ActedMovies { get; set; } = new List<Movie>();
    public List<Movie> DirectedMovies { get; set; } = new List<Movie>();
    
    public string FullName => $"{FirstName} {LastName}".Trim();
}