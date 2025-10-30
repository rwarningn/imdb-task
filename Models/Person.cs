using System.ComponentModel.DataAnnotations;


namespace IMDbApplication.Models;

public class Person
{
    [Key]
    public int ID { get; set; }
    public string Name { get; set; } = string.Empty;

    public ICollection<Movie> ActedMovies { get; set; } = new List<Movie>();
    public ICollection<Movie> DirectedMovies { get; set; } = new List<Movie>();
}