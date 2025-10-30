using System.Collections.Concurrent;


namespace IMDbApplication.Models;

public class Movie
{
    public int ID { get; set; }
    public string Title { get; set; } = string.Empty;
    public float Rating { get; set; } = -1;
    public string Director { get; set; } = string.Empty;
    public HashSet<Person> Actors { get; set; } = new();
    public HashSet<Tag> Tags { get; set; } = new();
}