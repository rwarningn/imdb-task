using System.Collections.Concurrent;


namespace IMDbApplication.Models;

public class Movie
{
    public int ID { get; set; }
    public string ImdbID { get; set; } = string.Empty;
    public string Title { get; set; } = string.Empty;
    public float Rating { get; set; } = -1;
    public Person? Director { get; set; } 
    public HashSet<Person> Actors { get; set; } = new HashSet<Person>();
    public HashSet<string> Tags { get; set; } = new HashSet<string>();

    public override bool Equals(object? obj)
    {
        return obj is Movie movie && ImdbID == movie.ImdbID;
    }

    public override int GetHashCode()
    {
        return ImdbID.GetHashCode();
    }

    public override string ToString()
    {
        return $"{Title} ({ImdbID}) - Rating: {Rating}";
    }
}