namespace IMDbApplication.Models;

public class Movie
{
    public int ID { get; set; }
    public string ImdbID { get; set; }
    public string Title { get; set; }
    public float Rating { get; set; }
    public string Director { get; set; }
    public HashSet<string> Actors { get; set; }
    public HashSet<string> Tags { get; set; }
    public float Raiting { get; set; } = -1;
}