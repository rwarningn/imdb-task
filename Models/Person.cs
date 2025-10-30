namespace IMDbApplication.Models;

public class Person
{
    public int ID { get; set; }
    public string Name { get; set; } = string.Empty;
    public HashSet<Movie> Movies { get; set; } = new();
}