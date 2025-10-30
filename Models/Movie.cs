using System.ComponentModel.DataAnnotations;

namespace IMDbApplication.Models;

public class Movie
{
    [Key]
    public int ID { get; set; }
    public string Title { get; set; } = string.Empty;
    public float Rating { get; set; } = -1;

    public int? DirectorId { get; set; }
    public Person? Director { get; set; }

    public ICollection<Person> Actors { get; set; } = new List<Person>();
    public ICollection<Tag> Tags { get; set; } = new List<Tag>();
}