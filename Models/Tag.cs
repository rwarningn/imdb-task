using System.ComponentModel.DataAnnotations;

namespace IMDbApplication.Models;

public class Tag
{
    [Key]
    public int ID { get; set; }
    public string Name { get; set; } = string.Empty;

    public ICollection<Movie> Movies { get; set; } = new List<Movie>();
}