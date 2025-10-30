using IMDbApplication.Models;
using Microsoft.EntityFrameworkCore;
using System.Diagnostics;
using System.Text;
using System.Globalization;

namespace IMDbApplication.Services;

public class DatabaseService
{
    public async Task RebuildDatabaseAsync(string[] filePaths)
    {
        Console.WriteLine("--- STARTING DATABASE REBUILD ---");
        var stopwatch = Stopwatch.StartNew();

        var processor = new Processor();
        var moviesTask = processor.ProcessMoviesAsync(filePaths[0]);
        var peopleTask = processor.ProcessPeopleAsync(filePaths[1]);
        var tagsTask = processor.ProcessTagsAsync(filePaths[2]);
        var linksTask = processor.ProcessMovieLensLinksAsync(filePaths[3]);
        await Task.WhenAll(moviesTask, peopleTask, tagsTask, linksTask);
        
        var ratingsTask = processor.ProcessRatingsAsync(filePaths[6]);
        var tagScoresTask = processor.ProcessTagScoresAsync(filePaths[5]);
        var actorLinksTask = processor.ProcessActorLinksAsync(filePaths[4]);
        await Task.WhenAll(ratingsTask, tagScoresTask, actorLinksTask);
        
        stopwatch.Stop();
        Console.WriteLine($"--- Data parsing complete in {stopwatch.Elapsed.TotalSeconds:F1} seconds ---");
        
        stopwatch.Restart();
        Console.WriteLine("Saving data to database...");

        await using var context = new MovieDbContext();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        context.ChangeTracker.AutoDetectChangesEnabled = false;
        context.ChangeTracker.QueryTrackingBehavior = QueryTrackingBehavior.NoTracking;

        await using var transaction = await context.Database.BeginTransactionAsync();

        try
        {
            Console.WriteLine($"Saving {processor.People.Count} people...");
            await BulkInsertPeople(context, processor.People.Values);
            
            Console.WriteLine($"Saving {processor.Tags.Count} tags...");
            await BulkInsertTags(context, processor.Tags.Values);
            
            Console.WriteLine($"Saving {processor.Movies.Count} movies...");
            await BulkInsertMovies(context, processor.Movies.Values);

            Console.WriteLine("Building relationships...");
            
            var actorLinks = new HashSet<(int, int)>();
            foreach (var movie in processor.Movies.Values)
            {
                foreach (var actor in movie.Actors)
                {
                    actorLinks.Add((movie.ID, actor.ID));
                }
            }
            Console.WriteLine($"Creating {actorLinks.Count} unique actor-movie relationships...");
            await BulkInsertMoviePerson(context, actorLinks.ToList());
            
            var tagLinks = new HashSet<(int, int)>();
            foreach (var movie in processor.Movies.Values)
            {
                foreach (var tag in movie.Tags)
                {
                    tagLinks.Add((movie.ID, tag.ID));
                }
            }
            Console.WriteLine($"Creating {tagLinks.Count} unique movie-tag relationships...");
            await BulkInsertMovieTag(context, tagLinks.ToList());

            await transaction.CommitAsync();
            
            stopwatch.Stop();
            Console.WriteLine($"--- Database saved in {stopwatch.Elapsed.TotalSeconds:F1} seconds ---");
        }
        catch (Exception ex)
        {
            await transaction.RollbackAsync();
            Console.WriteLine($"Error saving to database: {ex.Message}");
            throw;
        }
    }

    private async Task BulkInsertPeople(MovieDbContext context, IEnumerable<Person> people)
    {
        const int batchSize = 5000;
        var batch = new List<Person>(batchSize);
        int count = 0;

        foreach (var person in people)
        {
            batch.Add(person);
            
            if (batch.Count >= batchSize)
            {
                var sql = BuildPeopleBulkInsert(batch);
                await ExecuteSqlDirectAsync(context, sql);
                batch.Clear();
                count += batchSize;
                
                if (count % 50000 == 0)
                    Console.WriteLine($"  Saved {count} people...");
            }
        }

        if (batch.Count > 0)
        {
            var sql = BuildPeopleBulkInsert(batch);
            await ExecuteSqlDirectAsync(context, sql);
        }
    }

    private async Task BulkInsertTags(MovieDbContext context, IEnumerable<Tag> tags)
    {
        const int batchSize = 5000;
        var batch = new List<Tag>(batchSize);
        int count = 0;

        foreach (var tag in tags)
        {
            batch.Add(tag);
            
            if (batch.Count >= batchSize)
            {
                var sql = BuildTagsBulkInsert(batch);
                await ExecuteSqlDirectAsync(context, sql);
                batch.Clear();
                count += batchSize;
                
                if (count % 50000 == 0)
                    Console.WriteLine($"  Saved {count} tags...");
            }
        }

        if (batch.Count > 0)
        {
            var sql = BuildTagsBulkInsert(batch);
            await ExecuteSqlDirectAsync(context, sql);
        }
    }

    private async Task BulkInsertMovies(MovieDbContext context, IEnumerable<Movie> movies)
    {
        const int batchSize = 5000;
        var batch = new List<Movie>(batchSize);
        int count = 0;

        foreach (var movie in movies)
        {
            batch.Add(movie);
            
            if (batch.Count >= batchSize)
            {
                var sql = BuildMoviesBulkInsert(batch);
                await ExecuteSqlDirectAsync(context, sql);
                batch.Clear();
                count += batchSize;
                
                if (count % 50000 == 0)
                    Console.WriteLine($"  Saved {count} movies...");
            }
        }

        if (batch.Count > 0)
        {
            var sql = BuildMoviesBulkInsert(batch);
            await ExecuteSqlDirectAsync(context, sql);
        }
    }

    private async Task BulkInsertMoviePerson(MovieDbContext context, List<(int movieId, int personId)> links)
    {
        const int batchSize = 10000;
        int count = 0;

        for (int i = 0; i < links.Count; i += batchSize)
        {
            var batch = links.Skip(i).Take(batchSize).ToList();
            var sql = BuildMoviePersonBulkInsert(batch);
            await ExecuteSqlDirectAsync(context, sql);
            count += batch.Count;
            
            if (count % 100000 == 0)
                Console.WriteLine($"  Created {count} relationships...");
        }
    }

    private async Task BulkInsertMovieTag(MovieDbContext context, List<(int movieId, int tagId)> links)
    {
        const int batchSize = 10000;
        int count = 0;

        for (int i = 0; i < links.Count; i += batchSize)
        {
            var batch = links.Skip(i).Take(batchSize).ToList();
            var sql = BuildMovieTagBulkInsert(batch);
            await ExecuteSqlDirectAsync(context, sql);
            count += batch.Count;
            
            if (count % 100000 == 0)
                Console.WriteLine($"  Created {count} relationships...");
        }
    }

    private async Task ExecuteSqlDirectAsync(MovieDbContext context, string sql)
    {
        var connection = context.Database.GetDbConnection();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }

    private string BuildPeopleBulkInsert(List<Person> people)
    {
        var sb = new StringBuilder("INSERT INTO People (ID, Name) VALUES ");
        for (int i = 0; i < people.Count; i++)
        {
            if (i > 0) sb.Append(',');
            sb.Append('(');
            sb.Append(people[i].ID);
            sb.Append(",'");
            sb.Append(SqlEscape(people[i].Name));
            sb.Append("')");
        }
        return sb.ToString();
    }

    private string BuildTagsBulkInsert(List<Tag> tags)
    {
        var sb = new StringBuilder("INSERT INTO Tags (ID, Name) VALUES ");
        for (int i = 0; i < tags.Count; i++)
        {
            if (i > 0) sb.Append(',');
            sb.Append('(');
            sb.Append(tags[i].ID);
            sb.Append(",'");
            sb.Append(SqlEscape(tags[i].Name));
            sb.Append("')");
        }
        return sb.ToString();
    }

    private string BuildMoviesBulkInsert(List<Movie> movies)
    {
        var sb = new StringBuilder("INSERT INTO Movies (ID, Title, Rating, DirectorId) VALUES ");
        for (int i = 0; i < movies.Count; i++)
        {
            if (i > 0) sb.Append(',');
            sb.Append('(');
            sb.Append(movies[i].ID);
            sb.Append(",'");
            sb.Append(SqlEscape(movies[i].Title));
            sb.Append("',");
            sb.Append(movies[i].Rating.ToString(CultureInfo.InvariantCulture));
            sb.Append(',');
            if (movies[i].Director != null)
            {
                sb.Append(movies[i].Director.ID);
            }
            else
            {
                sb.Append("NULL");
            }
            sb.Append(')');
        }
        return sb.ToString();
    }

    private string BuildMoviePersonBulkInsert(List<(int movieId, int personId)> links)
    {
        var sb = new StringBuilder("INSERT INTO MoviePerson (ActedInMoviesID, ActorsID) VALUES ");
        for (int i = 0; i < links.Count; i++)
        {
            if (i > 0) sb.Append(',');
            sb.Append('(');
            sb.Append(links[i].movieId);
            sb.Append(',');
            sb.Append(links[i].personId);
            sb.Append(')');
        }
        return sb.ToString();
    }

    private string BuildMovieTagBulkInsert(List<(int movieId, int tagId)> links)
    {
        var sb = new StringBuilder("INSERT INTO MovieTag (MoviesID, TagsID) VALUES ");
        for (int i = 0; i < links.Count; i++)
        {
            if (i > 0) sb.Append(',');
            sb.Append('(');
            sb.Append(links[i].movieId);
            sb.Append(',');
            sb.Append(links[i].tagId);
            sb.Append(')');
        }
        return sb.ToString();
    }

    private string SqlEscape(string input)
    {
        return input.Replace("'", "''");
    }

    public async Task<(Dictionary<string, Movie> movies, Dictionary<string, Person> people, Dictionary<string, List<Movie>> tagsToMovies)> LoadDataFromDbAsync()
    {
        Console.WriteLine("Loading data from database...");
        var stopwatch = Stopwatch.StartNew();
        
        await using var context = new MovieDbContext();

        Console.WriteLine("  Loading movies...");
        var moviesList = await context.Movies.AsNoTracking().ToListAsync();
        
        Console.WriteLine("  Loading people...");
        var peopleList = await context.People.AsNoTracking().ToListAsync();
        
        Console.WriteLine("  Loading tags...");
        var tagsList = await context.Tags.AsNoTracking().ToListAsync();
        
        var moviesById = moviesList.ToDictionary(m => m.ID);
        var peopleById = peopleList.ToDictionary(p => p.ID);
        var tagsById = tagsList.ToDictionary(t => t.ID);

        Console.WriteLine("  Loading actor relationships...");
        var actorLinks = await context.Database
            .SqlQuery<ActorMovieLink>($"SELECT ActedInMoviesID, ActorsID FROM MoviePerson")
            .ToListAsync();
        
        Console.WriteLine("  Loading tag relationships...");
        var tagLinks = await context.Database
            .SqlQuery<MovieTagLink>($"SELECT MoviesID, TagsID FROM MovieTag")
            .ToListAsync();

        Console.WriteLine("  Building relationships...");
        foreach (var link in actorLinks)
        {
            if (moviesById.TryGetValue(link.ActedInMoviesID, out var movie) && 
                peopleById.TryGetValue(link.ActorsID, out var person))
            {
                movie.Actors.Add(person);
                person.ActedInMovies.Add(movie);
            }
        }

        foreach (var link in tagLinks)
        {
            if (moviesById.TryGetValue(link.MoviesID, out var movie) && 
                tagsById.TryGetValue(link.TagsID, out var tag))
            {
                movie.Tags.Add(tag);
                tag.Movies.Add(movie);
            }
        }

        Console.WriteLine("  Linking directors...");
        foreach (var movie in moviesList.Where(m => m.DirectorId.HasValue))
        {
            if (peopleById.TryGetValue(movie.DirectorId.Value, out var director))
            {
                movie.Director = director;
                director.DirectedMovies.Add(movie);
            }
        }

        Console.WriteLine("  Creating lookup dictionaries...");
        var moviesByTitle = new Dictionary<string, Movie>(StringComparer.OrdinalIgnoreCase);
        foreach (var movie in moviesList)
        {
            var key = movie.Title;
            if (moviesByTitle.ContainsKey(key))
            {
                key = $"{movie.Title} ({movie.ID})";
            }
            moviesByTitle[key] = movie;
        }

        var peopleByName = new Dictionary<string, Person>(StringComparer.OrdinalIgnoreCase);
        foreach (var person in peopleList)
        {
            var key = person.Name;
            if (peopleByName.ContainsKey(key))
            {
                key = $"{person.Name} ({person.ID})";
            }
            peopleByName[key] = person;
        }
        
        var tagsToMovies = new Dictionary<string, List<Movie>>(StringComparer.OrdinalIgnoreCase);
        foreach (var movie in moviesList.Where(m => m.Tags.Any()))
        {
            foreach (var tag in movie.Tags)
            {
                if (!tagsToMovies.ContainsKey(tag.Name)) 
                    tagsToMovies[tag.Name] = new List<Movie>();
                tagsToMovies[tag.Name].Add(movie);
            }
        }
        
        stopwatch.Stop();
        Console.WriteLine($"Data loaded from database in {stopwatch.Elapsed.TotalSeconds:F1} seconds.");
        
        return (moviesByTitle, peopleByName, tagsToMovies);
    }

    private class ActorMovieLink
    {
        public int ActedInMoviesID { get; set; }
        public int ActorsID { get; set; }
    }

    private class MovieTagLink
    {
        public int MoviesID { get; set; }
        public int TagsID { get; set; }
    }
}