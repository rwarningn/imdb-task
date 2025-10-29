using IMDbApplication.Models;
using System.Globalization;

namespace IMDbApplication.Utilities;

public static class HardcodedParsers
{
    public static bool TryParseMovieLine(string line, out (int id, string title) result)
    {
        result = default;
        try
        {
            int tab1 = line.IndexOf('\t');
            int tab2 = line.IndexOf('\t', tab1 + 1);
            int tab3 = line.IndexOf('\t', tab2 + 1);
            if (tab3 == -1) return false;

            int id = int.Parse(line.AsSpan(2, tab1 - 2));
            string title = line.Substring(tab2 + 1, tab3 - tab2 - 1);
            result = (id, title);
            return true;
        }
        catch { return false; }
    }
    
    public static bool TryParsePersonLine(string line, out (int id, Person person) result)
    {
        result = default;
        try
        {
            int tab1 = line.IndexOf('\t');
            int tab2 = line.IndexOf('\t', tab1 + 1);
            int tab3 = line.IndexOf('\t', tab2 + 1);
            int tab4 = line.IndexOf('\t', tab3 + 1);
            if (tab4 == -1) return false;

            int id = int.Parse(line.AsSpan(2, tab1 - 2));
            string fullName = line.Substring(tab1 + 1, tab2 - tab1 - 1);
            string birthYearStr = line.Substring(tab2 + 1, tab3 - tab2 - 1);
            string deathYearStr = line.Substring(tab3 + 1, tab4 - tab3 - 1);

            int.TryParse(birthYearStr, out int birthYear);
            int? deathYear = (deathYearStr != "\\N" && int.TryParse(deathYearStr, out int death)) ? death : null;

            int spaceIdx = fullName.IndexOf(' ');
            var person = new Person
            {
                FirstName = spaceIdx > 0 ? fullName.Substring(0, spaceIdx) : fullName,
                LastName = spaceIdx > 0 ? fullName.Substring(spaceIdx + 1) : "",
                BirthYear = birthYear,
                DeathYear = deathYear
            };

            result = (id, person);
            return true;
        }
        catch { return false; }
    }
    public static bool TryParseLinkLine(string line, out (int movieId, int personId, string category) result)
    {
        result = default;
        try
        {
            int tab1 = line.IndexOf('\t');
            int tab2 = line.IndexOf('\t', tab1 + 1);
            int tab3 = line.IndexOf('\t', tab2 + 1);
            int tab4 = line.IndexOf('\t', tab3 + 1);
            if (tab4 == -1) return false;
            
            int movieId = int.Parse(line.AsSpan(2, tab1 - 2));
            int personId = int.Parse(line.AsSpan(tab2 + 3, tab3 - (tab2 + 3)));
            string category = line.Substring(tab3 + 1, tab4 - tab3 - 1);
            
            result = (movieId, personId, category);
            return true;
        }
        catch { return false; }
    }

    public static bool TryParseRatingLine(string line, out (int movieId, float rating) result)
    {
        result = default;
        try
        {
            int tab1 = line.IndexOf('\t');
            int tab2 = line.IndexOf('\t', tab1 + 1);
            if (tab2 == -1) return false;

            var ratingSpan = line.AsSpan(tab1 + 1, tab2 - tab1 - 1);
            if (float.TryParse(ratingSpan, NumberStyles.Any, CultureInfo.InvariantCulture, out float rating))
            {
                int movieId = int.Parse(line.AsSpan(2, tab1 - 2));
                result = (movieId, rating);
                return true;
            }
            return false;
        }
        catch { return false; }
    }

    public static bool TryParseMovieLensLinkLine(string line, out (int movieLensId, int imdbId) result)
    {
        result = default;
        try
        {
            int comma1 = line.IndexOf(',');
            int comma2 = line.IndexOf(',', comma1 + 1);
            if (comma2 == -1) return false;

            int movieLensId = int.Parse(line.AsSpan(0, comma1));
            int imdbId = int.Parse(line.AsSpan(comma1 + 1, comma2 - comma1 - 1));

            result = (movieLensId, imdbId);
            return true;
        }
        catch { return false; }
    }

    public static bool TryParseTagCodeLine(string line, out (int tagId, string tagName) result)
    {
        result = default;
        try
        {
            int comma = line.IndexOf(',');
            if (comma == -1) return false;
            
            int tagId = int.Parse(line.AsSpan(0, comma));
            string tagName = line.Substring(comma + 1);
            
            result = (tagId, tagName);
            return true;
        }
        catch { return false; }
    }

    public static bool TryParseTagScoreLine(string line, out (int movieLensId, int tagId) result)
    {
        result = default;
        try
        {
            int comma1 = line.IndexOf(',');
            int comma2 = line.IndexOf(',', comma1 + 1);
            if (comma2 == -1) return false;

            int movieLensId = int.Parse(line.AsSpan(0, comma1));
            int tagId = int.Parse(line.AsSpan(comma1 + 1, comma2 - comma1 - 1));

            result = (movieLensId, tagId);
            return true;
        }
        catch { return false; }
    }
}