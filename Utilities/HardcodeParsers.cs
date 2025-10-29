// Utilities/HardcodedParsers.cs
using IMDbApplication.Models;
using System.Globalization;

namespace IMDbApplication.Utilities;


public static class HardcodedParsers
{
    public static bool TryParseMovieLine(string line, out (string ImdbID, string Title) result)
    {
        result = default;
        try
        {
            int tab1 = line.IndexOf('\t'); // 7
            int tab2 = line.IndexOf('\t', tab1 + 1); // 16
            int tab3 = line.IndexOf('\t', tab2 + 1); // 22
            int tab4 = line.IndexOf('\t', tab3 + 1); // 29
            int tab5 = line.IndexOf('\t', tab4 + 1); // 38
            if (tab5 == -1) return false; 

            string region = line.Substring(tab3 + 1, 2).ToLower();
            string language = line.Substring(tab4 + 1, 2).ToUpper(); 

            bool isSuitable = (region == "us" || region == "ru") || (language == "RU" || language == "EN");
            if (!isSuitable) return false;

            string imdbId = line.Substring(0, 9);
            int titleStartIndex = line.IndexOf('\t', line.IndexOf('\t', 9) + 1) + 1;
            int titleEndIndex = line.IndexOf('\t', titleStartIndex);
            string title = line.Substring(titleStartIndex, titleEndIndex - titleStartIndex);

            result = (imdbId, title);
            return true;
        }
        catch { return false; }
    }
    public static bool TryParsePersonLine(string line, out (string nconst, Person person) result)
    {
        result = default;
        try
        {
            int tab1 = line.IndexOf('\t');
            int tab2 = line.IndexOf('\t', tab1 + 1);
            int tab3 = line.IndexOf('\t', tab2 + 1);
            int tab4 = line.IndexOf('\t', tab3 + 1);
            if (tab4 == -1) return false;

            string nconst = line.Substring(0, 9);
            string fullName = line.Substring(tab1 + 1, tab2 - tab1 - 1);
            string birthYearStr = line.Substring(tab2 + 1, 4);
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

            result = (nconst, person);
            return true;
        }
        catch { return false; }
    }

    public static bool TryParseLinkLine(string line, out (string tconst, string nconst, string category) result)
    {
        result = default;
        try
        {
            int tab1 = line.IndexOf('\t');
            int tab2 = line.IndexOf('\t', tab1 + 1);
            int tab3 = line.IndexOf('\t', tab2 + 1);
            int tab4 = line.IndexOf('\t', tab3 + 1);
            if (tab4 == -1) return false;

            string category = line.Substring(tab3 + 1, tab4 - tab3 - 1);
            if (category != "actor" && category != "actress" && category != "director") return false;

            string tconst = line.Substring(0, 9);
            string nconst = line.Substring(tab2 + 1, 9);

            result = (tconst, nconst, category);
            return true;
        }
        catch { return false; }
    }

    public static bool TryParseRatingLine(string line, out (string tconst, float rating) result)
    {
        result = default;
        try
        {

            string ratingStr = line.Substring(10, 3);
            if (float.TryParse(ratingStr, NumberStyles.Any, CultureInfo.InvariantCulture, out float rating))
            {
                string tconst = line.Substring(0, 9);
                result = (tconst, rating);
                return true;
            }
            return false;
        }
        catch { return false; }
    }

    public static bool TryParseMovieLensLinkLine(string line, out (string movieLensId, string imdbId) result)
    {
        result = default;
        try
        {
            int comma1 = line.IndexOf(',');

            string movieLensId = line.Substring(0, comma1);
            string imdbId = line.Substring(comma1 + 1, 7);

            result = (movieLensId, "tt" + imdbId);
            return true;
        }
        catch { return false; }
    }

    public static bool TryParseTagCodeLine(string line, out (string tagId, string tagName) result)
    {
        result = default;

        try
        {
            int comma = line.IndexOf(',');
        
            string tagId = line.Substring(0, comma);
            string tagName = line.Substring(comma + 1);
            
            result = (tagId, tagName);
            return true;
        }
        catch { return false; }
    }

    public static bool TryParseTagScoreLine(string line, out (string movieId, string tagId) result, out float relevance)
    {
        result = default;
        relevance = 0;
        try
        {
            int comma1 = line.IndexOf(',');
            int comma2 = line.IndexOf(',', comma1 + 1);
                            
            string relevanceStr = line.Substring(comma2 + 1, 3);

            if (float.TryParse(relevanceStr, NumberStyles.Any, CultureInfo.InvariantCulture, out relevance) && relevance > 0.5f)
            {
                string movieId = line.Substring(0, comma1);
                string tagId = line.Substring(comma1 + 1, comma2 - comma1 - 1);
                result = (movieId, tagId);
                return true;
            }
            return false;
        }
        catch { return false; }
    }
}