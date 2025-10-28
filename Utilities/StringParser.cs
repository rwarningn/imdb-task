namespace IMDbApplication.Utilities;


public static class StringParser
{
    public static string ExtractTSVField(string line, int fieldIdx)
    {
        int currField = 0;
        int startIdx = 0;

        for (int i = 0; i < line.Length; i++)
        {
            if (line[i] == '\t')
            {
                if (currField == fieldIdx)
                {
                    return line.Substring(startIdx, i - startIdx);
                }
                currField++;
                startIdx = i + 1;
            }
        }

        if (currField == fieldIdx && startIdx < line.Length)
        {
            return line.Substring(startIdx);
        }

        return string.Empty;
    }

    public static string ExtractCSVField(string line, int fieldIdx)
    {
        int currField = 0;
        int startIdx = 0;

        for (int i = 0; i < line.Length; i++)
        {
            if (line[i] == ',')
            {
                if (currField == fieldIdx)
                {
                    return line.Substring(startIdx, i - startIdx);
                }
                currField++;
                startIdx = i + 1;
            }
        }

        if (currField == fieldIdx && startIdx < line.Length)
        {
            return line.Substring(startIdx);
        }

        return string.Empty;
    }

    public static string[] ExtractTSVFields(string line, params int[] fieldIndices)
    {
        var result = new string[fieldIndices.Length];
        var fieldMap = new Dictionary<int, int>();

        for (int i = 0; i < fieldIndices.Length; i++)
        {
            fieldMap[fieldIndices[i]] = i;
        }

        int currField = 0;
        int startIdx = 0;

        for (int i = 0; i < line.Length; i++)
        {
            if (line[i] == '\t')
            {
                if (fieldMap.ContainsKey(currField))
                {
                    result[fieldMap[currField]] = line.Substring(startIdx, i - startIdx);
                }
                currField++;
                startIdx = i + 1;
            }
        }

        // Last field
        if (fieldMap.ContainsKey(currField) && startIdx < line.Length)
        {
            result[fieldMap[currField]] = line.Substring(startIdx);
        }

        return result;
    }
    
    public static string[] ExtractCSVFields(string line, params int[] fieldIndices)
    {
        var result = new string[fieldIndices.Length];
        if (fieldIndices.Length == 0) return result;

        var fieldMap = new Dictionary<int, int>();
        for (int i = 0; i < fieldIndices.Length; i++)
        {
            fieldMap[fieldIndices[i]] = i;
        }

        int currentField = 0;
        int startIdx = 0;
        bool inQuotes = false;

        for (int i = 0; i < line.Length; i++)
        {
            char c = line[i];

            if (c == '"')
            {
                if (i + 1 < line.Length && line[i + 1] == '"')
                {
                    i++; 
                }
                else
                {
                    inQuotes = !inQuotes;
                }
            }
            else if (c == ',' && !inQuotes)
            {
                if (fieldMap.ContainsKey(currentField))
                {
                    string field = line.Substring(startIdx, i - startIdx).Trim('"');
                    result[fieldMap[currentField]] = field;
                }
                
                currentField++;
                startIdx = i + 1;
            }
        }

        if (fieldMap.ContainsKey(currentField) && startIdx < line.Length)
        {
            string field = line.Substring(startIdx).Trim('"');
            result[fieldMap[currentField]] = field;
        }

        return result;
    }
}