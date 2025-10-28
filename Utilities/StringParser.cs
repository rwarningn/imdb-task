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
}